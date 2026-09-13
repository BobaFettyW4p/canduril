"""Snapshotter service: consumes validated ticks from
market.ticks.normalized.<shard>, buffers them, and periodically flushes
batched writes to Postgres.

Behavioral port of LedgerFlux's services/snapshotter/app.py::Snapshotter,
with its per-tick synchronous Postgres writes (_write_tick_history,
_write_to_pg -- one round trip each, per message) replaced by an in-memory
buffer flushed on a timer or when it grows large, whichever comes first.
Two distinct wins: tick_history writes are simply batched (same row count,
fewer round trips); snapshots (latest-state) writes are batched *and*
coalesced -- only the final value in each flush window matters for a
"latest state" table, so repeated ticks for the same product overwrite a
dict entry instead of each issuing their own upsert.

Drops LedgerFlux's periodic (60s) "snapshot" NATS republish -- nothing
consumes it (the gateway reads Postgres directly for snapshots, not NATS).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import canduril

from services.common import (
    MARKET_TICKS_SUBJECTS,
    NATSStreamManager,
    PostgresSnapshotStore,
    load_nats_config,
)


def _load_service_config() -> Dict[str, Any]:
    cfg_path = Path(__file__).with_name("config.json")
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_shard_id(config: Dict[str, Any]) -> int:
    shard_id = config.get("shard_id", 0)
    if shard_id is None or str(shard_id).lower() == "auto":
        pod_name = os.environ.get("HOSTNAME", "")
        match = re.search(r"-(\d+)$", pod_name)
        return int(match.group(1)) if match else 0
    return int(shard_id)


def _tick_state(tick: "canduril.Tick") -> Dict[str, Any]:
    state: Dict[str, Any] = {}
    if tick.fields.last_trade:
        state["last_trade"] = {"px": tick.fields.last_trade.px, "qty": tick.fields.last_trade.qty}
    if tick.fields.best_bid:
        state["best_bid"] = {"px": tick.fields.best_bid.px, "qty": tick.fields.best_bid.qty}
    if tick.fields.best_ask:
        state["best_ask"] = {"px": tick.fields.best_ask.px, "qty": tick.fields.best_ask.qty}
    return state


class Snapshotter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.shard_id = _resolve_shard_id(config)
        self.num_shards = int(config.get("num_shards", 4))
        self.input_stream = str(config.get("input_stream", "market.ticks.normalized"))
        self.stream_name = str(config.get("stream_name", "market_ticks"))
        self.flush_interval_ms = int(config.get("flush_interval_ms", 1000))
        self.max_batch_size = int(config.get("max_batch_size", 500))

        nats_config = load_nats_config(
            stream_name=self.stream_name,
            subject_prefix=self.input_stream,
            stream_subjects=MARKET_TICKS_SUBJECTS,
        )
        self.broker = NATSStreamManager(nats_config)
        self.store = PostgresSnapshotStore(config.get("pg_dsn"))

        self._pending_history: List[Tuple] = []
        self._pending_latest: Dict[str, Tuple[int, int, int, Dict[str, Any]]] = {}
        self._flush_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None

        self.stats: Dict[str, Any] = {
            "messages_processed": 0,
            "history_rows_written": 0,
            "latest_upserts_written": 0,
            "errors": 0,
        }

    async def start(self) -> None:
        print(f"Starting Snapshotter (Shard {self.shard_id})")
        print(f"Input: {self.input_stream}.{self.shard_id}")
        print(f"Flush: every {self.flush_interval_ms}ms or {self.max_batch_size} rows")

        await self.store.connect()
        await self.store.ensure_schema()
        print("Connected to Postgres, schema ensured")

        await self.broker.connect(timeout=60.0)
        print("Connected to message broker")

        subject = f"{self.input_stream}.{self.shard_id}"
        consumer_name = f"snapshotter-{self.shard_id}"
        await self.broker.subscribe(subject, self._handle_message, consumer_name=consumer_name)
        print(f"Listening on {subject}")

        self._flush_task = asyncio.create_task(self._periodic_flush())

    async def _handle_message(self, raw: bytes) -> None:
        self.stats["messages_processed"] += 1
        try:
            tick = canduril.decode_tick(raw)
        except Exception as exc:
            print(f"Error decoding tick: {exc}")
            self.stats["errors"] += 1
            return

        price = tick.fields.last_trade.px if tick.fields.last_trade else None
        bid = tick.fields.best_bid.px if tick.fields.best_bid else None
        ask = tick.fields.best_ask.px if tick.fields.best_ask else None
        volume = tick.fields.last_trade.qty if tick.fields.last_trade else None

        self._pending_history.append(
            (tick.product, tick.seq, price, bid, ask, volume, tick.ts_event, tick.ts_ingest)
        )
        # Later ticks for the same product simply overwrite this entry --
        # only the final value before the next flush is ever observable in
        # a "latest state" table, so there is nothing lost by coalescing.
        self._pending_latest[tick.product] = (1, tick.seq, tick.ts_ingest, _tick_state(tick))

        if len(self._pending_history) >= self.max_batch_size:
            await self._flush()

        if self.stats["messages_processed"] % 500 == 0:
            print(f"Stats: {self.stats}")

    async def _periodic_flush(self) -> None:
        while True:
            await asyncio.sleep(self.flush_interval_ms / 1000)
            await self._flush()

    async def _flush(self) -> None:
        async with self._flush_lock:
            if self._pending_history:
                history, self._pending_history = self._pending_history, []
                try:
                    await self.store.insert_tick_history_batch(history)
                    self.stats["history_rows_written"] += len(history)
                except Exception as exc:
                    print(f"Error flushing tick_history batch: {exc}")
                    self.stats["errors"] += 1

            if self._pending_latest:
                latest, self._pending_latest = self._pending_latest, {}
                try:
                    await self.store.upsert_latest_batch(latest)
                    self.stats["latest_upserts_written"] += len(latest)
                except Exception as exc:
                    print(f"Error flushing snapshots batch: {exc}")
                    self.stats["errors"] += 1

    async def stop(self) -> None:
        print("Stopping snapshotter...")
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self._flush()  # drain whatever's left
        await self.broker.disconnect()
        await self.store.close()
        print("Snapshotter stopped")


async def main() -> None:
    config = _load_service_config()
    snapshotter = Snapshotter(config)
    try:
        await snapshotter.start()
        # start() only kicks off background subscription/flush tasks; keep
        # the process alive until interrupted.
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutting down...")
    finally:
        await snapshotter.stop()


if __name__ == "__main__":
    asyncio.run(main())
