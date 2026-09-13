"""Normalizer service: consumes Tick messages from its assigned input shard,
decodes/validates/shards/re-encodes them via canduril._core, and republishes.

Behavioral port of LedgerFlux's services/normalizer/app.py::Normalizer, with
the pure hot function (decode/validate/shard/encode) swapped from Pydantic to
the canduril C++ core (~5.4x faster mean latency per phase 1-2's benchmark).
Sequence-gap tracking stays here in Python since it's stateful per-instance
data, not part of the pure hot function.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any, Dict

import canduril

from services.common import MARKET_TICKS_SUBJECTS, NATSStreamManager, load_nats_config


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


class Normalizer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.shard_id = _resolve_shard_id(config)
        self.num_shards = int(config.get("num_shards", 4))
        self.input_stream = str(config.get("input_stream", "market.ticks"))
        self.output_stream = str(config.get("output_stream", "market.ticks"))
        self.stream_name = str(config.get("stream_name", "market_ticks"))

        nats_config = load_nats_config(
            stream_name=self.stream_name,
            subject_prefix=self.output_stream,
            stream_subjects=MARKET_TICKS_SUBJECTS,
        )
        self.broker = NATSStreamManager(nats_config)

        self.last_sequences: Dict[str, int] = {}
        self.stats: Dict[str, Any] = {
            "messages_processed": 0,
            "messages_validated": 0,
            "messages_rejected": 0,
            "products_seen": set(),
            "errors": 0,
        }

    async def start(self) -> None:
        print(f"Starting Normalizer (Shard {self.shard_id})")
        print(f"Input: {self.input_stream}.{self.shard_id}")
        print(f"Output: {self.output_stream}")

        await self.broker.connect(timeout=60.0)
        print("Connected to message broker")

        subject = f"{self.input_stream}.{self.shard_id}"
        consumer_name = f"normalizer-{self.shard_id}"
        await self.broker.subscribe(subject, self._handle_message, consumer_name=consumer_name)
        print(f"Listening on {subject}")

    async def _handle_message(self, raw: bytes) -> None:
        self.stats["messages_processed"] += 1
        try:
            tick = canduril.decode_tick(raw)
        except Exception as exc:
            print(f"Error decoding tick: {exc}")
            self.stats["errors"] += 1
            return

        self.stats["products_seen"].add(tick.product)

        if tick.product in self.last_sequences and tick.seq < self.last_sequences[tick.product]:
            print(
                f"Out-of-order sequence: {tick.product} {tick.seq} < "
                f"{self.last_sequences[tick.product]}"
            )
        self.last_sequences[tick.product] = tick.seq

        if not canduril.validate_tick(tick):
            self.stats["messages_rejected"] += 1
            return

        output_shard = canduril.shard_index(tick.product, self.num_shards)
        encoded = canduril.encode_tick(tick)
        # Stable per-tick id so JetStream's server-side dedup makes
        # redelivery-driven reprocessing a no-op. Prefixed with
        # output_stream because Nats-Msg-Id dedup is stream-wide, not
        # per-subject: raw.*/normalized.* live on the same physical
        # stream, so a bare f"{product}:{seq}" here would collide with
        # the ingestor's publish of the same tick and get silently
        # dropped as a "duplicate" -- confirmed via a live smoke test
        # where normalized output vanished despite normalizer.stats
        # showing successful publishes.
        msg_id = f"{self.output_stream}:{tick.product}:{tick.seq}"
        await self.broker.publish(f"{self.output_stream}.{output_shard}", encoded, msg_id=msg_id)

        self.stats["messages_validated"] += 1

        if self.stats["messages_processed"] % 50 == 0:
            print(
                f"Stats: processed={self.stats['messages_processed']}, "
                f"validated={self.stats['messages_validated']}, "
                f"rejected={self.stats['messages_rejected']}, "
                f"products={len(self.stats['products_seen'])}"
            )

    async def stop(self) -> None:
        print("Stopping normalizer...")
        await self.broker.disconnect()
        print("Normalizer stopped")


async def main() -> None:
    config = _load_service_config()
    normalizer = Normalizer(config)
    try:
        await normalizer.start()
        # start() only kicks off a background subscription task; keep the
        # process alive until interrupted.
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutting down...")
    finally:
        await normalizer.stop()


if __name__ == "__main__":
    asyncio.run(main())
