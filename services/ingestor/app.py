"""Ingestor service: connects to Coinbase's public ticker WebSocket feed,
parses/shards/encodes each tick via canduril._core, and publishes onto
market.ticks.raw.<shard> -- the subject the normalizer consumes from.

Behavioral port of LedgerFlux's services/ingestor/app.py::CoinbaseIngester,
with parse+encode swapped from Pydantic/json to the canduril C++ core
(~6.4x faster mean latency per phase 1's benchmark). Two deliberate
differences from the original:

  - Reconnects with exponential backoff on disconnect/error instead of
    letting the coroutine return and going silently idle forever (what
    LedgerFlux's _websocket_loop does today) -- this is a long-lived
    external network dependency, so it should recover on its own.
  - Branches on message type via canduril.message_type() (a cheap
    simdjson field lookup) instead of a full Python-side json.loads, since
    the hot functions here take raw bytes rather than pre-parsed dicts.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict

import websockets

import canduril

from services.common import MARKET_TICKS_SUBJECTS, NATSStreamManager, load_nats_config


def _load_service_config() -> Dict[str, Any]:
    cfg_path = Path(__file__).with_name("config.json")
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


class CoinbaseIngestor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.products = [str(p).strip().upper() for p in config.get("products", [])]
        self.channels = [str(c).strip() for c in config.get("channels", ["ticker", "heartbeat"])]
        self.num_shards = int(config.get("num_shards", 4))
        self.output_stream = str(config.get("output_stream", "market.ticks.raw"))
        self.stream_name = str(config.get("stream_name", "market_ticks"))
        self.ws_uri = str(config.get("ws_uri", "wss://ws-feed.exchange.coinbase.com"))

        nats_config = load_nats_config(
            stream_name=self.stream_name,
            subject_prefix=self.output_stream,
            stream_subjects=MARKET_TICKS_SUBJECTS,
        )
        self.broker = NATSStreamManager(nats_config)

        self.stats: Dict[str, Any] = {
            "messages_received": 0,
            "messages_published": 0,
            "errors": 0,
            "products": {p: 0 for p in self.products},
        }
        self._running = False

    async def start(self) -> None:
        print("Starting Coinbase Ingestor")
        print(f"Products: {', '.join(self.products)}")
        print(f"Channels: {', '.join(self.channels)}")
        print(f"Output: {self.output_stream}")

        await self.broker.connect(timeout=60.0)
        print("Connected to message broker")

        self._running = True
        await self._run_with_reconnect()

    async def _run_with_reconnect(self) -> None:
        # Starts below 1.0 so the *first* failure lands on a 1.0s wait
        # rather than doubling past it immediately (0.5 -> 1.0 -> 2.0 -> ...).
        backoff = 0.5
        max_backoff = 30.0
        while self._running:
            connected = False
            try:
                connected = await self._websocket_loop()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"WebSocket error: {exc}")
                self.stats["errors"] += 1

            if not self._running:
                break

            backoff = 1.0 if connected else min(backoff * 2, max_backoff)
            print(f"Reconnecting in {backoff:.1f}s...")
            await asyncio.sleep(backoff)

    async def _websocket_loop(self) -> bool:
        subscribe_message = json.dumps(
            {"type": "subscribe", "product_ids": self.products, "channels": self.channels}
        )
        connected = False
        print(f"Connecting to: {self.ws_uri}")
        async with websockets.connect(self.ws_uri) as ws:
            await ws.send(subscribe_message)
            connected = True
            print("WebSocket connected")
            async for message in ws:
                if not self._running:
                    break
                raw = message.encode("utf-8") if isinstance(message, str) else bytes(message)
                await self._handle_raw_message(raw)
        return connected

    async def _handle_raw_message(self, raw: bytes) -> None:
        self.stats["messages_received"] += 1
        try:
            msg_type = canduril.message_type(raw)
        except Exception as exc:
            print(f"Invalid message: {exc}")
            self.stats["errors"] += 1
            return

        if msg_type != "ticker":
            return

        try:
            tick = canduril.parse_coinbase_ticker(raw)
        except Exception as exc:
            print(f"Error parsing ticker: {exc}")
            self.stats["errors"] += 1
            return

        shard = canduril.shard_index(tick.product, self.num_shards)
        encoded = canduril.encode_tick(tick)
        # Prefixed with output_stream: Nats-Msg-Id dedup is stream-wide, not
        # per-subject, and raw.*/normalized.* share one physical stream --
        # see the matching comment in services/normalizer/app.py for the
        # cross-stage collision this avoids.
        msg_id = f"{self.output_stream}:{tick.product}:{tick.seq}"
        await self.broker.publish(f"{self.output_stream}.{shard}", encoded, msg_id=msg_id)

        self.stats["messages_published"] += 1
        self.stats["products"][tick.product] = self.stats["products"].get(tick.product, 0) + 1

        if self.stats["messages_published"] % 100 == 0:
            print(
                f"Stats: received={self.stats['messages_received']}, "
                f"published={self.stats['messages_published']}, "
                f"errors={self.stats['errors']}"
            )

    async def stop(self) -> None:
        print("Stopping ingestor...")
        self._running = False
        await self.broker.disconnect()
        print("Ingestor stopped")


async def main() -> None:
    config = _load_service_config()
    ingestor = CoinbaseIngestor(config)
    try:
        await ingestor.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutting down...")
    finally:
        await ingestor.stop()


if __name__ == "__main__":
    asyncio.run(main())
