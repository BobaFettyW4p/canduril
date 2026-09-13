"""Gateway service: WebSocket fan-out of normalized ticks to subscribed
clients, consuming from market.ticks.normalized.<shard>.

Behavioral port of LedgerFlux's services/gateway/app.py::Gateway, with its
serialize-per-client bug fixed. LedgerFlux's ClientConnection.send_message
does json.dumps(message) on every call, and it's called once per matching
client in _broadcast_tick -- so the identical tick gets fully re-serialized
once per subscriber. Ticks are already valid JSON bytes by the time they
reach the gateway (canduril.encode_tick's output, published by the
normalizer), so the fix here goes further than "serialize once": it never
serializes the tick payload at all. canduril.decode_tick(raw).product is
used exactly once, purely for subscription routing; the wire envelope is
built via a single byte concatenation (b'{"op":"incr","data":' + raw +
b'}'), decoded to str once, and that same string handed to every matching
client's send_text() -- no json.dumps/json.loads round-trip anywhere in
the broadcast path.

Subscribe requests with want_snapshot query Postgres (services.common.
PostgresSnapshotStore, written by services/snapshotter/app.py) for a real
persisted snapshot; LedgerFlux's own documented fallback placeholder is
used only when no row exists yet for that product, or Postgres is
unreachable (a failed connect at startup is a warning, not fatal -- the
gateway still serves live ticks without persisted snapshots). No
metrics/health surface, consistent with the ingestor/normalizer phases.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

import canduril

from services.common import (
    MARKET_TICKS_SUBJECTS,
    NATSStreamManager,
    PostgresSnapshotStore,
    load_nats_config,
)
from services.gateway.models import PingRequest, SubscribeRequest, UnsubscribeRequest


def _load_service_config() -> Dict[str, Any]:
    cfg_path = Path(__file__).with_name("config.json")
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _validate_products(products: List[str]) -> List[str]:
    if not products:
        raise ValueError("Product list cannot be empty")
    normalized = []
    for p in products:
        if not p or not isinstance(p, str):
            raise ValueError(f"Invalid product: {p}")
        np = p.upper().strip()
        if not np:
            raise ValueError(f"Empty product after normalization: {p}")
        normalized.append(np)
    return normalized


class RateLimiter:
    """Token bucket, ported near-verbatim from LedgerFlux -- pure algorithm,
    no reason to change it."""

    def __init__(self, max_rate: int, burst: int):
        self.max_rate = max_rate
        self.burst = burst
        self.tokens: float = float(burst)
        self.last_update = time.time()

    def is_allowed(self) -> bool:
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.burst, self.tokens + elapsed * self.max_rate)
        self.last_update = now
        if self.tokens >= 1.0 - 0.005:
            self.tokens -= 1
            return True
        return False

    def get_retry_delay(self) -> int:
        return int(1000 / self.max_rate)


class ClientConnection:
    def __init__(self, websocket: WebSocket, rate_limiter: RateLimiter):
        self.websocket = websocket
        self.rate_limiter = rate_limiter
        self.subscribed_products: Set[str] = set()
        self.connected_at = datetime.now(timezone.utc)

    async def send_text_if_allowed(self, text: str) -> bool:
        """The one place a message reaches the socket. Callers pass an
        already-built string -- this method never serializes anything, it
        only decides whether to send it or a rate-limit notice instead."""
        if not self.rate_limiter.is_allowed():
            retry_ms = self.rate_limiter.get_retry_delay()
            await self.websocket.send_text(json.dumps({"op": "rate_limit", "retry_ms": retry_ms}))
            return False
        await self.websocket.send_text(text)
        return True

    async def send_error(self, code: str, message: str) -> None:
        # Errors bypass the rate limiter, matching LedgerFlux's send_error.
        await self.websocket.send_text(json.dumps({"op": "error", "code": code, "msg": message}))


class Gateway:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.input_stream = str(config.get("input_stream", "market.ticks.normalized"))
        self.stream_name = str(config.get("stream_name", "market_ticks"))
        self.num_shards = int(config.get("num_shards", 4))
        self.port = int(config.get("port", 8000))
        self.max_msgs_per_sec = int(config.get("max_msgs_per_sec", 100))
        self.burst = int(config.get("burst", 200))

        self.app = FastAPI(title="canduril Market Data Gateway")
        self.clients: Dict[WebSocket, ClientConnection] = {}

        nats_config = load_nats_config(
            stream_name=self.stream_name,
            subject_prefix=self.input_stream,
            stream_subjects=MARKET_TICKS_SUBJECTS,
        )
        self.broker = NATSStreamManager(nats_config)
        self.store = PostgresSnapshotStore(config.get("pg_dsn"))
        self._store_ready = False

        self.stats: Dict[str, Any] = {
            "clients_connected": 0,
            "messages_sent": 0,
            "rate_limits": 0,
            "errors": 0,
        }

        self._setup_routes()

    def _setup_routes(self) -> None:
        port = self.port

        @self.app.get("/")
        async def root():
            return HTMLResponse(f"""
            <html>
                <head><title>canduril Market Data Gateway</title></head>
                <body>
                    <h1>canduril Market Data Gateway</h1>
                    <p>WebSocket endpoint: <code>ws://localhost:{port}/ws</code></p>
                    <h3>Subscribe</h3>
                    <pre>{{"op": "subscribe", "products": ["BTC-USD"], "want_snapshot": true}}</pre>
                    <h3>Unsubscribe</h3>
                    <pre>{{"op": "unsubscribe", "products": ["BTC-USD"]}}</pre>
                    <h3>Ping</h3>
                    <pre>{{"op": "ping", "t": 1234567890}}</pre>
                </body>
            </html>
            """)

        @self.app.get("/health")
        async def health():
            return {"status": "healthy", "clients": len(self.clients)}

        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self._handle_websocket(websocket)

    async def start(self) -> None:
        print("Starting canduril Market Data Gateway")
        print(f"WebSocket: ws://localhost:{self.port}/ws")
        print(f"Input: {self.input_stream}")

        await self.broker.connect(timeout=60.0)
        print("Connected to message broker")

        try:
            await self.store.connect()
            await self.store.ensure_schema()
            self._store_ready = True
            print("Connected to Postgres snapshot store")
        except Exception as exc:
            print(f"Warning: could not connect to Postgres snapshot store: {exc}")

        # HOSTNAME (set by k8s to the pod name) makes each replica's durable
        # consumer name unique. Without it, multiple gateway replicas
        # subscribing under the same durable name become NATS competing
        # consumers -- each tick delivered to only ONE replica, not all of
        # them -- so a client connected to a different replica than the one
        # that happened to receive a given tick would never see it. Found
        # live: with 2 replicas sharing "gateway-<shard>", a client pinned
        # to one pod for 15s received zero of the ticks the other pod was
        # visibly processing. Matches LedgerFlux's own gateway, which
        # scopes its consumer name by hostname for the same reason.
        hostname = os.environ.get("HOSTNAME", "local")
        for shard in range(self.num_shards):
            await self.broker.subscribe(
                f"{self.input_stream}.{shard}",
                self._on_tick,
                consumer_name=f"gateway-{hostname}-{shard}",
            )
        print(f"Subscribed to {self.num_shards} shard(s) of {self.input_stream}")

    async def _on_tick(self, raw: bytes) -> None:
        await self._broadcast_raw_tick(raw)

    async def _broadcast_raw_tick(self, raw: bytes) -> None:
        """The fixed hot path. Also called directly by bench/gateway_bench.py
        so the benchmark exercises the exact same code the real NATS handler
        does."""
        if not self.clients:
            return

        try:
            tick = canduril.decode_tick(raw)
        except Exception as exc:
            print(f"Error decoding tick for broadcast: {exc}")
            self.stats["errors"] += 1
            return

        targets = [c for c in self.clients.values() if tick.product in c.subscribed_products]
        if not targets:
            return

        envelope = (b'{"op":"incr","data":' + raw + b"}").decode("utf-8")
        for client in targets:
            sent = await client.send_text_if_allowed(envelope)
            if sent:
                self.stats["messages_sent"] += 1
            else:
                self.stats["rate_limits"] += 1

    async def _handle_websocket(self, websocket: WebSocket) -> None:
        await websocket.accept()
        rate_limiter = RateLimiter(self.max_msgs_per_sec, self.burst)
        client = ClientConnection(websocket, rate_limiter)
        self.clients[websocket] = client
        self.stats["clients_connected"] += 1

        try:
            while True:
                data = await websocket.receive_text()
                await self._handle_client_message(client, data)
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            print(f"WebSocket error: {exc}")
            self.stats["errors"] += 1
        finally:
            self.clients.pop(websocket, None)
            self.stats["clients_connected"] -= 1

    async def _handle_client_message(self, client: ClientConnection, data: str) -> None:
        try:
            message = json.loads(data)
        except json.JSONDecodeError:
            await client.send_error("INVALID_JSON", "Invalid JSON message")
            return

        op = message.get("op") or message.get("operation")
        if op == "subscribe":
            await self._handle_subscribe(client, message)
        elif op == "unsubscribe":
            await self._handle_unsubscribe(client, message)
        elif op == "ping":
            await self._handle_ping(client, message)
        else:
            await client.send_error("INVALID_OPERATION", f"Unknown operation: {op}")

    async def _handle_subscribe(self, client: ClientConnection, message: dict) -> None:
        try:
            request = SubscribeRequest.model_validate(message)
            products = _validate_products(request.products)
            client.subscribed_products.update(products)

            if request.want_snapshot:
                for product in products:
                    record = None
                    if self._store_ready:
                        try:
                            record = await self.store.get_latest(product)
                        except Exception as exc:
                            print(f"Error fetching snapshot from Postgres for {product}: {exc}")

                    if record is not None:
                        snapshot = {
                            "op": "snapshot",
                            "data": {
                                "product": record.product,
                                "seq": record.last_seq,
                                "ts_snapshot": record.ts_snapshot,
                                "state": record.state,
                            },
                        }
                    else:
                        # No persisted snapshot yet (or store unreachable) --
                        # LedgerFlux's own documented fallback placeholder.
                        snapshot = {
                            "op": "snapshot",
                            "data": {
                                "product": product,
                                "seq": 0,
                                "ts_snapshot": int(
                                    datetime.now(timezone.utc).timestamp() * 1_000_000_000
                                ),
                                "state": {"last_trade": {"px": 0, "qty": 0}},
                            },
                        }
                    await client.send_text_if_allowed(json.dumps(snapshot))
        except Exception as exc:
            await client.send_error("SUBSCRIBE_ERROR", str(exc))

    async def _handle_unsubscribe(self, client: ClientConnection, message: dict) -> None:
        try:
            request = UnsubscribeRequest.model_validate(message)
            products = _validate_products(request.products)
            client.subscribed_products.difference_update(products)
        except Exception as exc:
            await client.send_error("UNSUBSCRIBE_ERROR", str(exc))

    async def _handle_ping(self, client: ClientConnection, message: dict) -> None:
        try:
            request = PingRequest.model_validate(message)
            await client.send_text_if_allowed(json.dumps({"op": "pong", "t": request.t}))
        except Exception as exc:
            await client.send_error("PING_ERROR", str(exc))

    async def stop(self) -> None:
        print("Stopping gateway...")
        await self.broker.disconnect()
        await self.store.close()
        print("Gateway stopped")


async def main() -> None:
    config = _load_service_config()
    gateway = Gateway(config)
    try:
        await gateway.start()
        server_config = uvicorn.Config(
            app=gateway.app, host="0.0.0.0", port=gateway.port, log_level="info"
        )
        server = uvicorn.Server(server_config)
        await server.serve()
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutting down...")
    finally:
        await gateway.stop()


if __name__ == "__main__":
    asyncio.run(main())
