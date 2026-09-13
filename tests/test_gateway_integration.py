"""Real end-to-end integration test: runs the actual Gateway (uvicorn, real
sockets) against real dockerized NATS+JetStream, connects a real WebSocket
client, and asserts a synthetic normalized tick published directly to NATS
is correctly fanned out -- not a mocked broker or a mocked websocket.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import nats
import pytest
import uvicorn
import websockets

import canduril
from services.common import PostgresSnapshotStore
from services.gateway.app import Gateway
from tests.conftest import NATS_URL

GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 18765
GATEWAY_WS_URL = f"ws://{GATEWAY_HOST}:{GATEWAY_PORT}/ws"
NUM_SHARDS = 4


def _make_gateway() -> Gateway:
    return Gateway(
        {
            "port": GATEWAY_PORT,
            "input_stream": "market.ticks.normalized",
            "stream_name": "market_ticks",
            "num_shards": NUM_SHARDS,
            "max_msgs_per_sec": 1000,
            "burst": 1000,
        }
    )


def _encoded_tick(product: str, px: float) -> bytes:
    tick = canduril.Tick()
    tick.product = product
    tick.seq = 1
    tick.ts_event = 1_700_000_000_000_000_000
    tick.ts_ingest = 1_700_000_000_100_000_000
    tick.fields = canduril.TickFields()
    tick.fields.last_trade = canduril.TradeData(px=px, qty=1.0)
    return canduril.encode_tick(tick)


async def _wait_for_gateway(timeout: float = 10.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    last_error: Exception | None = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            async with websockets.connect(GATEWAY_WS_URL):
                return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.2)
    raise AssertionError(f"gateway never became ready on {GATEWAY_WS_URL}: {last_error}")


@asynccontextmanager
async def _running_gateway():
    gateway = _make_gateway()
    await gateway.start()

    server_config = uvicorn.Config(
        app=gateway.app, host=GATEWAY_HOST, port=GATEWAY_PORT, log_level="warning"
    )
    server = uvicorn.Server(server_config)
    server_task = asyncio.create_task(server.serve())
    try:
        await _wait_for_gateway()
        yield gateway
    finally:
        server.should_exit = True
        await server_task
        await gateway.stop()


async def test_subscribed_client_receives_broadcast_tick(nats_available):
    async with _running_gateway():
        publisher = await nats.connect(NATS_URL)
        try:
            async with websockets.connect(GATEWAY_WS_URL) as ws:
                await ws.send(
                    json.dumps(
                        {"op": "subscribe", "products": ["BTC-USD"], "want_snapshot": False}
                    )
                )

                js = publisher.jetstream()
                shard = canduril.shard_index("BTC-USD", NUM_SHARDS)
                await js.publish(
                    f"market.ticks.normalized.{shard}", _encoded_tick("BTC-USD", 12345.0)
                )

                frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
                message = json.loads(frame)
                assert message["op"] == "incr"
                assert message["data"]["product"] == "BTC-USD"
                assert message["data"]["fields"]["last_trade"]["px"] == 12345.0
        finally:
            await publisher.close()


async def test_unsubscribed_product_produces_no_frame(nats_available):
    async with _running_gateway():
        publisher = await nats.connect(NATS_URL)
        try:
            async with websockets.connect(GATEWAY_WS_URL) as ws:
                await ws.send(
                    json.dumps(
                        {"op": "subscribe", "products": ["ETH-USD"], "want_snapshot": False}
                    )
                )

                js = publisher.jetstream()
                shard = canduril.shard_index("BTC-USD", NUM_SHARDS)
                await js.publish(f"market.ticks.normalized.{shard}", _encoded_tick("BTC-USD", 1.0))

                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(ws.recv(), timeout=2.0)
        finally:
            await publisher.close()


async def test_subscribe_with_snapshot_returns_real_persisted_data(nats_available, pg_available):
    store = PostgresSnapshotStore()
    await store.connect()
    await store.ensure_schema()
    await store.upsert_latest(
        "SEED-USD",
        version=1,
        last_seq=42,
        ts_snapshot_ns=1_700_000_000_000_000_000,
        state={"last_trade": {"px": 999.5, "qty": 2.0}},
    )
    await store.close()

    async with _running_gateway():
        async with websockets.connect(GATEWAY_WS_URL) as ws:
            await ws.send(
                json.dumps({"op": "subscribe", "products": ["SEED-USD"], "want_snapshot": True})
            )
            frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
            message = json.loads(frame)
            assert message["op"] == "snapshot"
            assert message["data"]["product"] == "SEED-USD"
            assert message["data"]["seq"] == 42
            assert message["data"]["state"]["last_trade"]["px"] == 999.5


async def test_subscribe_with_snapshot_falls_back_to_placeholder_when_no_row(
    nats_available, pg_available
):
    async with _running_gateway():
        async with websockets.connect(GATEWAY_WS_URL) as ws:
            await ws.send(
                json.dumps(
                    {"op": "subscribe", "products": ["NEVER-SEEN-USD"], "want_snapshot": True}
                )
            )
            frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
            message = json.loads(frame)
            assert message["op"] == "snapshot"
            assert message["data"]["product"] == "NEVER-SEEN-USD"
            assert message["data"]["seq"] == 0
            assert message["data"]["state"] == {"last_trade": {"px": 0, "qty": 0}}
