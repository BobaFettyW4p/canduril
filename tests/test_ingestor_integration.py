"""Real end-to-end integration test: runs the actual CoinbaseIngestor
against a real NATS+JetStream instance and a local fake WebSocket server
that replays real captured Coinbase traffic (bench/fixtures/sample_capture
.jsonl) -- proving the parse -> shard -> encode -> publish path against
real infra without depending on a live Coinbase connection for automated
runs (a real live-Coinbase smoke test was also run manually, see the
project notes).

Also exercises the new reconnect-with-backoff behavior for real: the fake
server closes the connection after its first batch, and the test asserts
the ingestor reconnects and keeps publishing on a second connection.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import nats
import websockets

import canduril
from services.ingestor.app import CoinbaseIngestor
from tests.conftest import NATS_URL

NUM_SHARDS = 4
FIXTURE_PATH = Path(__file__).parent.parent / "bench" / "fixtures" / "sample_capture.jsonl"


def _load_fixture_messages() -> list[str]:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        return [json.loads(line)["raw"] for line in f]


def _messages_by_type(messages: list[str], msg_type: str, limit: int) -> list[str]:
    out = []
    for raw in messages:
        if json.loads(raw).get("type") == msg_type:
            out.append(raw)
            if len(out) >= limit:
                break
    return out


async def _serve_batches(batches: list[list[str]]):
    """Starts a local WebSocket server on an ephemeral port. The Nth
    accepted connection is sent batches[N] (in order), then closed --
    lets a test control exactly what the ingestor sees per connection,
    including forcing a disconnect to exercise reconnect."""
    connection_index = 0

    async def handler(websocket):
        nonlocal connection_index
        idx = connection_index
        connection_index += 1
        batch = batches[idx] if idx < len(batches) else []

        try:
            await asyncio.wait_for(websocket.recv(), timeout=2.0)  # the subscribe message
        except Exception:
            pass

        for msg in batch:
            await websocket.send(msg)
        await websocket.close()

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}"


async def _drain(js, subject: str, durable: str, timeout_s: float = 2.0) -> list:
    """Fetches whatever's available on `subject` via a fresh durable
    consumer within timeout_s, decoded as Ticks. NATS fetch() returns as
    soon as messages are available, so this is fast when data is already
    published (as it is by the time tests call this)."""
    consumer = await js.pull_subscribe(subject, durable=durable)
    try:
        msgs = await consumer.fetch(50, timeout=timeout_s)
    except TimeoutError:
        return []
    ticks = []
    for msg in msgs:
        ticks.append(canduril.decode_tick(msg.data))
        await msg.ack()
    return ticks


def _make_ingestor(ws_uri: str) -> CoinbaseIngestor:
    return CoinbaseIngestor(
        {
            "products": ["BTC-USD", "ETH-USD", "ADA-USD"],
            "channels": ["ticker", "heartbeat"],
            "num_shards": NUM_SHARDS,
            "output_stream": "market.ticks.raw",
            "stream_name": "market_ticks",
            "ws_uri": ws_uri,
        }
    )


async def test_ticker_messages_are_published_heartbeat_and_acks_are_not(nats_available):
    fixture = _load_fixture_messages()
    subscriptions = _messages_by_type(fixture, "subscriptions", limit=1)
    tickers = _messages_by_type(fixture, "ticker", limit=3)
    heartbeats = _messages_by_type(fixture, "heartbeat", limit=1)
    assert tickers and heartbeats and subscriptions, "fixture must contain all three types"

    server, ws_uri = await _serve_batches([subscriptions + tickers + heartbeats])
    ingestor = _make_ingestor(ws_uri)
    consumer_nats = None
    task = None
    try:
        task = asyncio.create_task(ingestor.start())

        deadline = asyncio.get_event_loop().time() + 10.0
        while (
            ingestor.stats["messages_published"] < len(tickers)
            and asyncio.get_event_loop().time() < deadline
        ):
            await asyncio.sleep(0.2)

        assert ingestor.stats["messages_published"] == len(tickers)
        # subscriptions ack + tickers + heartbeat were all received, but only
        # tickers counted as published.
        assert ingestor.stats["messages_received"] == len(subscriptions) + len(tickers) + len(
            heartbeats
        )
        assert ingestor.stats["errors"] == 0

        expected_products = {json.loads(t)["product_id"] for t in tickers}

        consumer_nats = await nats.connect(NATS_URL)
        js = consumer_nats.jetstream()
        seen_products: set[str] = set()
        for shard in range(NUM_SHARDS):
            published = await _drain(
                js,
                f"market.ticks.raw.{shard}",
                durable=f"test-ingestor-observer-{shard}",
            )
            seen_products.update(t.product for t in published)

        assert expected_products <= seen_products
    finally:
        await ingestor.stop()
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        server.close()
        await server.wait_closed()
        if consumer_nats is not None:
            await consumer_nats.close()


async def test_reconnects_and_keeps_publishing_after_forced_disconnect(nats_available):
    fixture = _load_fixture_messages()
    tickers = _messages_by_type(fixture, "ticker", limit=4)
    assert len(tickers) == 4

    first_batch = tickers[:1]  # server closes right after this -- forces reconnect
    second_batch = tickers[1:]

    server, ws_uri = await _serve_batches([first_batch, second_batch])
    ingestor = _make_ingestor(ws_uri)
    task = None
    try:
        task = asyncio.create_task(ingestor.start())

        deadline = asyncio.get_event_loop().time() + 15.0
        while (
            ingestor.stats["messages_published"] < len(tickers)
            and asyncio.get_event_loop().time() < deadline
        ):
            await asyncio.sleep(0.2)

        assert ingestor.stats["messages_published"] == len(tickers)
    finally:
        await ingestor.stop()
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        server.close()
        await server.wait_closed()
