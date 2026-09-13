"""Real end-to-end integration test: runs the actual Snapshotter against
real dockerized NATS+JetStream and real dockerized Postgres, publishes
synthetic normalized ticks (including repeats for the same product), and
asserts on what actually lands in Postgres after a flush -- not a mocked
store.
"""

from __future__ import annotations

import asyncio

import nats

import canduril
from services.common import PostgresSnapshotStore
from services.snapshotter.app import Snapshotter
from tests.conftest import NATS_URL

NUM_SHARDS = 4


def _shard0_products(count: int) -> list[str]:
    products = [f"SNAP-{i}" for i in range(200)]
    on_shard0 = [p for p in products if canduril.shard_index(p, NUM_SHARDS) == 0]
    assert len(on_shard0) >= count, "need more synthetic candidate products"
    return on_shard0[:count]


def _build_tick(product: str, seq: int, px: float) -> bytes:
    tick = canduril.Tick()
    tick.product = product
    tick.seq = seq
    tick.ts_event = 1_700_000_000_000_000_000
    tick.ts_ingest = 1_700_000_000_100_000_000
    tick.fields = canduril.TickFields()
    tick.fields.last_trade = canduril.TradeData(px=px, qty=1.0)
    return canduril.encode_tick(tick)


def _make_snapshotter() -> Snapshotter:
    return Snapshotter(
        {
            "shard_id": 0,
            "num_shards": NUM_SHARDS,
            "input_stream": "market.ticks.normalized",
            "stream_name": "market_ticks",
            "flush_interval_ms": 300,
            "max_batch_size": 500,
        }
    )


async def _count_tick_history(store: PostgresSnapshotStore, product: str) -> int:
    async with store._conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM tick_history WHERE product = %s", (product,))
        row = await cur.fetchone()
        return int(row[0])


async def test_batched_history_and_coalesced_latest_land_in_postgres(
    nats_available, pg_available
):
    product_a, product_b = _shard0_products(2)

    snapshotter = _make_snapshotter()
    publisher = None
    try:
        await snapshotter.start()

        publisher = await nats.connect(NATS_URL)
        js = publisher.jetstream()

        # product_a ticks 3 times (repeats should coalesce to the last
        # value in `snapshots`, but all 3 rows should still land in
        # tick_history); product_b ticks once.
        await js.publish("market.ticks.normalized.0", _build_tick(product_a, seq=1, px=100.0))
        await js.publish("market.ticks.normalized.0", _build_tick(product_a, seq=2, px=101.0))
        await js.publish("market.ticks.normalized.0", _build_tick(product_a, seq=3, px=102.0))
        await js.publish("market.ticks.normalized.0", _build_tick(product_b, seq=1, px=50.0))

        deadline = asyncio.get_event_loop().time() + 10.0
        while (
            snapshotter.stats["messages_processed"] < 4
            and asyncio.get_event_loop().time() < deadline
        ):
            await asyncio.sleep(0.1)

        # Give the periodic flush (300ms) time to run.
        await asyncio.sleep(0.6)

        # Product-scoped queries are exact: product_a/product_b are unique
        # to this test, so no other integration test file can contribute
        # rows for them.
        assert await _count_tick_history(snapshotter.store, product_a) == 3
        assert await _count_tick_history(snapshotter.store, product_b) == 1

        record_a = await snapshotter.store.get_latest(product_a)
        assert record_a is not None
        assert record_a.last_seq == 3
        assert record_a.state["last_trade"]["px"] == 102.0

        record_b = await snapshotter.store.get_latest(product_b)
        assert record_b is not None
        assert record_b.last_seq == 1
        assert record_b.state["last_trade"]["px"] == 50.0

        # The global stats counters are not product-scoped: when
        # `make test-integration` runs multiple integration test files
        # against one shared NATS container, a fresh durable consumer
        # replays the *entire* subject history by default, including
        # messages other test files' real services published earlier to
        # this same shard. So these are lower bounds, not exact counts --
        # the product-scoped assertions above are the real correctness
        # proof.
        assert snapshotter.stats["messages_processed"] >= 4
        assert snapshotter.stats["history_rows_written"] >= 4
        assert snapshotter.stats["latest_upserts_written"] >= 2  # coalesced: >= these 2 products
    finally:
        await snapshotter.stop()
        if publisher is not None:
            await publisher.close()
