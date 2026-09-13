"""Fast, no-I/O tests for the snapshotter service: buffering/coalescing
logic and flush triggers, with PostgresSnapshotStore and NATSStreamManager
mocked out entirely.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import canduril
from services.snapshotter.app import Snapshotter


@pytest.fixture
def snapshotter_config():
    return {
        "shard_id": 0,
        "num_shards": 4,
        "input_stream": "market.ticks.normalized",
        "stream_name": "market_ticks",
        "flush_interval_ms": 1000,
        "max_batch_size": 5,
    }


def make_snapshotter(config: dict) -> Snapshotter:
    with (
        patch("services.snapshotter.app.load_nats_config") as mock_load_config,
        patch("services.snapshotter.app.NATSStreamManager") as mock_broker_class,
        patch("services.snapshotter.app.PostgresSnapshotStore") as mock_store_class,
    ):
        mock_load_config.return_value = MagicMock()
        mock_broker_class.return_value = MagicMock()
        mock_store_class.return_value = MagicMock(
            insert_tick_history_batch=AsyncMock(),
            upsert_latest_batch=AsyncMock(),
        )
        return Snapshotter(config)


def _encoded_tick(product: str, seq: int, px: float) -> bytes:
    tick = canduril.Tick()
    tick.product = product
    tick.seq = seq
    tick.ts_event = 1_700_000_000_000_000_000
    tick.ts_ingest = 1_700_000_000_100_000_000
    tick.fields = canduril.TickFields()
    tick.fields.last_trade = canduril.TradeData(px=px, qty=1.0)
    return canduril.encode_tick(tick)


class TestBuffering:
    async def test_every_tick_appends_to_history(self, snapshotter_config):
        snapshotter = make_snapshotter(snapshotter_config)
        for seq in range(1, 4):
            await snapshotter._handle_message(_encoded_tick("BTC-USD", seq, 100.0 + seq))

        assert len(snapshotter._pending_history) == 3
        assert snapshotter.stats["messages_processed"] == 3

    async def test_repeated_product_coalesces_to_last_value_only(self, snapshotter_config):
        snapshotter = make_snapshotter(snapshotter_config)
        for seq in range(1, 4):
            await snapshotter._handle_message(_encoded_tick("BTC-USD", seq, 100.0 + seq))

        assert len(snapshotter._pending_latest) == 1
        version, last_seq, ts_ingest, state = snapshotter._pending_latest["BTC-USD"]
        assert last_seq == 3
        assert state["last_trade"]["px"] == 103.0

    async def test_distinct_products_each_get_an_entry(self, snapshotter_config):
        snapshotter = make_snapshotter(snapshotter_config)
        await snapshotter._handle_message(_encoded_tick("BTC-USD", 1, 100.0))
        await snapshotter._handle_message(_encoded_tick("ETH-USD", 1, 50.0))

        assert set(snapshotter._pending_latest.keys()) == {"BTC-USD", "ETH-USD"}


class TestFlush:
    async def test_flush_calls_batch_methods_once_and_clears_buffers(self, snapshotter_config):
        snapshotter = make_snapshotter(snapshotter_config)
        await snapshotter._handle_message(_encoded_tick("BTC-USD", 1, 100.0))
        await snapshotter._handle_message(_encoded_tick("BTC-USD", 2, 101.0))
        await snapshotter._handle_message(_encoded_tick("ETH-USD", 1, 50.0))

        await snapshotter._flush()

        snapshotter.store.insert_tick_history_batch.assert_awaited_once()
        history_rows = snapshotter.store.insert_tick_history_batch.call_args.args[0]
        assert len(history_rows) == 3

        snapshotter.store.upsert_latest_batch.assert_awaited_once()
        latest = snapshotter.store.upsert_latest_batch.call_args.args[0]
        assert set(latest.keys()) == {"BTC-USD", "ETH-USD"}

        assert snapshotter._pending_history == []
        assert snapshotter._pending_latest == {}
        assert snapshotter.stats["history_rows_written"] == 3
        assert snapshotter.stats["latest_upserts_written"] == 2

    async def test_flush_with_empty_buffers_does_not_call_store(self, snapshotter_config):
        snapshotter = make_snapshotter(snapshotter_config)
        await snapshotter._flush()

        snapshotter.store.insert_tick_history_batch.assert_not_awaited()
        snapshotter.store.upsert_latest_batch.assert_not_awaited()

    async def test_reaching_max_batch_size_triggers_immediate_flush(self, snapshotter_config):
        snapshotter = make_snapshotter(snapshotter_config)  # max_batch_size=5
        for seq in range(1, 5):
            await snapshotter._handle_message(_encoded_tick("BTC-USD", seq, 100.0))
        snapshotter.store.insert_tick_history_batch.assert_not_awaited()

        await snapshotter._handle_message(_encoded_tick("BTC-USD", 5, 100.0))

        snapshotter.store.insert_tick_history_batch.assert_awaited_once()
        assert snapshotter._pending_history == []
