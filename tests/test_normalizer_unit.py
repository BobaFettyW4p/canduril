"""Fast, no-I/O tests for the normalizer service: shard-id resolution and the
sequence-gap/validate/publish decision logic in _handle_message, with
NATSStreamManager mocked out entirely.

Mirrors the pattern used by LedgerFlux's tests/unit/normalizer/test_app.py
(mock NATSStreamManager/load_nats_config, assert on resolved config).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import canduril
from services.normalizer.app import Normalizer, _resolve_shard_id


@pytest.fixture
def normalizer_config():
    return {
        "shard_id": 0,
        "num_shards": 4,
        "input_stream": "market.ticks.raw",
        "output_stream": "market.ticks.normalized",
        "stream_name": "market_ticks",
    }


def make_normalizer(config: dict) -> Normalizer:
    with (
        patch("services.normalizer.app.load_nats_config") as mock_load_config,
        patch("services.normalizer.app.NATSStreamManager") as mock_broker_class,
    ):
        mock_load_config.return_value = MagicMock()
        mock_broker_class.return_value = MagicMock(publish=AsyncMock())
        return Normalizer(config)


class TestShardIdResolution:
    def test_explicit_shard_id(self):
        assert _resolve_shard_id({"shard_id": 2}) == 2

    def test_default_when_absent(self):
        assert _resolve_shard_id({}) == 0

    @patch.dict("os.environ", {"HOSTNAME": "normalizer-pod-3"})
    def test_auto_from_hostname(self):
        assert _resolve_shard_id({"shard_id": "auto"}) == 3

    @patch.dict("os.environ", {"HOSTNAME": "normalizer-0"})
    def test_auto_shard_zero(self):
        assert _resolve_shard_id({"shard_id": "auto"}) == 0

    @patch.dict("os.environ", {"HOSTNAME": "some-pod-without-number"})
    def test_auto_no_match_falls_back_to_zero(self):
        assert _resolve_shard_id({"shard_id": "auto"}) == 0

    @patch.dict("os.environ", {"HOSTNAME": "test-pod-5"})
    def test_none_triggers_auto_detection(self):
        assert _resolve_shard_id({"shard_id": None}) == 5


class TestNormalizerInitialization:
    def test_init_with_explicit_shard_id(self, normalizer_config):
        normalizer = make_normalizer(normalizer_config)

        assert normalizer.shard_id == 0
        assert normalizer.num_shards == 4
        assert normalizer.input_stream == "market.ticks.raw"
        assert normalizer.output_stream == "market.ticks.normalized"
        assert normalizer.stream_name == "market_ticks"
        assert normalizer.stats["messages_processed"] == 0
        assert normalizer.last_sequences == {}

    def test_init_with_defaults(self):
        normalizer = make_normalizer({})
        assert normalizer.shard_id == 0
        assert normalizer.num_shards == 4
        assert normalizer.input_stream == "market.ticks"


def _encoded_tick(product="BTC-USD", seq=1, px=50000.0):
    tick = canduril.Tick()
    tick.product = product
    tick.seq = seq
    tick.ts_event = 1_700_000_000_000_000_000
    tick.ts_ingest = 1_700_000_000_100_000_000
    tick.fields = canduril.TickFields()
    tick.fields.last_trade = canduril.TradeData(px=px, qty=1.0)
    return canduril.encode_tick(tick)


class TestHandleMessage:
    async def test_valid_tick_is_published_and_counted(self, normalizer_config):
        normalizer = make_normalizer(normalizer_config)
        await normalizer._handle_message(_encoded_tick())

        assert normalizer.stats["messages_processed"] == 1
        assert normalizer.stats["messages_validated"] == 1
        assert normalizer.stats["messages_rejected"] == 0
        assert normalizer.last_sequences["BTC-USD"] == 1
        normalizer.broker.publish.assert_awaited_once()
        subject, payload = normalizer.broker.publish.call_args.args
        assert subject == "market.ticks.normalized.1"  # shard_index("BTC-USD", 4)
        assert b'"product":"BTC-USD"' in payload
        assert (
            normalizer.broker.publish.call_args.kwargs["msg_id"]
            == "market.ticks.normalized:BTC-USD:1"
        )

    async def test_invalid_price_is_rejected_and_not_published(self, normalizer_config):
        normalizer = make_normalizer(normalizer_config)
        await normalizer._handle_message(_encoded_tick(px=-1.0))

        assert normalizer.stats["messages_rejected"] == 1
        normalizer.broker.publish.assert_not_awaited()

    async def test_out_of_order_sequence_is_still_processed(self, normalizer_config, capsys):
        normalizer = make_normalizer(normalizer_config)
        await normalizer._handle_message(_encoded_tick(seq=5))
        await normalizer._handle_message(_encoded_tick(seq=3))

        assert normalizer.stats["messages_validated"] == 2
        assert normalizer.last_sequences["BTC-USD"] == 3
        assert "Out-of-order sequence" in capsys.readouterr().out

    async def test_malformed_payload_counts_as_error_not_crash(self, normalizer_config):
        normalizer = make_normalizer(normalizer_config)
        await normalizer._handle_message(b"not valid json")

        assert normalizer.stats["errors"] == 1
        normalizer.broker.publish.assert_not_awaited()
