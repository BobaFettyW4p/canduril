"""Correctness tests for canduril._core.

These pin the C++ core's behavior against independent Python reference
computations of the same semantics implemented in LedgerFlux
(services/common/util.py::shard_index, services/ingestor/app.py, and
services/normalizer/app.py::Normalizer._validate_tick) -- without importing
LedgerFlux itself, so canduril has no cross-repo dependency.
"""

import datetime
import hashlib
import json

import pytest

import canduril


def py_shard_index(product: str, num_shards: int) -> int:
    digest = hashlib.sha256(product.encode("utf-8"))
    return int(digest.hexdigest(), 16) % num_shards


def epoch_ns(iso: str) -> int:
    """Precise integer epoch-nanoseconds for an RFC3339 UTC timestamp.

    Deliberately avoids `datetime.timestamp() * 1e9`, which loses precision
    at nanosecond scale due to float64's ~15-17 significant digits (that's
    the exact computation LedgerFlux's own ingestor uses, and it is off by
    tens of nanoseconds -- canduril's C++ path uses integer arithmetic and
    is more precise, so this reference must be too).
    """
    dt = datetime.datetime.fromisoformat(iso)
    epoch_days = dt.date().toordinal() - datetime.date(1970, 1, 1).toordinal()
    epoch_s = epoch_days * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second
    return epoch_s * 1_000_000_000 + dt.microsecond * 1000


COINBASE_TICKER = {
    "type": "ticker",
    "sequence": 37475248783,
    "product_id": "ETH-USD",
    "price": "1285.22",
    "best_bid": "1285.04",
    "best_bid_size": "0.46688654",
    "best_ask": "1285.27",
    "best_ask_size": "1.56637040",
    "time": "2022-10-19T23:28:22.061769Z",
    "last_size": "0.3",
}


class TestShardIndex:
    @pytest.mark.parametrize("product", ["BTC-USD", "ETH-USD", "ADA-USD", "X", ""])
    @pytest.mark.parametrize("num_shards", [1, 2, 3, 4, 7, 16, 1000])
    def test_matches_python_reference(self, product, num_shards):
        assert canduril.shard_index(product, num_shards) == py_shard_index(product, num_shards)

    def test_stable_across_calls(self):
        first = canduril.shard_index("BTC-USD", 4)
        assert all(canduril.shard_index("BTC-USD", 4) == first for _ in range(100))

    def test_rejects_non_positive_num_shards(self):
        with pytest.raises(Exception):
            canduril.shard_index("BTC-USD", 0)


class TestMessageType:
    def test_ticker(self):
        raw = json.dumps(COINBASE_TICKER).encode()
        assert canduril.message_type(raw) == "ticker"

    def test_heartbeat(self):
        raw = json.dumps({"type": "heartbeat", "sequence": 1}).encode()
        assert canduril.message_type(raw) == "heartbeat"

    def test_subscriptions(self):
        raw = json.dumps({"type": "subscriptions", "channels": []}).encode()
        assert canduril.message_type(raw) == "subscriptions"

    def test_invalid_json_raises(self):
        with pytest.raises(RuntimeError):
            canduril.message_type(b"not json")

    def test_missing_type_field_raises(self):
        with pytest.raises(RuntimeError):
            canduril.message_type(json.dumps({"sequence": 1}).encode())


class TestParseCoinbaseTicker:
    def test_full_ticker(self):
        raw = json.dumps(COINBASE_TICKER).encode()
        tick = canduril.parse_coinbase_ticker(raw)

        assert tick.product == "ETH-USD"
        assert tick.seq == 37475248783
        assert tick.ts_event == epoch_ns("2022-10-19T23:28:22.061769+00:00")

        assert tick.fields.last_trade.px == pytest.approx(1285.22)
        assert tick.fields.last_trade.qty == pytest.approx(0.3)
        assert tick.fields.best_bid.px == pytest.approx(1285.04)
        assert tick.fields.best_bid.qty == pytest.approx(0.46688654)
        assert tick.fields.best_ask.px == pytest.approx(1285.27)
        assert tick.fields.best_ask.qty == pytest.approx(1.5663704)

    def test_ts_ingest_is_populated_recently(self):
        raw = json.dumps(COINBASE_TICKER).encode()
        before = datetime.datetime.now(datetime.timezone.utc).timestamp() * 1_000_000_000
        tick = canduril.parse_coinbase_ticker(raw)
        after = datetime.datetime.now(datetime.timezone.utc).timestamp() * 1_000_000_000
        assert before - 1_000_000 <= tick.ts_ingest <= after + 1_000_000

    def test_partial_fields_no_bid_ask(self):
        raw = json.dumps(
            {
                "type": "ticker",
                "sequence": 1,
                "product_id": "BTC-USD",
                "price": "50000.5",
                "last_size": "0.01",
                "time": "2024-01-01T00:00:00Z",
            }
        ).encode()
        tick = canduril.parse_coinbase_ticker(raw)
        assert tick.fields.last_trade.px == pytest.approx(50000.5)
        assert tick.fields.best_bid is None
        assert tick.fields.best_ask is None

    def test_no_fractional_seconds(self):
        raw = json.dumps(
            {
                "type": "ticker",
                "sequence": 1,
                "product_id": "BTC-USD",
                "time": "2024-01-01T00:00:00Z",
            }
        ).encode()
        tick = canduril.parse_coinbase_ticker(raw)
        assert tick.ts_event == epoch_ns("2024-01-01T00:00:00+00:00")

    def test_heartbeat_message_rejected(self):
        raw = json.dumps({"type": "heartbeat"}).encode()
        with pytest.raises(RuntimeError):
            canduril.parse_coinbase_ticker(raw)

    def test_subscriptions_message_rejected(self):
        raw = json.dumps({"type": "subscriptions", "channels": []}).encode()
        with pytest.raises(RuntimeError):
            canduril.parse_coinbase_ticker(raw)

    def test_invalid_json_rejected(self):
        with pytest.raises(RuntimeError):
            canduril.parse_coinbase_ticker(b"not json")

    def test_missing_required_field_rejected(self):
        raw = json.dumps({"type": "ticker", "sequence": 1}).encode()
        with pytest.raises(RuntimeError):
            canduril.parse_coinbase_ticker(raw)


class TestEncodeDecodeRoundTrip:
    def test_round_trip_preserves_values(self):
        tick = canduril.parse_coinbase_ticker(json.dumps(COINBASE_TICKER).encode())
        encoded = canduril.encode_tick(tick)
        decoded = canduril.decode_tick(encoded)

        assert decoded.v == tick.v
        assert decoded.type == tick.type
        assert decoded.product == tick.product
        assert decoded.seq == tick.seq
        assert decoded.ts_event == tick.ts_event
        assert decoded.ts_ingest == tick.ts_ingest
        assert decoded.fields.last_trade.px == tick.fields.last_trade.px
        assert decoded.fields.best_bid.qty == tick.fields.best_bid.qty
        assert decoded.fields.best_ask.px == tick.fields.best_ask.px

    def test_encode_produces_valid_json(self):
        tick = canduril.parse_coinbase_ticker(json.dumps(COINBASE_TICKER).encode())
        encoded = canduril.encode_tick(tick)
        parsed = json.loads(encoded)
        assert parsed["product"] == "ETH-USD"
        assert parsed["fields"]["last_trade"]["px"] == pytest.approx(1285.22)

    def test_null_fields_round_trip(self):
        tick = canduril.parse_coinbase_ticker(
            json.dumps(
                {
                    "type": "ticker",
                    "sequence": 1,
                    "product_id": "BTC-USD",
                    "time": "2024-01-01T00:00:00Z",
                }
            ).encode()
        )
        encoded = canduril.encode_tick(tick)
        parsed = json.loads(encoded)
        assert parsed["fields"]["last_trade"] is None
        assert parsed["fields"]["best_bid"] is None
        assert parsed["fields"]["best_ask"] is None

        decoded = canduril.decode_tick(encoded)
        assert decoded.fields.last_trade is None
        assert decoded.fields.best_bid is None
        assert decoded.fields.best_ask is None

    def test_decode_accepts_pydantic_shaped_json(self):
        # Shape produced by Pydantic's Tick.model_dump_json() in LedgerFlux --
        # canduril's decode_tick is the normalizer's inbound path and must
        # accept exactly this wire format.
        raw = json.dumps(
            {
                "v": 1,
                "type": "tick",
                "product": "BTC-USD",
                "seq": 42,
                "ts_event": 1_700_000_000_000_000_000,
                "ts_ingest": 1_700_000_000_100_000_000,
                "fields": {
                    "last_trade": {"px": 50000.0, "qty": 0.001},
                    "best_bid": None,
                    "best_ask": {"px": 50001.0, "qty": 0.5},
                },
            }
        ).encode()
        tick = canduril.decode_tick(raw)
        assert tick.product == "BTC-USD"
        assert tick.seq == 42
        assert tick.fields.last_trade.px == 50000.0
        assert tick.fields.best_bid is None
        assert tick.fields.best_ask.qty == 0.5


class TestValidateTick:
    def _tick(self, **fields_kwargs):
        tick = canduril.parse_coinbase_ticker(json.dumps(COINBASE_TICKER).encode())
        for key, value in fields_kwargs.items():
            setattr(tick.fields, key, value)
        return tick

    def test_valid_tick_passes(self):
        assert canduril.validate_tick(self._tick()) is True

    def test_non_positive_last_trade_price_rejected(self):
        tick = self._tick(last_trade=canduril.TradeData(px=0.0, qty=1.0))
        assert canduril.validate_tick(tick) is False

    def test_negative_bid_price_rejected(self):
        tick = self._tick(best_bid=canduril.TradeData(px=-1.0, qty=1.0))
        assert canduril.validate_tick(tick) is False

    def test_inverted_spread_rejected(self):
        tick = self._tick(
            best_bid=canduril.TradeData(px=100.0, qty=1.0),
            best_ask=canduril.TradeData(px=99.0, qty=1.0),
        )
        assert canduril.validate_tick(tick) is False

    def test_empty_product_rejected(self):
        tick = self._tick()
        tick.product = ""
        assert canduril.validate_tick(tick) is False
