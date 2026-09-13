"""Fast, no-I/O tests for the ingestor service: message-type branching,
publish behavior, and reconnect backoff, with NATSStreamManager and the
WebSocket connection mocked out entirely.

Uses real captured Coinbase traffic from bench/fixtures/sample_capture.jsonl
(the same fixture phase 1's benchmark harness uses) rather than hand-written
fixtures, so these tests exercise the exact message shapes the service will
see in production.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import canduril
from services.ingestor.app import CoinbaseIngestor

FIXTURE_PATH = Path(__file__).parent.parent / "bench" / "fixtures" / "sample_capture.jsonl"


def _real_message(msg_type: str) -> bytes:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            raw = json.loads(line)["raw"]
            if json.loads(raw).get("type") == msg_type:
                return raw.encode()
    raise AssertionError(f"no captured message of type {msg_type!r} found in {FIXTURE_PATH}")


@pytest.fixture
def ingestor_config():
    return {
        "products": ["BTC-USD", "ETH-USD", "ADA-USD"],
        "channels": ["ticker", "heartbeat"],
        "num_shards": 4,
        "output_stream": "market.ticks.raw",
        "stream_name": "market_ticks",
        "ws_uri": "wss://example.invalid",
    }


def make_ingestor(config: dict) -> CoinbaseIngestor:
    with (
        patch("services.ingestor.app.load_nats_config") as mock_load_config,
        patch("services.ingestor.app.NATSStreamManager") as mock_broker_class,
    ):
        mock_load_config.return_value = MagicMock()
        mock_broker_class.return_value = MagicMock(publish=AsyncMock())
        return CoinbaseIngestor(config)


class TestHandleRawMessage:
    async def test_ticker_is_parsed_sharded_and_published(self, ingestor_config):
        ingestor = make_ingestor(ingestor_config)
        raw = _real_message("ticker")
        raw_msg = json.loads(raw)
        product = raw_msg["product_id"]
        seq = raw_msg["sequence"]

        await ingestor._handle_raw_message(raw)

        assert ingestor.stats["messages_received"] == 1
        assert ingestor.stats["messages_published"] == 1
        assert ingestor.stats["errors"] == 0
        assert ingestor.stats["products"][product] == 1

        ingestor.broker.publish.assert_awaited_once()
        subject, payload = ingestor.broker.publish.call_args.args
        expected_shard = canduril.shard_index(product, ingestor.num_shards)
        assert subject == f"market.ticks.raw.{expected_shard}"
        assert json.loads(payload)["product"] == product
        assert ingestor.broker.publish.call_args.kwargs["msg_id"] == (
            f"{ingestor.output_stream}:{product}:{seq}"
        )

    async def test_heartbeat_is_skipped_not_an_error(self, ingestor_config):
        ingestor = make_ingestor(ingestor_config)
        await ingestor._handle_raw_message(_real_message("heartbeat"))

        assert ingestor.stats["messages_received"] == 1
        assert ingestor.stats["messages_published"] == 0
        assert ingestor.stats["errors"] == 0
        ingestor.broker.publish.assert_not_awaited()

    async def test_subscriptions_ack_is_skipped_not_an_error(self, ingestor_config):
        ingestor = make_ingestor(ingestor_config)
        await ingestor._handle_raw_message(_real_message("subscriptions"))

        assert ingestor.stats["messages_received"] == 1
        assert ingestor.stats["messages_published"] == 0
        assert ingestor.stats["errors"] == 0
        ingestor.broker.publish.assert_not_awaited()

    async def test_malformed_bytes_counts_as_error(self, ingestor_config):
        ingestor = make_ingestor(ingestor_config)
        await ingestor._handle_raw_message(b"not valid json")

        assert ingestor.stats["errors"] == 1
        ingestor.broker.publish.assert_not_awaited()


class TestReconnectBackoff:
    async def test_backoff_doubles_and_caps_then_resets_after_success(self, ingestor_config):
        ingestor = make_ingestor(ingestor_config)
        ingestor._running = True

        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            if len(sleeps) >= 6:
                ingestor._running = False

        # Fail 4 times (backoff: 1, 2, 4, 8), succeed once (resets to 1),
        # then fail again (back to 1 -> 2) before the harness stops it.
        results = [
            Exception("boom"),
            Exception("boom"),
            Exception("boom"),
            Exception("boom"),
            True,
            Exception("boom"),
        ]

        with patch.object(
            ingestor, "_websocket_loop", new_callable=AsyncMock, side_effect=results
        ):
            with patch("services.ingestor.app.asyncio.sleep", side_effect=fake_sleep):
                await ingestor._run_with_reconnect()

        assert sleeps == [1.0, 2.0, 4.0, 8.0, 1.0, 2.0]

    async def test_stops_retrying_once_running_is_false(self, ingestor_config):
        ingestor = make_ingestor(ingestor_config)
        ingestor._running = True

        async def fail_and_stop():
            ingestor._running = False
            raise Exception("boom")

        with patch.object(ingestor, "_websocket_loop", side_effect=fail_and_stop):
            with patch("services.ingestor.app.asyncio.sleep", new_callable=AsyncMock) as sleep:
                await ingestor._run_with_reconnect()
                sleep.assert_not_awaited()
