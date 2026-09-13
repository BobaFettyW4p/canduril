"""Fast, no-I/O tests for the gateway service: RateLimiter token-bucket
behavior, broadcast routing/rate-limiting, and client message dispatch,
with NATSStreamManager and the WebSocket connection mocked out entirely.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import canduril
from services.gateway.app import ClientConnection, Gateway, RateLimiter


@pytest.fixture
def gateway_config():
    return {
        "port": 8000,
        "input_stream": "market.ticks.normalized",
        "stream_name": "market_ticks",
        "num_shards": 4,
        "max_msgs_per_sec": 100,
        "burst": 200,
    }


def make_gateway(config: dict) -> Gateway:
    with (
        patch("services.gateway.app.load_nats_config") as mock_load_config,
        patch("services.gateway.app.NATSStreamManager") as mock_broker_class,
    ):
        mock_load_config.return_value = MagicMock()
        mock_broker_class.return_value = MagicMock()
        return Gateway(config)


def make_client(gateway: Gateway, products: set, *, max_rate: int = 1000, burst: int = 1000):
    ws = MagicMock()
    ws.send_text = AsyncMock()
    limiter = RateLimiter(max_rate=max_rate, burst=burst)
    client = ClientConnection(ws, limiter)
    client.subscribed_products = set(products)
    gateway.clients[ws] = client
    return client


def _encoded_tick(product: str, px: float = 100.0) -> bytes:
    tick = canduril.Tick()
    tick.product = product
    tick.seq = 1
    tick.ts_event = 1_700_000_000_000_000_000
    tick.ts_ingest = 1_700_000_000_100_000_000
    tick.fields = canduril.TickFields()
    tick.fields.last_trade = canduril.TradeData(px=px, qty=1.0)
    return canduril.encode_tick(tick)


class TestRateLimiter:
    def test_burst_allows_up_to_burst_immediately(self):
        limiter = RateLimiter(max_rate=10, burst=5)
        assert all(limiter.is_allowed() for _ in range(5))
        assert limiter.is_allowed() is False

    def test_refills_over_time(self):
        limiter = RateLimiter(max_rate=10, burst=1)
        assert limiter.is_allowed() is True
        assert limiter.is_allowed() is False

        with patch(
            "services.gateway.app.time.time", return_value=limiter.last_update + 0.2
        ):
            assert limiter.is_allowed() is True

    def test_does_not_exceed_burst_cap_on_refill(self):
        limiter = RateLimiter(max_rate=1000, burst=2)
        with patch(
            "services.gateway.app.time.time", return_value=limiter.last_update + 100
        ):
            # would refill to 100000 tokens if uncapped
            assert limiter.is_allowed() is True
            assert limiter.is_allowed() is True
            assert limiter.is_allowed() is False

    def test_get_retry_delay(self):
        assert RateLimiter(max_rate=100, burst=200).get_retry_delay() == 10


class TestBroadcastRawTick:
    async def test_routes_only_to_subscribed_clients(self, gateway_config):
        gateway = make_gateway(gateway_config)
        btc_client = make_client(gateway, {"BTC-USD"})
        eth_client = make_client(gateway, {"ETH-USD"})

        await gateway._broadcast_raw_tick(_encoded_tick("BTC-USD"))

        btc_client.websocket.send_text.assert_awaited_once()
        eth_client.websocket.send_text.assert_not_awaited()

        envelope = json.loads(btc_client.websocket.send_text.call_args.args[0])
        assert envelope["op"] == "incr"
        assert envelope["data"]["product"] == "BTC-USD"

    async def test_no_clients_is_a_no_op(self, gateway_config):
        gateway = make_gateway(gateway_config)
        await gateway._broadcast_raw_tick(_encoded_tick("BTC-USD"))  # must not raise

    async def test_no_matching_subscribers_sends_nothing(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = make_client(gateway, {"ETH-USD"})
        await gateway._broadcast_raw_tick(_encoded_tick("BTC-USD"))
        client.websocket.send_text.assert_not_awaited()

    async def test_rate_limited_client_gets_rate_limit_message_not_tick(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = make_client(gateway, {"BTC-USD"}, max_rate=1, burst=0)

        await gateway._broadcast_raw_tick(_encoded_tick("BTC-USD"))

        client.websocket.send_text.assert_awaited_once()
        payload = json.loads(client.websocket.send_text.call_args.args[0])
        assert payload["op"] == "rate_limit"


class TestHandleClientMessage:
    def _client(self, gateway):
        ws = MagicMock()
        ws.send_text = AsyncMock()
        return ClientConnection(ws, RateLimiter(max_rate=1000, burst=1000))

    async def test_subscribe_updates_products_and_sends_snapshot(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = self._client(gateway)

        await gateway._handle_client_message(
            client, json.dumps({"op": "subscribe", "products": ["btc-usd"], "want_snapshot": True})
        )

        assert client.subscribed_products == {"BTC-USD"}
        client.websocket.send_text.assert_awaited_once()
        snapshot = json.loads(client.websocket.send_text.call_args.args[0])
        assert snapshot["op"] == "snapshot"
        assert snapshot["data"]["product"] == "BTC-USD"

    async def test_subscribe_without_snapshot_sends_nothing(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = self._client(gateway)

        await gateway._handle_client_message(
            client, json.dumps({"op": "subscribe", "products": ["BTC-USD"], "want_snapshot": False})
        )

        assert client.subscribed_products == {"BTC-USD"}
        client.websocket.send_text.assert_not_awaited()

    async def test_unsubscribe_removes_products(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = self._client(gateway)
        client.subscribed_products = {"BTC-USD", "ETH-USD"}

        await gateway._handle_client_message(
            client, json.dumps({"op": "unsubscribe", "products": ["BTC-USD"]})
        )

        assert client.subscribed_products == {"ETH-USD"}

    async def test_ping_replies_with_pong(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = self._client(gateway)

        await gateway._handle_client_message(client, json.dumps({"op": "ping", "t": 123}))

        assert json.loads(client.websocket.send_text.call_args.args[0]) == {"op": "pong", "t": 123}

    async def test_unknown_operation_sends_error(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = self._client(gateway)

        await gateway._handle_client_message(client, json.dumps({"op": "bogus"}))

        err = json.loads(client.websocket.send_text.call_args.args[0])
        assert err["op"] == "error"
        assert err["code"] == "INVALID_OPERATION"

    async def test_invalid_json_sends_error(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = self._client(gateway)

        await gateway._handle_client_message(client, "not valid json")

        err = json.loads(client.websocket.send_text.call_args.args[0])
        assert err["code"] == "INVALID_JSON"

    async def test_subscribe_empty_products_sends_subscribe_error(self, gateway_config):
        gateway = make_gateway(gateway_config)
        client = self._client(gateway)

        await gateway._handle_client_message(
            client, json.dumps({"op": "subscribe", "products": []})
        )

        err = json.loads(client.websocket.send_text.call_args.args[0])
        assert err["code"] == "SUBSCRIBE_ERROR"
