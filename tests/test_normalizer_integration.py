"""Real end-to-end integration test: runs the actual Normalizer service
against a real NATS+JetStream instance (see `make nats-up` / `make
test-integration`), publishes synthetic valid and invalid ticks directly
onto its raw input subject, and asserts on what actually gets republished
to the normalized output subject -- not a mocked broker.

Topology: input (`market.ticks.raw.<shard>`) and output
(`market.ticks.normalized.<shard>`) are on separate subjects, so the
normalizer never resubscribes to its own output -- this fixes a self-
feedback loop that was confirmed to reprocess a single tick 5,200+ times
in 2 seconds when input/output shared one subject (LedgerFlux's actual
default config). A stable Nats-Msg-Id (`f"{product}:{seq}"`) is also set on
every republish, so ordinary at-least-once redelivery (a failed ack
causing NATS to redeliver the same raw message) can't duplicate the
normalized output either -- verified directly below.
"""

from __future__ import annotations

import asyncio

import nats

import canduril
from services.normalizer.app import Normalizer
from tests.conftest import NATS_URL

NUM_SHARDS = 4


def _build_tick(product: str, seq: int, px: float) -> bytes:
    tick = canduril.Tick()
    tick.product = product
    tick.seq = seq
    tick.ts_event = 1_700_000_000_000_000_000
    tick.ts_ingest = 1_700_000_000_100_000_000
    tick.fields = canduril.TickFields()
    tick.fields.last_trade = canduril.TradeData(px=px, qty=1.0)
    return canduril.encode_tick(tick)


def _shard0_products(count: int) -> list[str]:
    products = [f"SYNTH-{i}" for i in range(200)]
    on_shard0 = [p for p in products if canduril.shard_index(p, NUM_SHARDS) == 0]
    assert len(on_shard0) >= count, "need more synthetic candidate products"
    return on_shard0[:count]


def _make_normalizer() -> Normalizer:
    return Normalizer(
        {
            "shard_id": 0,
            "num_shards": NUM_SHARDS,
            "input_stream": "market.ticks.raw",
            "output_stream": "market.ticks.normalized",
            "stream_name": "market_ticks",
        }
    )


async def _drain(js, subject: str, durable: str, expected: int, timeout_s: float = 5.0) -> list:
    """Fetch up to `expected` messages from `subject` via a fresh durable
    consumer, decoded as Ticks. Stops as soon as `expected` are seen or the
    timeout elapses."""
    consumer = await js.pull_subscribe(subject, durable=durable)
    ticks = []
    deadline = asyncio.get_event_loop().time() + timeout_s
    while len(ticks) < expected and asyncio.get_event_loop().time() < deadline:
        try:
            msgs = await consumer.fetch(20, timeout=1.0)
        except TimeoutError:
            continue
        for msg in msgs:
            ticks.append(canduril.decode_tick(msg.data))
            await msg.ack()
    return ticks


async def test_valid_tick_is_republished_invalid_is_not(nats_available):
    valid_product, invalid_product = _shard0_products(2)

    normalizer = _make_normalizer()
    publisher = None
    try:
        await normalizer.start()  # idempotently creates the JetStream stream

        publisher = await nats.connect(NATS_URL)
        js = publisher.jetstream()

        await js.publish("market.ticks.raw.0", _build_tick(valid_product, seq=1, px=100.0))
        await js.publish(
            "market.ticks.raw.0", _build_tick(invalid_product, seq=2, px=-1.0)
        )  # invalid: non-positive price

        deadline = asyncio.get_event_loop().time() + 10.0
        while (
            normalizer.stats["messages_processed"] < 2
            and asyncio.get_event_loop().time() < deadline
        ):
            await asyncio.sleep(0.2)

        # Exact counts, not >= workarounds: with raw/normalized on separate
        # subjects the normalizer cannot resubscribe to its own output, so
        # there is no loop to make these counts drift.
        assert normalizer.stats["messages_processed"] == 2
        assert normalizer.stats["messages_validated"] == 1
        assert normalizer.stats["messages_rejected"] == 1

        normalized = await _drain(
            js, "market.ticks.normalized.0", durable="test-observer-valid", expected=1
        )
        products = [t.product for t in normalized]
        assert products == [valid_product]
    finally:
        await normalizer.stop()
        if publisher is not None:
            await publisher.close()


async def test_redelivery_of_same_tick_is_deduplicated(nats_available):
    """Simulates an at-least-once redelivery (e.g. a failed ack causing NATS
    to redeliver the same raw message) by feeding the identical tick through
    _handle_message twice directly. The stable Nats-Msg-Id must make the
    second publish a server-side no-op."""
    (product,) = _shard0_products(1)

    normalizer = _make_normalizer()
    publisher = None
    try:
        await normalizer.start()
        publisher = await nats.connect(NATS_URL)
        js = publisher.jetstream()

        raw = _build_tick(product, seq=1, px=42.0)
        await normalizer._handle_message(raw)
        await normalizer._handle_message(raw)  # simulated redelivery

        assert normalizer.stats["messages_validated"] == 2  # processed twice...

        normalized = await _drain(
            js, "market.ticks.normalized.0", durable="test-observer-dedup", expected=1
        )
        # ...but only landed on the stream once, thanks to Nats-Msg-Id dedup.
        assert len(normalized) == 1
        assert normalized[0].product == product
    finally:
        await normalizer.stop()
        if publisher is not None:
            await publisher.close()
