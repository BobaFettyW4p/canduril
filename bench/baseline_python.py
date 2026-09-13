"""Self-contained reimplementation of LedgerFlux's ingestor + normalizer hot
paths, for a fair 'before' baseline in the benchmark harness.

This intentionally does NOT import LedgerFlux -- canduril has no cross-repo
dependency. It reproduces the exact logic (including known quirks, like the
float64 timestamp precision loss in transform_coinbase_ticker -- see
tests/test_core.py's epoch_ns() docstring) from:
  - services/ingestor/app.py::transform_coinbase_ticker / create_tick
  - services/common/util.py::shard_index
  - services/normalizer/app.py::Normalizer._validate_tick
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class TradeData(BaseModel):
    px: float
    qty: float


class TickFields(BaseModel):
    last_trade: Optional[TradeData] = None
    best_bid: Optional[TradeData] = None
    best_ask: Optional[TradeData] = None


class Tick(BaseModel):
    v: int = 1
    type: str = "tick"
    product: str
    seq: int
    ts_event: int
    ts_ingest: int
    fields: TickFields


def shard_index(product: str, num_shards: int) -> int:
    digest = hashlib.sha256(product.encode("utf-8"))
    return int(digest.hexdigest(), 16) % num_shards


def transform_coinbase_ticker(coinbase_data: dict) -> Tick:
    event_time = datetime.fromisoformat(coinbase_data["time"].replace("Z", "+00:00"))
    ts_event = int(event_time.timestamp() * 1_000_000_000)

    fields = TickFields()

    if "price" in coinbase_data and "last_size" in coinbase_data:
        fields.last_trade = TradeData(
            px=float(coinbase_data["price"]), qty=float(coinbase_data["last_size"])
        )
    if "best_bid" in coinbase_data and "best_bid_size" in coinbase_data:
        fields.best_bid = TradeData(
            px=float(coinbase_data["best_bid"]), qty=float(coinbase_data["best_bid_size"])
        )
    if "best_ask" in coinbase_data and "best_ask_size" in coinbase_data:
        fields.best_ask = TradeData(
            px=float(coinbase_data["best_ask"]), qty=float(coinbase_data["best_ask_size"])
        )

    ts_ingest = int(datetime.now().timestamp() * 1_000_000_000)
    return Tick(
        product=coinbase_data["product_id"],
        seq=coinbase_data["sequence"],
        ts_event=ts_event,
        ts_ingest=ts_ingest,
        fields=fields,
    )


def validate_tick(tick: Tick) -> bool:
    if not tick.product or not tick.fields:
        return False
    if tick.fields.last_trade and tick.fields.last_trade.px <= 0:
        return False
    if tick.fields.best_bid and tick.fields.best_bid.px <= 0:
        return False
    if tick.fields.best_ask and tick.fields.best_ask.px <= 0:
        return False
    if (
        tick.fields.best_bid
        and tick.fields.best_ask
        and tick.fields.best_ask.px <= tick.fields.best_bid.px
    ):
        return False
    return True


def ingestor_path(raw_json: bytes, num_shards: int = 4) -> bytes:
    """WS message -> Tick -> shard -> wire JSON. Mirrors CoinbaseIngester._process_ticker."""
    data = json.loads(raw_json)
    tick = transform_coinbase_ticker(data)
    shard_index(tick.product, num_shards)  # computed for parity of work; result unused here
    return tick.model_dump_json().encode()


def normalizer_path(raw_json: bytes, num_shards: int = 4) -> bytes:
    """Wire JSON -> Tick -> validate -> shard -> wire JSON. Mirrors Normalizer._process_tick."""
    data = json.loads(raw_json)
    tick = Tick.model_validate(data)
    if not validate_tick(tick):
        raise ValueError("tick failed validation")
    shard_index(tick.product, num_shards)
    return tick.model_dump_json().encode()
