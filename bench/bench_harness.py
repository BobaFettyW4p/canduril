"""Replay engine: baseline_python vs canduril._core over the ingestor and
normalizer hot paths, measuring per-call latency and single-threaded
throughput.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import canduril

import baseline_python as baseline


@dataclass
class BenchResult:
    label: str
    iterations: int
    throughput_msgs_per_sec: float
    mean_ns: float
    p50_ns: int
    p90_ns: int
    p99_ns: int
    p999_ns: int

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "iterations": self.iterations,
            "throughput_msgs_per_sec": self.throughput_msgs_per_sec,
            "mean_us": self.mean_ns / 1000,
            "p50_us": self.p50_ns / 1000,
            "p90_us": self.p90_ns / 1000,
            "p99_us": self.p99_ns / 1000,
            "p999_us": self.p999_ns / 1000,
        }


def load_ticker_messages(fixture_path: Path) -> list[bytes]:
    """Raw Coinbase 'ticker' channel messages -- the ingestor's inbound corpus."""
    messages = []
    with fixture_path.open("r", encoding="utf-8") as f:
        for line in f:
            entry = json.loads(line)
            raw = entry["raw"]
            msg = json.loads(raw)
            if msg.get("type") == "ticker":
                messages.append(raw.encode())
    return messages


def build_tick_corpus(ticker_messages: list[bytes]) -> list[bytes]:
    """Wire-format Tick JSON -- the normalizer's inbound corpus.

    Derived by running the C++ parser (already correctness-tested against
    the Python reference in tests/test_core.py) once over the captured
    tickers, since normalizer consumes Tick JSON, not raw Coinbase messages.
    """
    corpus = []
    for raw in ticker_messages:
        tick = canduril.parse_coinbase_ticker(raw)
        corpus.append(canduril.encode_tick(tick))
    return corpus


def measure(
    label: str, fn: Callable[[bytes], object], corpus: list[bytes], iterations: int
) -> BenchResult:
    if not corpus:
        raise ValueError("corpus is empty -- nothing to benchmark")

    warmup = min(50, len(corpus))
    for i in range(warmup):
        fn(corpus[i])

    n = len(corpus)
    latencies_ns: list[int] = [0] * iterations
    t_start = time.perf_counter()
    for i in range(iterations):
        raw = corpus[i % n]
        t0 = time.perf_counter_ns()
        fn(raw)
        t1 = time.perf_counter_ns()
        latencies_ns[i] = t1 - t0
    total_s = time.perf_counter() - t_start

    latencies_ns.sort()

    def pct(p: float) -> int:
        idx = min(len(latencies_ns) - 1, int(len(latencies_ns) * p))
        return latencies_ns[idx]

    return BenchResult(
        label=label,
        iterations=iterations,
        throughput_msgs_per_sec=iterations / total_s if total_s > 0 else float("inf"),
        mean_ns=sum(latencies_ns) / len(latencies_ns),
        p50_ns=pct(0.50),
        p90_ns=pct(0.90),
        p99_ns=pct(0.99),
        p999_ns=pct(0.999),
    )


def cpp_ingestor_call(num_shards: int = 4) -> Callable[[bytes], bytes]:
    def call(raw: bytes) -> bytes:
        tick = canduril.parse_coinbase_ticker(raw)
        canduril.shard_index(tick.product, num_shards)
        return canduril.encode_tick(tick)

    return call


def cpp_normalizer_call(num_shards: int = 4) -> Callable[[bytes], bytes]:
    def call(raw: bytes) -> bytes:
        tick = canduril.decode_tick(raw)
        if not canduril.validate_tick(tick):
            raise ValueError("tick failed validation")
        canduril.shard_index(tick.product, num_shards)
        return canduril.encode_tick(tick)

    return call


def run_all(fixture_path: Path, iterations: int) -> list[BenchResult]:
    ticker_messages = load_ticker_messages(fixture_path)
    if not ticker_messages:
        raise ValueError(f"no 'ticker' messages found in {fixture_path}")
    tick_corpus = build_tick_corpus(ticker_messages)

    return [
        measure(
            "ingestor/python",
            lambda raw: baseline.ingestor_path(raw),
            ticker_messages,
            iterations,
        ),
        measure("ingestor/cpp", cpp_ingestor_call(), ticker_messages, iterations),
        measure(
            "normalizer/python",
            lambda raw: baseline.normalizer_path(raw),
            tick_corpus,
            iterations,
        ),
        measure("normalizer/cpp", cpp_normalizer_call(), tick_corpus, iterations),
    ]
