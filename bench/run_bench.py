"""CLI entrypoint for the canduril benchmark harness.

Usage:
    uv run python bench/run_bench.py [--fixture PATH] [--iterations N] [--json-out results.json]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import bench_harness  # noqa: E402


def format_table(results: list[bench_harness.BenchResult]) -> str:
    header = (
        f"{'path':<20}{'throughput (msg/s)':>20}{'mean (us)':>12}"
        f"{'p50 (us)':>10}{'p90 (us)':>10}{'p99 (us)':>10}{'p999 (us)':>10}"
    )
    lines = [header, "-" * len(header)]
    for r in results:
        d = r.to_dict()
        lines.append(
            f"{r.label:<20}{d['throughput_msgs_per_sec']:>20,.0f}"
            f"{d['mean_us']:>12.2f}{d['p50_us']:>10.2f}{d['p90_us']:>10.2f}"
            f"{d['p99_us']:>10.2f}{d['p999_us']:>10.2f}"
        )
    return "\n".join(lines)


def summarize_speedups(results: list[bench_harness.BenchResult]) -> str:
    by_label = {r.label: r for r in results}
    pairs = [
        ("ingestor", "ingestor/python", "ingestor/cpp"),
        ("normalizer", "normalizer/python", "normalizer/cpp"),
    ]
    lines = []
    for name, py_label, cpp_label in pairs:
        if py_label in by_label and cpp_label in by_label:
            py_r, cpp_r = by_label[py_label], by_label[cpp_label]
            speedup = py_r.mean_ns / cpp_r.mean_ns
            lines.append(
                f"{name}: C++ is {speedup:.1f}x faster "
                f"(mean latency {py_r.mean_ns / 1000:.2f}us -> {cpp_r.mean_ns / 1000:.2f}us)"
            )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path(__file__).with_name("fixtures") / "sample_capture.jsonl",
    )
    parser.add_argument("--iterations", type=int, default=20000)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    print(f"Fixture: {args.fixture}")
    print(f"Iterations per path: {args.iterations}\n")

    results = bench_harness.run_all(args.fixture, args.iterations)

    print(format_table(results))
    print()
    print(summarize_speedups(results))

    if args.json_out:
        args.json_out.write_text(json.dumps([r.to_dict() for r in results], indent=2))
        print(f"\nResults written to {args.json_out}")


if __name__ == "__main__":
    main()
