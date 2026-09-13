"""Capture live Coinbase ticker/heartbeat traffic to a JSONL fixture.

Records raw WebSocket frames (unmodified text) plus a monotonic receive
timestamp, so bench_harness.py can replay them deterministically -- either
at a controlled synthetic rate or preserving the original inter-arrival
spacing.

Usage:
    uv run python bench/capture_coinbase.py --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

import websockets

DEFAULT_WS_URI = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_PRODUCTS = ["BTC-USD", "ETH-USD", "ADA-USD"]
DEFAULT_CHANNELS = ["ticker", "heartbeat"]


async def capture(
    *,
    uri: str,
    products: list[str],
    channels: list[str],
    duration_s: float,
    out_path: Path,
) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    subscribe_message = json.dumps(
        {"type": "subscribe", "product_ids": products, "channels": channels}
    )

    count = 0
    start = time.monotonic()
    deadline = start + duration_s

    with out_path.open("w", encoding="utf-8") as f:
        async with websockets.connect(uri) as ws:
            await ws.send(subscribe_message)
            print(f"Subscribed to {channels} for {products}, capturing for {duration_s:.0f}s...")

            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break

                recv_ns = int((time.monotonic() - start) * 1_000_000_000)
                if isinstance(message, bytes):
                    message = message.decode("utf-8")
                f.write(json.dumps({"recv_ns": recv_ns, "raw": message}) + "\n")
                count += 1

                if count % 200 == 0:
                    print(f"  captured {count} messages...")

    print(f"Done: {count} messages written to {out_path}")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=DEFAULT_WS_URI)
    parser.add_argument("--products", default=",".join(DEFAULT_PRODUCTS))
    parser.add_argument("--channels", default=",".join(DEFAULT_CHANNELS))
    parser.add_argument("--duration", type=float, default=180.0, help="seconds to capture")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).with_name("fixtures") / "coinbase_capture.jsonl",
    )
    args = parser.parse_args()

    asyncio.run(
        capture(
            uri=args.uri,
            products=args.products.split(","),
            channels=args.channels.split(","),
            duration_s=args.duration,
            out_path=args.out,
        )
    )


if __name__ == "__main__":
    main()
