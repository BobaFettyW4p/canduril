"""Client-scaling benchmark: canduril's fixed gateway broadcast
(Gateway._broadcast_raw_tick) vs a faithful reproduction of LedgerFlux's
actual per-client-serialize bug (gateway_naive_broadcast.broadcast_naive),
against real connected WebSocket clients (not mocks) at several
client-count tiers, all subscribed to the same product (worst-case
fan-out).

Usage:
    uv run python bench/gateway_bench.py [--tiers 1,10,50,100,500] [--iterations 100] [--json-out results.json]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Awaitable, Callable, List

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import uvicorn
import websockets

import canduril
from gateway_naive_broadcast import broadcast_naive

from services.gateway.app import Gateway

HOST = "127.0.0.1"
PORT = 18766
WS_URL = f"ws://{HOST}:{PORT}/ws"


def _make_gateway() -> Gateway:
    return Gateway(
        {
            "port": PORT,
            "input_stream": "market.ticks.normalized",
            "stream_name": "market_ticks",
            "num_shards": 4,
            # Effectively unlimited -- this benchmark measures broadcast
            # serialization cost, not the rate limiter.
            "max_msgs_per_sec": 1_000_000,
            "burst": 1_000_000,
        }
    )


def _encoded_tick(seq: int, px: float) -> bytes:
    tick = canduril.Tick()
    tick.product = "BTC-USD"
    tick.seq = seq
    tick.ts_event = 1_700_000_000_000_000_000
    tick.ts_ingest = 1_700_000_000_100_000_000
    tick.fields = canduril.TickFields()
    tick.fields.last_trade = canduril.TradeData(px=px, qty=1.0)
    return canduril.encode_tick(tick)


async def _wait_for_gateway(timeout: float = 10.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    last_error: Exception | None = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            async with websockets.connect(WS_URL):
                return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.2)
    raise RuntimeError(f"gateway never became ready: {last_error}")


async def _drain_forever(ws) -> None:
    # Keeps client-side recv buffers empty so server-side sends never block
    # on TCP backpressure -- we're measuring serialization cost, not
    # client consumption speed.
    try:
        async for _ in ws:
            pass
    except Exception:
        pass


async def _connect_clients(n: int):
    conns = []
    drain_tasks = []
    for _ in range(n):
        ws = await websockets.connect(WS_URL)
        await ws.send(
            json.dumps({"op": "subscribe", "products": ["BTC-USD"], "want_snapshot": False})
        )
        conns.append(ws)
        drain_tasks.append(asyncio.create_task(_drain_forever(ws)))
    return conns, drain_tasks


async def _disconnect_clients(conns, drain_tasks) -> None:
    for task in drain_tasks:
        task.cancel()
    await asyncio.gather(*drain_tasks, return_exceptions=True)

    async def _safe_close(ws) -> None:
        try:
            await asyncio.wait_for(ws.close(), timeout=2.0)
        except Exception:
            pass

    await asyncio.gather(*(_safe_close(ws) for ws in conns))


async def measure(fn: Callable[[bytes], Awaitable[None]], iterations: int) -> dict:
    warmup = min(10, iterations)
    for i in range(warmup):
        await fn(_encoded_tick(i, 100.0))

    latencies_ns: List[int] = []
    for i in range(iterations):
        raw = _encoded_tick(1000 + i, 100.0 + i)
        t0 = time.perf_counter_ns()
        await fn(raw)
        t1 = time.perf_counter_ns()
        latencies_ns.append(t1 - t0)

    latencies_ns.sort()
    mean_ns = sum(latencies_ns) / len(latencies_ns)

    def pct(p: float) -> int:
        idx = min(len(latencies_ns) - 1, int(len(latencies_ns) * p))
        return latencies_ns[idx]

    return {"mean_us": mean_ns / 1000, "p50_us": pct(0.50) / 1000, "p99_us": pct(0.99) / 1000}


async def run_all(tiers: List[int], iterations: int) -> List[dict]:
    gateway = _make_gateway()
    server_config = uvicorn.Config(app=gateway.app, host=HOST, port=PORT, log_level="warning")
    server = uvicorn.Server(server_config)
    server_task = asyncio.create_task(server.serve())
    await _wait_for_gateway()

    results = []
    try:
        for n in tiers:
            conns, drain_tasks = await _connect_clients(n)

            deadline = asyncio.get_event_loop().time() + 10.0
            while (
                len(gateway.clients) < n
                or any(not c.subscribed_products for c in gateway.clients.values())
            ) and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.05)
            assert (
                len(gateway.clients) == n
            ), f"expected {n} connected clients, got {len(gateway.clients)}"

            async def call_naive(raw: bytes) -> None:
                await broadcast_naive(gateway.clients.values(), raw)

            async def call_fixed(raw: bytes) -> None:
                await gateway._broadcast_raw_tick(raw)

            naive_result = await measure(call_naive, iterations)
            fixed_result = await measure(call_fixed, iterations)
            results.append({"clients": n, "naive": naive_result, "fixed": fixed_result})
            print(f"  clients={n}: naive={naive_result['mean_us']:.2f}us fixed={fixed_result['mean_us']:.2f}us")

            await _disconnect_clients(conns, drain_tasks)
            deadline = asyncio.get_event_loop().time() + 5.0
            while len(gateway.clients) > 0 and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.05)
    finally:
        server.should_exit = True
        await server_task

    return results


def format_table(results: List[dict]) -> str:
    header = f"{'clients':>10}{'naive mean (us)':>20}{'fixed mean (us)':>20}{'speedup':>12}"
    lines = [header, "-" * len(header)]
    for r in results:
        speedup = r["naive"]["mean_us"] / r["fixed"]["mean_us"]
        lines.append(
            f"{r['clients']:>10}{r['naive']['mean_us']:>20.2f}"
            f"{r['fixed']['mean_us']:>20.2f}{speedup:>11.1f}x"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tiers", default="1,10,50,100,500")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    tiers = [int(t) for t in args.tiers.split(",")]
    print(f"Tiers: {tiers}, iterations per path per tier: {args.iterations}\n")

    results = asyncio.run(run_all(tiers, args.iterations))

    print()
    print(format_table(results))

    if args.json_out:
        args.json_out.write_text(json.dumps(results, indent=2))
        print(f"\nResults written to {args.json_out}")


if __name__ == "__main__":
    main()
