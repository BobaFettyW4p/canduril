# canduril

**C(++) + Anduril** — a reforged [LedgerFlux](https://github.com/BobaFettyW4p/LedgerFlux): the same real-time market-data pipeline concept (Coinbase ticker feed → NATS JetStream → normalize → snapshot/serve), rebuilt with its performance-critical hot paths in C++ instead of pure Python, and with several real production bugs LedgerFlux has, found and fixed along the way.

## Why

LedgerFlux's ingestor and normalizer spend their hot loop parsing/validating/re-serializing JSON through `json.loads` + Pydantic. A benchmark against real captured Coinbase traffic (`bench/`) showed that path costing ~9.5-13us mean latency per tick. `canduril._core`, a small [nanobind](https://github.com/wjakob/nanobind) C++ extension built on [simdjson](https://github.com/simdjson/simdjson), does the same work in ~1.8-2us — a 5-6x reduction — while the surrounding service (NATS I/O, config, reconnect handling, WebSocket fan-out) stays in Python.

## Bugs found along the way

- **Normalizer self-feedback loop.** LedgerFlux's normalizer defaults to `input_stream == output_stream == "market.ticks"`. Since shard assignment is a pure function of product name, a validated republish lands back on the exact subject the normalizer itself consumes from — it reprocesses its own output forever. Confirmed live: a single tick was reprocessed 5,200+ times in 2 seconds. Because `gateway` and `snapshotter` also subscribe to that same shared subject in LedgerFlux, normalization is bypassed entirely for every downstream consumer. Fixed by splitting the namespace (`market.ticks.raw` vs `market.ticks.normalized`).
- **Stream-wide `Nats-Msg-Id` dedup collision.** JetStream's server-side dedup is scoped to the *stream*, not the subject. Publishing the same `f"{product}:{seq}"` id from both the ingestor and the normalizer (on different subjects of the same stream) meant the normalizer's publish was silently dropped as a "duplicate" of the ingestor's. Fixed by scoping the id to the publishing stage.
- **Gateway per-client serialization.** LedgerFlux's `_broadcast_tick` calls `json.dumps` once per matching subscriber for the identical tick. Fixed by decoding once for routing and reusing one pre-built wire envelope for every send — no `json.dumps`/`json.loads` round-trip in the broadcast path at all.
- **Snapshotter's per-tick Postgres writes.** LedgerFlux issues one synchronous round trip per tick, twice (history insert + latest-state upsert). Fixed with an in-memory buffer flushed on a timer/size threshold; latest-state writes are also *coalesced* — repeated ticks for the same product overwrite one dict entry, so a flush issues one upsert per product, not per tick.
- **Multi-replica NATS consumer collision.** Found only once the stack was actually deployed with multiple gateway replicas: sharing one durable consumer name per shard makes replicas NATS *competing* consumers, so a tick is delivered to only one of them — a client connected to a different pod would never see it. Fixed by scoping consumer names to the pod's `HOSTNAME`.
- **Non-atomic `CREATE TABLE IF NOT EXISTS`.** With 4 snapshotter replicas calling `ensure_schema()` concurrently on startup, one loses a race and crashes with a Postgres catalog-level `UniqueViolation`. Fixed with a Postgres advisory lock around schema creation.

## Architecture

```
Coinbase WebSocket
      |
      v
  ingestor --(C++ parse/shard/encode)--> market.ticks.raw.<shard>
                                                |
                                                v
                                          normalizer --(C++ decode/validate/shard/encode)--> market.ticks.normalized.<shard>
                                                                                                    |
                                                                        +-------------------------------+-------------------------------+
                                                                        v                                                               v
                                                                  snapshotter                                                      gateway
                                                          (batched/coalesced Postgres writes)                          (WebSocket fan-out; reads
                                                                                                                     Postgres for snapshot-on-subscribe)
```

- **`include/canduril/`, `src/core/`** — the C++ core: Tick/Snapshot structs, Coinbase ticker parsing, wire-format encode/decode, SHA-256-based sharding (bit-for-bit compatible with LedgerFlux's `shard_index`), all exposed to Python via nanobind as `canduril._core`.
- **`services/ingestor/`** — connects to Coinbase's public ticker WebSocket, publishes to `market.ticks.raw.<shard>`. Reconnects with exponential backoff on disconnect (LedgerFlux's original gives up permanently on the first drop).
- **`services/normalizer/`** — consumes `market.ticks.raw.<shard>`, validates and republishes to `market.ticks.normalized.<shard>`.
- **`services/snapshotter/`** — consumes `market.ticks.normalized.<shard>`, batches/coalesces writes into Postgres (`tick_history`, `snapshots`).
- **`services/gateway/`** — WebSocket server: subscribe/unsubscribe/ping protocol, fans out normalized ticks to subscribed clients, serves persisted snapshots from Postgres on subscribe.
- **`services/common/`** — shared NATS/JetStream plumbing (bytes-only, schema-agnostic) and the Postgres store.
- **`bench/`** — the benchmark harness, including a checked-in sample of real captured Coinbase traffic (`bench/fixtures/sample_capture.jsonl`) used both for benchmarking and as realistic test fixtures.
- **`docker/`, `k8s/`, `skaffold.yaml`** — container images and a full Minikube deployment (namespace, NATS, Postgres, all four services) matching LedgerFlux's own topology.

## Getting started

Requires [`uv`](https://docs.astral.sh/uv/) and Docker. Minikube/kubectl/Skaffold are only needed for `make up`/`make down`.

```bash
make build             # build the C++ extension (uv sync)
make test              # fast correctness tests, no external services
make test-integration  # all four services against real dockerized NATS+JetStream+Postgres
make bench             # baseline-vs-C++ latency/throughput comparison
make capture           # capture a fresh live Coinbase traffic fixture

# run the full stack locally against dockerized NATS + Postgres:
make nats-up pg-up
uv run python -m services.ingestor.app &
uv run python -m services.normalizer.app &
uv run python -m services.snapshotter.app &
uv run python -m services.gateway.app &

# or deploy the whole stack to a real Minikube cluster:
make up      # starts Minikube, builds images, deploys via Skaffold
make down    # tears everything down
```
