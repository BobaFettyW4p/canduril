"""Async PostgreSQL store for snapshot (latest-state) and tick-history data.

Behavioral port of LedgerFlux's services/common/pg_store.py::
PostgresSnapshotStore for connect/close/get_latest/single-row upsert_latest
(the gateway's read path, and useful for tests seeding data) -- those
aren't the inefficient part. ensure_schema() is extended to also create
tick_history (LedgerFlux creates it via a separately-mounted k8s init
script; canduril has no such mechanism, so both tables are created here
directly). Two new batch methods exist for the snapshotter's write path --
see services/snapshotter/app.py for why per-tick writes were replaced with
a buffered, coalesced batch flush.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


@dataclass
class SnapshotRecord:
    product: str
    version: int
    last_seq: int
    ts_snapshot: int
    state: Dict[str, Any]


class PostgresSnapshotStore:
    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn_input = dsn
        self._conn: Optional[psycopg.AsyncConnection[Any]] = None

    def _build_dsn(self) -> str:
        if self._dsn_input:
            return self._dsn_input

        env_dsn = os.getenv("PG_DSN")
        if env_dsn:
            return env_dsn

        host = os.getenv("PG_HOST", "localhost")
        port = int(os.getenv("PG_PORT", "5432"))
        database = os.getenv("PG_DATABASE", "canduril")
        user = os.getenv("PG_USER", "postgres")
        password = os.getenv("PG_PASSWORD")

        auth = user
        if password:
            auth = f"{user}:{password}"

        return f"postgresql://{auth}@{host}:{port}/{database}"

    async def connect(self) -> None:
        if self._conn:
            return
        dsn = self._build_dsn()
        self._conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)

    async def ensure_schema(self) -> None:
        if not self._conn:
            await self.connect()
        assert self._conn is not None

        snapshots_stmt = """
        CREATE TABLE IF NOT EXISTS snapshots (
            product TEXT PRIMARY KEY,
            version INT NOT NULL,
            last_seq BIGINT NOT NULL,
            ts_snapshot BIGINT NOT NULL,
            state JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        tick_history_stmt = """
        CREATE TABLE IF NOT EXISTS tick_history (
            id BIGSERIAL PRIMARY KEY,
            product TEXT NOT NULL,
            sequence BIGINT NOT NULL,
            price NUMERIC(20, 8),
            bid NUMERIC(20, 8),
            ask NUMERIC(20, 8),
            volume NUMERIC(20, 8),
            ts_event BIGINT NOT NULL,
            ts_ingest BIGINT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product, sequence, timestamp)
        )
        """
        # Multiple replicas call this concurrently on startup (e.g. 4
        # snapshotter pods). CREATE TABLE IF NOT EXISTS is not atomic
        # against concurrent DDL -- two sessions can both see "doesn't
        # exist yet" and both attempt CREATE TABLE, and the loser gets a
        # UniqueViolation on the underlying pg_type catalog entry rather
        # than a clean no-op (confirmed live: this crashed a snapshotter
        # replica on startup during the Minikube deployment). An advisory
        # lock serializes the DDL so only one session creates the schema
        # at a time; everyone else waits, then correctly no-ops.
        lock_key = 892740193  # arbitrary constant, namespaced to this app
        async with self._conn.cursor() as cur:
            await cur.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
        try:
            async with self._conn.cursor() as cur:
                await cur.execute(snapshots_stmt)
                await cur.execute(tick_history_stmt)
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(ts_snapshot)"
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tick_history_product_ts "
                    "ON tick_history(product, timestamp)"
                )
        finally:
            async with self._conn.cursor() as cur:
                await cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))

    def _serializable_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        serializable_state = dict(state)
        last_update = serializable_state.get("last_update")
        try:
            from datetime import datetime

            if isinstance(last_update, datetime):
                serializable_state["last_update"] = last_update.isoformat()
        except Exception:
            pass
        return serializable_state

    async def upsert_latest(
        self,
        product: str,
        version: int,
        last_seq: int,
        ts_snapshot_ns: int,
        state: Dict[str, Any],
    ) -> None:
        if not self._conn:
            await self.connect()
        assert self._conn is not None

        upsert_stmt = """
        INSERT INTO snapshots (product, version, last_seq, ts_snapshot, state)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product)
        DO UPDATE SET
            version = EXCLUDED.version,
            last_seq = EXCLUDED.last_seq,
            ts_snapshot = EXCLUDED.ts_snapshot,
            state = EXCLUDED.state,
            updated_at = CURRENT_TIMESTAMP
        """
        async with self._conn.cursor() as cur:
            await cur.execute(
                upsert_stmt,
                (
                    product,
                    version,
                    last_seq,
                    ts_snapshot_ns,
                    Json(self._serializable_state(state)),
                ),
            )

    async def upsert_latest_batch(
        self, latest: Dict[str, Tuple[int, int, int, Dict[str, Any]]]
    ) -> None:
        """latest: product -> (version, last_seq, ts_snapshot_ns, state). One
        row per *product*, not per tick -- callers are expected to have
        already coalesced repeated ticks for the same product down to the
        last-observed value before calling this."""
        if not latest:
            return
        if not self._conn:
            await self.connect()
        assert self._conn is not None

        upsert_stmt = """
        INSERT INTO snapshots (product, version, last_seq, ts_snapshot, state)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product)
        DO UPDATE SET
            version = EXCLUDED.version,
            last_seq = EXCLUDED.last_seq,
            ts_snapshot = EXCLUDED.ts_snapshot,
            state = EXCLUDED.state,
            updated_at = CURRENT_TIMESTAMP
        """
        params = [
            (product, version, last_seq, ts_snapshot_ns, Json(self._serializable_state(state)))
            for product, (version, last_seq, ts_snapshot_ns, state) in latest.items()
        ]
        async with self._conn.cursor() as cur:
            await cur.executemany(upsert_stmt, params)

    async def insert_tick_history_batch(
        self, rows: List[Tuple[str, int, Optional[float], Optional[float], Optional[float], Optional[float], int, int]]
    ) -> None:
        """rows: (product, sequence, price, bid, ask, volume, ts_event, ts_ingest)."""
        if not rows:
            return
        if not self._conn:
            await self.connect()
        assert self._conn is not None

        insert_stmt = """
        INSERT INTO tick_history
            (product, sequence, price, bid, ask, volume, ts_event, ts_ingest)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product, sequence, timestamp) DO NOTHING
        """
        async with self._conn.cursor() as cur:
            await cur.executemany(insert_stmt, rows)

    async def get_latest(self, product: str) -> Optional[SnapshotRecord]:
        if not self._conn:
            await self.connect()
        assert self._conn is not None

        query = """
        SELECT product, version, last_seq, ts_snapshot, state
        FROM snapshots
        WHERE product = %s
        """
        async with self._conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, (product,))
            row = await cur.fetchone()
            if not row:
                return None
            return SnapshotRecord(
                product=row["product"],
                version=int(row["version"]),
                last_seq=int(row["last_seq"]),
                ts_snapshot=int(row["ts_snapshot"]),
                state=row["state"],
            )

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None
