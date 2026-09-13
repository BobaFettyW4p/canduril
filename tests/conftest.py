"""Shared fixtures for canduril's real-infra integration tests."""

from __future__ import annotations

import asyncio

import nats
import pytest
import pytest_asyncio

from services.common import PostgresSnapshotStore

NATS_URL = "nats://localhost:4222"


async def _wait_for_nats(url: str, timeout: float = 10.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    last_error: Exception | None = None
    while loop.time() < deadline:
        try:
            nc = await asyncio.wait_for(nats.connect(url), timeout=2.0)
            await nc.close()
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.5)
    pytest.skip(f"NATS not reachable at {url} (run `make nats-up` first): {last_error}")


@pytest_asyncio.fixture
async def nats_available():
    await _wait_for_nats(NATS_URL)


async def _wait_for_postgres(timeout: float = 15.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    last_error: Exception | None = None
    while loop.time() < deadline:
        store = PostgresSnapshotStore()
        try:
            await asyncio.wait_for(store.connect(), timeout=2.0)
            await store.close()
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.5)
    pytest.skip(f"Postgres not reachable (run `make pg-up` first): {last_error}")


@pytest_asyncio.fixture
async def pg_available():
    await _wait_for_postgres()
