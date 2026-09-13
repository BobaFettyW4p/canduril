"""Async NATS JetStream pull-consumer wrapper for canduril services.

Modeled on LedgerFlux's services/common/stream.py (same connect retry/backoff
and idempotent-stream-creation semantics), but deliberately schema-agnostic:
it moves raw `bytes` only. Callers (e.g. services/normalizer/app.py) apply
canduril's decode_tick/encode_tick at their own boundary.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional

from .config import NATSConfig


class NATSStreamManager:
    def __init__(self, config: NATSConfig):
        self.config = config
        self.nats_connection: Any = None
        self.jetstream: Any = None
        self._fetch_tasks: list[asyncio.Task] = []

    async def connect(self, timeout: float = 30.0) -> None:
        import nats
        import nats.js.api as jsapi

        start_time = asyncio.get_event_loop().time()
        attempt = 0
        last_error: Optional[Exception] = None

        while (asyncio.get_event_loop().time() - start_time) < timeout:
            attempt += 1
            try:
                self.nats_connection = await asyncio.wait_for(
                    nats.connect(self.config.urls), timeout=5.0
                )
                self.jetstream = self.nats_connection.jetstream()
                break
            except Exception as exc:
                last_error = exc
                elapsed = asyncio.get_event_loop().time() - start_time
                remaining = timeout - elapsed
                if remaining <= 0:
                    break
                wait_time = min(2 ** (attempt - 1), remaining)
                await asyncio.sleep(wait_time)

        if self.nats_connection is None:
            raise ConnectionError(
                f"Failed to connect to NATS at {self.config.urls} after {timeout}s: {last_error}"
            )

        # Idempotent stream creation: if it already exists, leave it alone.
        try:
            info = await self.jetstream.stream_info(self.config.stream_name)
            if info:
                return
        except Exception:
            pass

        if self.config.delete_existing:
            try:
                await self.jetstream.delete_stream(self.config.stream_name)
            except Exception:
                pass

        try:
            stream_config = jsapi.StreamConfig(
                name=self.config.stream_name,
                subjects=self.config.subjects or [f"{self.config.subject_prefix}.*"],
                duplicate_window=self.config.dedup_window_seconds,
            )
            await self.jetstream.add_stream(config=stream_config)
        except Exception as exc:
            msg = str(exc).lower()
            if "already in use" not in msg and "exists" not in msg:
                raise

    async def disconnect(self) -> None:
        for task in self._fetch_tasks:
            task.cancel()
        self._fetch_tasks.clear()
        if self.nats_connection:
            await self.nats_connection.close()

    async def publish(self, subject: str, data: bytes, msg_id: Optional[str] = None) -> Any:
        """Publish `data` to `subject`. If `msg_id` is given, it's sent as
        the Nats-Msg-Id header so JetStream's server-side dedup silently
        drops re-publishes of the same id within the stream's
        duplicate_window -- makes at-least-once redelivery idempotent.
        Returns the PubAck (`.duplicate` is set when the server deduped it).
        """
        if not self.jetstream:
            raise RuntimeError("JetStream is not connected")

        headers = None
        if msg_id is not None:
            import nats.js.api as jsapi

            headers = {jsapi.Header.MSG_ID: msg_id}

        return await self.jetstream.publish(subject, data, headers=headers)

    async def subscribe(
        self,
        subject: str,
        callback: Callable[[bytes], Awaitable[None]],
        consumer_name: Optional[str] = None,
    ) -> None:
        """Pull-subscribe to `subject`; each message's raw payload is passed
        to `callback`, which is ack'd on success. Runs as a background task."""
        if not self.jetstream:
            raise RuntimeError("JetStream is not connected")

        if consumer_name:
            sub = await self.jetstream.pull_subscribe(subject, durable=consumer_name)
        else:
            sub = await self.jetstream.pull_subscribe(subject)

        task = asyncio.create_task(self._fetch_loop(sub, callback, subject))
        self._fetch_tasks.append(task)

    async def _fetch_loop(
        self, sub: Any, callback: Callable[[bytes], Awaitable[None]], subject: str
    ) -> None:
        while True:
            try:
                msgs = await sub.fetch(10, timeout=1.0)
                for msg in msgs:
                    try:
                        await callback(msg.data)
                        await msg.ack()
                    except Exception as exc:
                        print(f"Error processing message from {subject}: {exc}")
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"Error fetching messages from {subject}: {exc}")
                await asyncio.sleep(1)
