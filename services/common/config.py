"""NATS/JetStream configuration for canduril services.

Behavioral port of LedgerFlux's services/common/config.py: same env var
names, same defaults, same precedence order. This means a canduril service
can point at the same NATS deployment LedgerFlux uses with zero config
changes -- it does not import LedgerFlux, this is an independent
reimplementation of the same contract.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_CONFIG_PATH = Path(__file__).with_name("nats.config.json")

DEFAULT_DEDUP_WINDOW_SECONDS = 120.0

# The full set of subject patterns any market-ticks-touching service should
# declare when connecting, regardless of which subject(s) it personally
# reads/writes. NATSStreamManager.connect() only creates the JetStream
# stream if one doesn't already exist -- whichever service connects first
# otherwise determines the stream's permanent subject list, silently
# dropping subjects only a *later*-connecting service needed. Every service
# passing this same constant means startup order can't cause that.
MARKET_TICKS_SUBJECTS = ["market.ticks.raw.*", "market.ticks.normalized.*"]


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if not isinstance(value, str):
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class NATSConfig:
    urls: str
    stream_name: str
    subject_prefix: str
    retention_minutes: int
    max_age_seconds: int
    delete_existing: bool = False
    # Subject patterns the JetStream stream must cover (e.g. multiple
    # namespaces feeding into/out of one stream). Defaults to a single
    # pattern derived from subject_prefix -- see stream_subjects below.
    subjects: List[str] = field(default_factory=list)
    # Server-side dedup window for the Nats-Msg-Id header, in seconds. Set
    # explicitly (rather than relying on the implicit duplicate_window=0
    # default) so services publishing with a stable per-tick id get
    # idempotent-publish protection out of the box.
    dedup_window_seconds: float = DEFAULT_DEDUP_WINDOW_SECONDS


def load_nats_config(
    stream_name: Optional[str] = None,
    subject_prefix: Optional[str] = None,
    stream_subjects: Optional[List[str]] = None,
    config_path: Optional[Path] = None,
) -> NATSConfig:
    """Load NATS/JetStream configuration shared across canduril services.

    Precedence:
    1. Explicit `stream_name`/`subject_prefix` arguments.
    2. Environment variables: NATS_URLS, JS_STREAM_NAME, JS_RETENTION_MINUTES,
       JS_MAX_AGE_SECONDS, NATS_DELETE_EXISTING.
    3. JSON config file (default: services/common/nats.config.json).
    4. Hardcoded defaults.

    `stream_subjects`, if given, is used as-is as the full list of subject
    patterns the stream must cover (e.g. separate raw/normalized
    namespaces feeding one stream). Otherwise defaults to a single pattern
    derived from the resolved `subject_prefix`.
    """
    path = config_path or DEFAULT_CONFIG_PATH
    file_config: Dict[str, Any] = {}
    if path.exists():
        try:
            file_config = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in {path}: {exc}") from exc

    urls = os.getenv("NATS_URLS", file_config.get("urls", "nats://localhost:4222"))

    retention_minutes = int(
        os.getenv("JS_RETENTION_MINUTES", file_config.get("retention_minutes", 30))
    )
    if retention_minutes <= 0:
        raise ValueError("JS_RETENTION_MINUTES must be positive")

    max_age_seconds = int(
        os.getenv(
            "JS_MAX_AGE_SECONDS",
            file_config.get("max_age_seconds", retention_minutes * 60),
        )
    )
    if max_age_seconds <= 0:
        raise ValueError("JS_MAX_AGE_SECONDS must be positive")

    resolved_stream_name = (
        stream_name or os.getenv("JS_STREAM_NAME") or file_config.get("stream_name") or "market_ticks"
    )
    if "." in resolved_stream_name:
        raise ValueError(
            f"Invalid JetStream name '{resolved_stream_name}'. "
            "Stream names cannot contain '.'. Consider using JS_SUBJECT_PREFIX for subjects."
        )

    resolved_subject_prefix = (
        subject_prefix
        or os.getenv("JS_SUBJECT_PREFIX")
        or file_config.get("subject_prefix")
        or "market.ticks"
    )

    delete_existing = _parse_bool(
        os.getenv("NATS_DELETE_EXISTING", file_config.get("delete_existing", False))
    )

    resolved_subjects = list(stream_subjects) if stream_subjects else [f"{resolved_subject_prefix}.*"]

    return NATSConfig(
        urls=str(urls),
        stream_name=str(resolved_stream_name),
        subject_prefix=str(resolved_subject_prefix),
        retention_minutes=int(retention_minutes),
        max_age_seconds=int(max_age_seconds),
        delete_existing=delete_existing,
        subjects=resolved_subjects,
        dedup_window_seconds=DEFAULT_DEDUP_WINDOW_SECONDS,
    )
