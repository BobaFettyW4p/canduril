"""Control-plane request models for the gateway's WebSocket protocol.

These are the only Pydantic models in canduril's services -- deliberately.
They're low-frequency, client-driven, and validation ergonomics matter more
than microseconds here, unlike the per-tick hot path (which never touches
Pydantic or json.dumps at all, see Gateway._broadcast_raw_tick).
"""

from __future__ import annotations

from typing import List

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class SubscribeRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    op: str = Field(default="subscribe", validation_alias=AliasChoices("op", "operation"))
    products: List[str]
    want_snapshot: bool = True


class UnsubscribeRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    op: str = Field(default="unsubscribe", validation_alias=AliasChoices("op", "operation"))
    products: List[str]


class PingRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    op: str = Field(default="ping", validation_alias=AliasChoices("op", "operation"))
    t: int = Field(validation_alias=AliasChoices("t", "timestamp"))
