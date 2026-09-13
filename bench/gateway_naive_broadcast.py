"""Faithful reproduction of LedgerFlux's actual gateway broadcast bug, for
a fair 'before' baseline in the client-scaling benchmark.

Mirrors services/gateway/app.py::Gateway._broadcast_tick +
ClientConnection.send_message in LedgerFlux: the tick is decoded once (for
parity with the fixed path's single canduril.decode_tick call -- this
isolates *just* the per-client serialization difference), but
json.dumps(message) happens inside send_message, which is called once per
matching client -- so the identical envelope is fully re-serialized once
per subscriber.
"""

from __future__ import annotations

import json
from typing import Any, Iterable


async def broadcast_naive(clients: Iterable[Any], raw_tick: bytes) -> int:
    """clients: objects with `.subscribed_products: set[str]` and
    `.websocket.send_text(str)` (i.e. services.gateway.app.ClientConnection).
    Returns the number of clients sent to."""
    tick_dict = json.loads(raw_tick)
    product = tick_dict["product"]
    targets = [c for c in clients if product in c.subscribed_products]

    sent = 0
    for client in targets:
        message = {"op": "incr", "data": tick_dict}
        text = json.dumps(message)  # re-serialized on every iteration -- the bug
        await client.websocket.send_text(text)
        sent += 1
    return sent
