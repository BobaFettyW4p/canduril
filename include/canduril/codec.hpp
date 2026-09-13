#pragma once

#include <string>
#include <string_view>

#include "canduril/tick.hpp"

namespace canduril {

// Extracts just the "type" field from a raw Coinbase message (e.g.
// "ticker", "heartbeat", "subscriptions") without parsing the rest of the
// payload -- lets callers branch before deciding whether to invoke
// parse_coinbase_ticker at all. Throws std::runtime_error on invalid JSON
// or a missing "type" field.
std::string message_type(std::string_view raw_json);

// Parses a raw Coinbase "ticker" channel message into a Tick, mirroring
// services/ingestor/app.py::transform_coinbase_ticker + create_tick.
// Throws std::runtime_error on malformed or unexpected input.
Tick parse_coinbase_ticker(std::string_view raw_json);

// Decodes a wire-format Tick JSON payload, mirroring
// Tick.model_validate(json.loads(...)) on the normalizer's inbound path.
Tick decode_tick(std::string_view raw_json);

// Encodes a Tick back to wire-format JSON, mirroring
// tick.model_dump_json().encode().
std::string encode_tick(const Tick& tick);

// Mirrors Normalizer._validate_tick's price/spread checks. Sequence-gap
// tracking is stateful per-normalizer-instance and stays in Python.
bool validate_tick(const Tick& tick);

}  // namespace canduril
