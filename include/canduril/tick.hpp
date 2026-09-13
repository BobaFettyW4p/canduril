#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace canduril {

struct TradeData {
    double px = 0.0;
    double qty = 0.0;
};

struct TickFields {
    std::optional<TradeData> last_trade;
    std::optional<TradeData> best_bid;
    std::optional<TradeData> best_ask;
};

// Mirrors services/common/models.py::Tick in LedgerFlux.
struct Tick {
    int v = 1;
    std::string type = "tick";
    std::string product;
    int64_t seq = 0;
    int64_t ts_event = 0;
    int64_t ts_ingest = 0;
    TickFields fields;
};

}  // namespace canduril
