#include "canduril/codec.hpp"

#include <simdjson.h>

#include <charconv>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>

namespace canduril {
namespace {

simdjson::dom::parser& thread_parser() {
    thread_local simdjson::dom::parser parser;
    return parser;
}

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

double parse_double(std::string_view s) {
    double value = 0.0;
    auto result = std::from_chars(s.data(), s.data() + s.size(), value);
    if (result.ec != std::errc{}) {
        throw std::runtime_error("invalid numeric field: " + std::string(s));
    }
    return value;
}

// Days since 1970-01-01 for a proleptic-Gregorian y/m/d, per Howard Hinnant's
// well-known public-domain civil_from_days/days_from_civil algorithm. Used
// instead of timegm() so this stays independent of glibc extensions/feature
// test macros under strict -std=c++20.
constexpr int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

// Coinbase "time" is RFC3339 UTC, e.g. "2024-01-01T00:00:00.123456Z".
// Mirrors datetime.fromisoformat(...).timestamp() * 1e9 in
// transform_coinbase_ticker.
int64_t parse_iso8601_ns(std::string_view s) {
    if (s.size() < 20 || s.back() != 'Z') {
        throw std::runtime_error("unsupported timestamp format: " + std::string(s));
    }

    auto digits = [&](size_t pos, size_t len) {
        int value = 0;
        for (size_t i = 0; i < len; ++i) {
            value = value * 10 + (s[pos + i] - '0');
        }
        return value;
    };

    int year = digits(0, 4);
    unsigned month = static_cast<unsigned>(digits(5, 2));
    unsigned day = static_cast<unsigned>(digits(8, 2));
    int hour = digits(11, 2);
    int minute = digits(14, 2);
    int second = digits(17, 2);

    int64_t epoch_s = days_from_civil(year, month, day) * 86400 + hour * 3600 + minute * 60 + second;

    int64_t nanos_frac = 0;
    if (s[19] == '.') {
        std::string_view frac = s.substr(20, s.size() - 21);  // strip trailing 'Z'
        std::string padded(frac);
        if (padded.size() > 9) {
            padded.resize(9);
        }
        while (padded.size() < 9) {
            padded.push_back('0');
        }
        nanos_frac = std::stoll(padded);
    }

    return epoch_s * 1'000'000'000LL + nanos_frac;
}

std::string_view get_string(simdjson::dom::element doc, const char* key) {
    std::string_view value;
    auto field = doc[key];
    if (field.error() != simdjson::SUCCESS) {
        throw std::runtime_error(std::string("missing field: ") + key);
    }
    if (field.get(value) != simdjson::SUCCESS) {
        throw std::runtime_error(std::string("field is not a string: ") + key);
    }
    return value;
}

bool try_get_string(simdjson::dom::element doc, const char* key, std::string_view& out) {
    auto field = doc[key];
    if (field.error() != simdjson::SUCCESS) {
        return false;
    }
    return field.get(out) == simdjson::SUCCESS;
}

int64_t get_int64(simdjson::dom::element doc, const char* key) {
    int64_t value = 0;
    auto field = doc[key];
    if (field.error() != simdjson::SUCCESS) {
        throw std::runtime_error(std::string("missing field: ") + key);
    }
    if (field.get(value) != simdjson::SUCCESS) {
        throw std::runtime_error(std::string("field is not an integer: ") + key);
    }
    return value;
}

// Returns std::nullopt if the key is absent OR its value is JSON null,
// mirroring how Pydantic's Optional[TradeData] round-trips through
// model_dump_json() (missing key and explicit null are both "no data").
std::optional<TradeData> get_trade_data(simdjson::dom::element parent, const char* key) {
    auto field = parent[key];
    if (field.error() != simdjson::SUCCESS) {
        return std::nullopt;
    }
    simdjson::dom::element elem;
    if (field.get(elem) != simdjson::SUCCESS) {
        throw std::runtime_error(std::string("malformed field: ") + key);
    }
    if (elem.is_null()) {
        return std::nullopt;
    }
    double px = 0.0, qty = 0.0;
    if (elem["px"].get(px) != simdjson::SUCCESS || elem["qty"].get(qty) != simdjson::SUCCESS) {
        throw std::runtime_error(std::string("malformed trade data: ") + key);
    }
    return TradeData{px, qty};
}

void append_int(std::string& out, int64_t value) {
    char buf[32];
    auto result = std::to_chars(buf, buf + sizeof(buf), value);
    out.append(buf, result.ptr);
}

void append_double(std::string& out, double value) {
    char buf[64];
    auto result = std::to_chars(buf, buf + sizeof(buf), value);
    out.append(buf, result.ptr);
}

void append_escaped(std::string& out, const std::string& s) {
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
}

void append_trade(std::string& out, const std::optional<TradeData>& trade) {
    if (!trade) {
        out += "null";
        return;
    }
    out += "{\"px\":";
    append_double(out, trade->px);
    out += ",\"qty\":";
    append_double(out, trade->qty);
    out += "}";
}

}  // namespace

std::string message_type(std::string_view raw_json) {
    auto& parser = thread_parser();
    simdjson::dom::element doc;
    if (parser.parse(raw_json.data(), raw_json.size()).get(doc) != simdjson::SUCCESS) {
        throw std::runtime_error("invalid JSON");
    }
    return std::string(get_string(doc, "type"));
}

Tick parse_coinbase_ticker(std::string_view raw_json) {
    auto& parser = thread_parser();
    simdjson::dom::element doc;
    if (parser.parse(raw_json.data(), raw_json.size()).get(doc) != simdjson::SUCCESS) {
        throw std::runtime_error("invalid JSON");
    }

    std::string_view type = get_string(doc, "type");
    if (type != "ticker") {
        throw std::runtime_error("not a ticker message");
    }

    Tick tick;
    tick.product = std::string(get_string(doc, "product_id"));
    tick.seq = get_int64(doc, "sequence");
    tick.ts_event = parse_iso8601_ns(get_string(doc, "time"));
    tick.ts_ingest = now_ns();

    std::string_view price, last_size;
    if (try_get_string(doc, "price", price) && try_get_string(doc, "last_size", last_size)) {
        tick.fields.last_trade = TradeData{parse_double(price), parse_double(last_size)};
    }

    std::string_view best_bid, best_bid_size;
    if (try_get_string(doc, "best_bid", best_bid) &&
        try_get_string(doc, "best_bid_size", best_bid_size)) {
        tick.fields.best_bid = TradeData{parse_double(best_bid), parse_double(best_bid_size)};
    }

    std::string_view best_ask, best_ask_size;
    if (try_get_string(doc, "best_ask", best_ask) &&
        try_get_string(doc, "best_ask_size", best_ask_size)) {
        tick.fields.best_ask = TradeData{parse_double(best_ask), parse_double(best_ask_size)};
    }

    return tick;
}

Tick decode_tick(std::string_view raw_json) {
    auto& parser = thread_parser();
    simdjson::dom::element doc;
    if (parser.parse(raw_json.data(), raw_json.size()).get(doc) != simdjson::SUCCESS) {
        throw std::runtime_error("invalid JSON");
    }

    Tick tick;

    int64_t v = 1;
    if (doc["v"].get(v) == simdjson::SUCCESS) {
        tick.v = static_cast<int>(v);
    }
    std::string_view type_sv;
    if (doc["type"].get(type_sv) == simdjson::SUCCESS) {
        tick.type = std::string(type_sv);
    }

    tick.product = std::string(get_string(doc, "product"));
    tick.seq = get_int64(doc, "seq");
    tick.ts_event = get_int64(doc, "ts_event");
    tick.ts_ingest = get_int64(doc, "ts_ingest");

    simdjson::dom::element fields_elem;
    if (doc["fields"].get(fields_elem) != simdjson::SUCCESS) {
        throw std::runtime_error("missing field: fields");
    }

    tick.fields.last_trade = get_trade_data(fields_elem, "last_trade");
    tick.fields.best_bid = get_trade_data(fields_elem, "best_bid");
    tick.fields.best_ask = get_trade_data(fields_elem, "best_ask");

    return tick;
}

std::string encode_tick(const Tick& tick) {
    std::string out;
    out.reserve(256);
    out += "{\"v\":";
    append_int(out, tick.v);
    out += ",\"type\":\"";
    append_escaped(out, tick.type);
    out += "\",\"product\":\"";
    append_escaped(out, tick.product);
    out += "\",\"seq\":";
    append_int(out, tick.seq);
    out += ",\"ts_event\":";
    append_int(out, tick.ts_event);
    out += ",\"ts_ingest\":";
    append_int(out, tick.ts_ingest);
    out += ",\"fields\":{\"last_trade\":";
    append_trade(out, tick.fields.last_trade);
    out += ",\"best_bid\":";
    append_trade(out, tick.fields.best_bid);
    out += ",\"best_ask\":";
    append_trade(out, tick.fields.best_ask);
    out += "}}";
    return out;
}

bool validate_tick(const Tick& tick) {
    if (tick.product.empty()) {
        return false;
    }
    if (tick.fields.last_trade && tick.fields.last_trade->px <= 0) {
        return false;
    }
    if (tick.fields.best_bid && tick.fields.best_bid->px <= 0) {
        return false;
    }
    if (tick.fields.best_ask && tick.fields.best_ask->px <= 0) {
        return false;
    }
    if (tick.fields.best_bid && tick.fields.best_ask &&
        tick.fields.best_ask->px <= tick.fields.best_bid->px) {
        return false;
    }
    return true;
}

}  // namespace canduril
