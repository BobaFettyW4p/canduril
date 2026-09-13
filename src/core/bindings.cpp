#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>

#include <string>

#include "canduril/codec.hpp"
#include "canduril/shard.hpp"
#include "canduril/tick.hpp"

namespace nb = nanobind;
using namespace canduril;

NB_MODULE(_core, m) {
    m.doc() =
        "canduril native core: fast Tick codec, Coinbase ticker parsing, and shard hashing";

    nb::class_<TradeData>(m, "TradeData")
        .def(nb::init<>())
        .def(nb::init<double, double>(), nb::arg("px"), nb::arg("qty"))
        .def_rw("px", &TradeData::px)
        .def_rw("qty", &TradeData::qty)
        .def("__repr__", [](const TradeData& t) {
            return "TradeData(px=" + std::to_string(t.px) + ", qty=" + std::to_string(t.qty) +
                   ")";
        });

    nb::class_<TickFields>(m, "TickFields")
        .def(nb::init<>())
        .def_rw("last_trade", &TickFields::last_trade)
        .def_rw("best_bid", &TickFields::best_bid)
        .def_rw("best_ask", &TickFields::best_ask);

    nb::class_<Tick>(m, "Tick")
        .def(nb::init<>())
        .def_rw("v", &Tick::v)
        .def_rw("type", &Tick::type)
        .def_rw("product", &Tick::product)
        .def_rw("seq", &Tick::seq)
        .def_rw("ts_event", &Tick::ts_event)
        .def_rw("ts_ingest", &Tick::ts_ingest)
        .def_rw("fields", &Tick::fields);

    m.def(
        "message_type",
        [](nb::bytes raw) {
            return message_type(std::string_view(raw.c_str(), raw.size()));
        },
        nb::arg("raw_json"),
        "Extract the 'type' field from a raw Coinbase message without full parsing.");

    m.def(
        "parse_coinbase_ticker",
        [](nb::bytes raw) {
            return parse_coinbase_ticker(std::string_view(raw.c_str(), raw.size()));
        },
        nb::arg("raw_json"),
        "Parse a raw Coinbase 'ticker' channel message into a Tick.");

    m.def(
        "decode_tick",
        [](nb::bytes raw) { return decode_tick(std::string_view(raw.c_str(), raw.size())); },
        nb::arg("raw_json"),
        "Decode a wire-format Tick JSON payload.");

    m.def(
        "encode_tick",
        [](const Tick& tick) {
            std::string encoded = encode_tick(tick);
            return nb::bytes(encoded.data(), encoded.size());
        },
        nb::arg("tick"),
        "Encode a Tick back to wire-format JSON bytes.");

    m.def("validate_tick", &validate_tick, nb::arg("tick"),
          "Price/spread validation, mirroring Normalizer._validate_tick.");

    m.def("shard_index", &shard_index, nb::arg("product"), nb::arg("num_shards"),
          "sha256(product) % num_shards, matching services/common/util.py::shard_index.");
}
