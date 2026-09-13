#pragma once

#include <string>

namespace canduril {

// Mirrors services/common/util.py::shard_index in LedgerFlux:
// int(sha256(product).hexdigest(), 16) % num_shards.
// Must stay byte-for-byte compatible so shard assignment matches the
// existing NATS subject scheme (market.ticks.<shard>).
int shard_index(const std::string& product, int num_shards);

}  // namespace canduril
