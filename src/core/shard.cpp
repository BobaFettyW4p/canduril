#include "canduril/shard.hpp"

#include <openssl/evp.h>

#include <cstdint>
#include <stdexcept>

namespace canduril {

int shard_index(const std::string& product, int num_shards) {
    if (num_shards <= 0) {
        throw std::invalid_argument("num_shards must be positive");
    }

    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    if (EVP_Digest(product.data(), product.size(), digest, &digest_len, EVP_sha256(),
                    nullptr) != 1) {
        throw std::runtime_error("SHA-256 digest computation failed");
    }

    // Python does int(hexdigest(), 16) % num_shards, i.e. treats the full
    // digest as a big-endian base-256 integer. Reduce incrementally so we
    // never need a bignum type; this is exactly equivalent to that modulo.
    int64_t remainder = 0;
    for (unsigned int i = 0; i < digest_len; ++i) {
        remainder = (remainder * 256 + digest[i]) % num_shards;
    }
    return static_cast<int>(remainder);
}

}  // namespace canduril
