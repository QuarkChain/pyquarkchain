#include <cstdint>
#include <climits>

#include <array>
#include <chrono>
#include <iostream>
#include <random>
#include <set>

#include <ext/pb_ds/assoc_container.hpp>
#include <ext/pb_ds/tree_policy.hpp>

using namespace std;
using namespace __gnu_pbds;

typedef
tree<
    uint64_t,
    null_type,
    less<uint64_t>,
    rb_tree_tag,
    tree_order_statistics_node_update>
ordered_set_t;


namespace org {
namespace quarkchain {

const uint32_t FNV_PRIME_32 = 0x01000193;
const uint64_t FNV_PRIME_64 = 0x100000001b3ULL;
const uint32_t ACCESS_ROUND = 128;
const uint32_t INIT_SET_ENTRIES = 1024 * 64;

/*
 * 32-bit FNV function
 */
uint32_t fnv32(uint32_t v1, uint32_t v2) {
    return (v1 * FNV_PRIME_32) ^ v2;
}

/*
 * 64-bit FNV function
 */
uint64_t fnv64(uint64_t v1, uint64_t v2) {
    return (v1 * FNV_PRIME_64) ^ v2;
}

/*
 * A simplified version of generating initial set.
 * A more secure way is to use the cache generation in eth.
 */
void generate_init_set(ordered_set_t& oset, uint64_t seed, uint32_t size) {
    std::uniform_int_distribution<uint64_t> dist(0, ULLONG_MAX);
    std::default_random_engine generator(seed);

    for (uint32_t i = 0; i < size; i++) {
        uint64_t v = dist(generator);
        oset.insert(v);
    }
}

/*
 * QKC hash using ordered set.
 */
void qkc_hash(
        ordered_set_t& oset,
        std::array<uint64_t, 8>& seed,
        std::array<uint64_t, 8>& result) {
    std::array<uint64_t, 16> mix;
    for (uint32_t i = 0; i < mix.size(); i++) {
        mix[i] = seed[i % seed.size()];
    }

    for (uint32_t i = 0; i < ACCESS_ROUND; i ++) {
        std::array<uint64_t, 16> new_data;
        uint64_t p = fnv64(i ^ seed[0], mix[i % mix.size()]);
        for (uint32_t j = 0; j < mix.size(); j++) {
            // Find the pth element and remove it
            auto it = oset.find_by_order(p % oset.size());
            new_data[j] = *it;
            oset.erase(it);

            // Generate random data and insert it
            p = fnv64(p, new_data[j]);
            oset.insert(p);

            // Find the next element index (ordered)
            p = fnv64(p, new_data[j]);
        }

        for (uint32_t j = 0; j < mix.size(); j++) {
            mix[j] = fnv64(mix[j], new_data[j]);
        }
    }

    /*
     * Truncate mix directly.
     * Similar to ETH, compression and post-sha3 can be applied.
     */
    for (uint32_t i = 0; i < result.size(); i++) {
        result[i] = mix[i];
    }
}


} // quarkchain
} // org


int main() {

    ordered_set_t oset ;

    auto t_start = std::chrono::high_resolution_clock::now();
    org::quarkchain::generate_init_set(
        oset, 1, org::quarkchain::INIT_SET_ENTRIES);
    auto used_time = std::chrono::high_resolution_clock::now() - t_start;
    std::cout << "Generate time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    t_start = std::chrono::high_resolution_clock::now();
    ordered_set_t noset = oset;
    used_time = std::chrono::high_resolution_clock::now() - t_start;
    std::cout << "Copy time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    std::uniform_int_distribution<uint64_t> dist(0, ULLONG_MAX);
    std::default_random_engine generator(475);

    t_start = std::chrono::high_resolution_clock::now();
    uint32_t count = 10000;
    std::array<uint64_t, 8> seed;
    std::array<uint64_t, 8> result;
    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t j = 0; j < 8; j++) {
            seed[j] = dist(generator);
        }

        // TODO: Reset oset.
        org::quarkchain::qkc_hash(oset, seed, result);
    }
    used_time = std::chrono::high_resolution_clock::now() - t_start;
    std::cout << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    return 0;
}
