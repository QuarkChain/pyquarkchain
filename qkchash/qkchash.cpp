#include <climits>
#include <cstdint>
#include <cstring>

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
const uint32_t ACCESS_ROUND = 64;
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
        std::array<uint64_t, 4>& result) {
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
     * Compress
     */
    for (uint32_t i = 0; i < result.size(); i++) {
        uint32_t j = i * 4;
        result[i] = fnv64(fnv64(fnv64(mix[j], mix[j + 1]), mix[j + 2]), mix[j + 3]);
    }
}

void qkc_hash_sorted_list(
        std::vector<uint64_t>& slist,
        std::array<uint64_t, 8>& seed,
        std::array<uint64_t, 4>& result) {
    std::array<uint64_t, 16> mix;
    for (uint32_t i = 0; i < mix.size(); i++) {
        mix[i] = seed[i % seed.size()];
    }

    for (uint32_t i = 0; i < ACCESS_ROUND; i ++) {
        std::array<uint64_t, 16> new_data;
        uint64_t p = fnv64(i ^ seed[0], mix[i % mix.size()]);
        for (uint32_t j = 0; j < mix.size(); j++) {
            // Find the pth element and remove it
            uint32_t idx = p % slist.size();
            new_data[j] = slist[idx];
            slist.erase(slist.begin() + idx);

            // Generate random data and insert it
            // if the vector doesn't contain it.
            p = fnv64(p, new_data[j]);
            auto it = std::lower_bound(slist.begin(), slist.end(), p);
            if (it == slist.end() || *it != p) {
                slist.insert(it, p);
            }

            // Find the next element index (ordered)
            p = fnv64(p, new_data[j]);
        }

        for (uint32_t j = 0; j < mix.size(); j++) {
            mix[j] = fnv64(mix[j], new_data[j]);
        }
    }

    /*
     * Compress
     */
    for (uint32_t i = 0; i < result.size(); i++) {
        uint32_t j = i * 4;
        result[i] = fnv64(fnv64(fnv64(mix[j], mix[j + 1]), mix[j + 2]), mix[j + 3]);
    }
}


} // quarkchain
} // org


extern "C" void *cache_create(uint64_t *cache_ptr,
                              uint32_t cache_size) {
    ordered_set_t *oset = new ordered_set_t();
    for (uint32_t i = 0; i < cache_size; i++) {
        oset->insert(cache_ptr[i]);
    }
    return oset;
}

extern "C" void *cache_copy(void *ptr) {
    return new ordered_set_t(*(ordered_set_t *)ptr);
}

extern "C" void cache_destroy(void *ptr) {
    auto cache = (ordered_set_t *)ptr;
    delete cache;
}

extern "C" void qkc_hash(void *cache_ptr,
                         uint64_t* seed_ptr,
                         uint64_t* result_ptr) {
    ordered_set_t *oset = (ordered_set_t *)cache_ptr;

    std::array<uint64_t, 8> seed;
    std::array<uint64_t, 4> result;
    std::copy(seed_ptr, seed_ptr + seed.size(), seed.begin());

    org::quarkchain::qkc_hash(*oset, seed, result);

    std::copy(result.begin(), result.end(), result_ptr);
}

void test_sorted_list() {
    std::cout << "Testing sorted list implementation" << std::endl;
    ordered_set_t oset;

    org::quarkchain::generate_init_set(
        oset, 431, org::quarkchain::INIT_SET_ENTRIES);

    std::vector<uint64_t> slist;
    for (auto v : oset) {
        slist.push_back(v);
    }

    std::uniform_int_distribution<uint64_t> dist(0, ULLONG_MAX);
    std::default_random_engine generator(475);
    std::array<uint64_t, 8> seed;
    for (uint32_t j = 0; j < 8; j++) {
        seed[j] = dist(generator);
    }

    std::array<uint64_t, 4> result0;
    std::array<uint64_t, 4> result1;
    org::quarkchain::qkc_hash(oset, seed, result0);
    org::quarkchain::qkc_hash_sorted_list(slist, seed, result1);

    for (uint32_t i = 0; i < result0.size(); i++) {
        if (result0[i] != result1[i]) {
            std::cout << "Test failed" << std::endl;
            return;
        }
    }
    std::cout << "Test passed" << std::endl;
}

void test_qkc_hash_perf() {
    ordered_set_t oset;

    auto t_start = std::chrono::steady_clock::now();
    org::quarkchain::generate_init_set(
        oset, 1, org::quarkchain::INIT_SET_ENTRIES);
    auto used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Generate time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    t_start = std::chrono::steady_clock::now();
    ordered_set_t noset = oset;
    used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Copy time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    std::uniform_int_distribution<uint64_t> dist(0, ULLONG_MAX);
    std::default_random_engine generator(475);

    t_start = std::chrono::steady_clock::now();
    uint32_t count = 1000;
    std::array<uint64_t, 8> seed;
    std::array<uint64_t, 4> result;
    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t j = 0; j < 8; j++) {
            seed[j] = dist(generator);
        }

        ordered_set_t new_oset(oset);
        org::quarkchain::qkc_hash(new_oset, seed, result);
    }
    used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Duration: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;
}

void test_qkc_hash_slist_perf() {
    ordered_set_t oset;

    auto t_start = std::chrono::steady_clock::now();
    org::quarkchain::generate_init_set(
        oset, 1, org::quarkchain::INIT_SET_ENTRIES);
    auto used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Generate time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    std::vector<uint64_t> slist;
    for (auto v : oset) {
        slist.push_back(v);
    }
    t_start = std::chrono::steady_clock::now();
    std::vector<uint64_t> nslist(slist);
    used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Copy time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    std::uniform_int_distribution<uint64_t> dist(0, ULLONG_MAX);
    std::default_random_engine generator(475);

    t_start = std::chrono::steady_clock::now();
    uint32_t count = 1000;
    std::array<uint64_t, 8> seed;
    std::array<uint64_t, 4> result;
    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t j = 0; j < 8; j++) {
            seed[j] = dist(generator);
        }

        std::vector<uint64_t> new_slist(slist);
        org::quarkchain::qkc_hash_sorted_list(new_slist, seed, result);
    }
    used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Duration: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;
}

int main(int argc, char** argv) {
    if (argc <= 1) {
        std::cout << "Must specify command in "
                     "qkc_perf, slist_test, slist_perf"
                  << std::endl;
        return -1;
    }

    if (strcmp(argv[1], "qkc_perf") == 0) {
        test_qkc_hash_perf();
    } else if (strcmp(argv[1], "slist_perf") == 0) {
        test_qkc_hash_slist_perf();
    } else if (strcmp(argv[1], "slist_test") == 0) {
        test_sorted_list();
    } else {
        std::cout << "Unrecognized command: " << argv[1] << std::endl;
        return -1;
    }

    return 0;
}
