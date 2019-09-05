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

#include "util.h"

#include "qkchash_llrb.h"

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

void qkc_hash_llrb(
        org::quarkchain::LLRB<uint64_t>& cache,
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
            uint32_t idx = p % cache.size();
            new_data[j] = cache.eraseByOrder(idx);

            // Generate random data and insert it
            // if the vector doesn't contain it.
            p = fnv64(p, new_data[j]);
            cache.insert(p);

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
    void *arena0 = malloc(org::quarkchain::INIT_SET_ENTRIES *
                          org::quarkchain::LLRB<uint64_t>::getNodeSize());
    org::quarkchain::LLRB<uint64_t>* tree0 = new org::quarkchain::LLRB<uint64_t>(
        (uintptr_t)arena0,
        org::quarkchain::INIT_SET_ENTRIES *
            org::quarkchain::LLRB<uint64_t>::getNodeSize());
    for (uint32_t i = 0; i < cache_size; i++) {
        tree0->insert(cache_ptr[i]);
    }

    return tree0;
}

extern "C" void cache_destroy(void *ptr) {
    org::quarkchain::LLRB<uint64_t>* tree = (org::quarkchain::LLRB<uint64_t>*)ptr;
    free((void *)tree->getArenaBase());
    delete tree;
}

void qkc_hashx(org::quarkchain::LLRB<uint64_t>* tree0,
               uint64_t* seed_ptr,
               uint64_t* result_ptr,
               bool with_rotation_stats) {
    void *arena1 = malloc(org::quarkchain::INIT_SET_ENTRIES *
                          org::quarkchain::LLRB<uint64_t>::getNodeSize());
    org::quarkchain::LLRB<uint64_t> tree1 = tree0->copy((uintptr_t)arena1);

    std::array<uint64_t, 8> seed;
    std::array<uint64_t, 4> result;
    std::copy(seed_ptr, seed_ptr + seed.size(), seed.begin());

    org::quarkchain::qkc_hash_llrb(tree1, seed, result);

    if (with_rotation_stats) {
        std::array<uint64_t, 4> r_stats = tree1.getRotationStats();
        for (size_t i = 0; i < r_stats.size(); i++) {
            result[i] ^= r_stats[i];
        }
    }
    std::copy(result.begin(), result.end(), result_ptr);
    free(arena1);
}

extern "C" void qkc_hash(void *ptr,
                         uint64_t* seed_ptr,
                         uint64_t* result_ptr) {
    org::quarkchain::LLRB<uint64_t>* tree0 = (org::quarkchain::LLRB<uint64_t>*)ptr;
    qkc_hashx(tree0, seed_ptr, result_ptr, false);
}

extern "C" void qkc_hash_with_rotation_stats(void *ptr,
                                             uint64_t* seed_ptr,
                                             uint64_t* result_ptr) {
    org::quarkchain::LLRB<uint64_t>* tree0 = (org::quarkchain::LLRB<uint64_t>*)ptr;
    qkc_hashx(tree0, seed_ptr, result_ptr, true);
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

void test_qkc_hash_llrb() {
    std::cout << "Testing llrb implementation" << std::endl;
    ordered_set_t oset;

    org::quarkchain::generate_init_set(
        oset, 431, org::quarkchain::INIT_SET_ENTRIES);

    void *arena0 = malloc(org::quarkchain::INIT_SET_ENTRIES *
                          org::quarkchain::LLRB<uint64_t>::getNodeSize());
    void *arena1 = malloc(org::quarkchain::INIT_SET_ENTRIES *
                          org::quarkchain::LLRB<uint64_t>::getNodeSize());
    org::quarkchain::LLRB<uint64_t> tree0(
        (uintptr_t)arena0,
        org::quarkchain::INIT_SET_ENTRIES *
            org::quarkchain::LLRB<uint64_t>::getNodeSize());
    for (auto v : oset) {
        tree0.insert(v);
    }

    uint32_t trials = 100;

    std::array<uint64_t, 4> result0;
    std::array<uint64_t, 4> result1;
    for (uint32_t i = 0; i < trials; i ++) {
        std::uniform_int_distribution<uint64_t> dist(0, ULLONG_MAX);
        std::default_random_engine generator(475);
        std::array<uint64_t, 8> seed;
        for (uint32_t j = 0; j < 8; j++) {
            seed[j] = dist(generator);
        }

        auto nset = oset;
        org::quarkchain::LLRB<uint64_t> tree1 = tree0.copy((uintptr_t)arena1);
        org::quarkchain::qkc_hash(nset, seed, result0);
        org::quarkchain::qkc_hash_llrb(tree1, seed, result1);

        for (uint32_t j = 0; j < result0.size(); j++) {
            if (result0[j] != result1[j]) {
                std::cout << "Test failed at " << i << " trial" << std::endl;
                return;
            }
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
    uint32_t count = 1000, total_count = 0;
    std::array<uint64_t, 8> seed;
    std::array<uint64_t, 4> result;

    while (1) {
        for (uint32_t i = 0; i < count; i++) {
            for (uint32_t j = 0; j < 8; j++) {
                seed[j] = dist(generator);
            }

            ordered_set_t new_oset(oset);
            org::quarkchain::qkc_hash(new_oset, seed, result);
        }
        total_count += count;
        used_time = std::chrono::steady_clock::now() - t_start;
        auto used_time_ms = (uint64_t)std::chrono::duration<double, std::milli>(used_time).count();
        std::cout << total_count * 1000 / used_time_ms
                  << std::endl;
        std::cout << "Hash:" << std::endl;
        for (uint64_t v : result) {
            std::cout << v << " ";
        }
        std::cout << std::endl;
    }
}

void test_qkc_hash_llrb_perf() {
    ordered_set_t oset;
    org::quarkchain::generate_init_set(
        oset, 1, org::quarkchain::INIT_SET_ENTRIES);

    void *arena0 = malloc(org::quarkchain::INIT_SET_ENTRIES *
                          org::quarkchain::LLRB<uint64_t>::getNodeSize());
    void *arena1 = malloc(org::quarkchain::INIT_SET_ENTRIES *
                          org::quarkchain::LLRB<uint64_t>::getNodeSize());
    org::quarkchain::LLRB<uint64_t> tree0(
        (uintptr_t)arena0,
        org::quarkchain::INIT_SET_ENTRIES *
            org::quarkchain::LLRB<uint64_t>::getNodeSize());

    for (auto v : oset) {
        tree0.insert(v);
    }

    auto t_start = std::chrono::steady_clock::now();
    org::quarkchain::LLRB<uint64_t> tree1 = tree0.copy((uintptr_t)arena1);
    auto used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Copy time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    std::uniform_int_distribution<uint64_t> dist(0, ULLONG_MAX);
    std::default_random_engine generator(475);

    t_start = std::chrono::steady_clock::now();
    uint32_t count = 1000, total_count = 0;
    std::array<uint64_t, 8> seed;
    std::array<uint64_t, 4> result;

    while (1) {
        for (uint32_t i = 0; i < count; i++) {
            for (uint32_t j = 0; j < 8; j++) {
                seed[j] = dist(generator);
            }

            tree1 = tree0.copy((uintptr_t)arena1);
            org::quarkchain::qkc_hash_llrb(tree1, seed, result);
        }
        total_count += count;

        used_time = std::chrono::steady_clock::now() - t_start;
        auto used_time_ms = (uint64_t)std::chrono::duration<double, std::milli>(used_time).count();
        std::cout << total_count * 1000 / used_time_ms
                  << std::endl;

        std::cout << "Hash:" << std::endl;
        for (uint64_t v : result) {
            std::cout << v << " ";
        }
        std::cout << std::endl;
    }
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
                     "qkc_perf, llrb_perf, slist_test, slist_perf"
                  << std::endl;
        return -1;
    }

    if (strcmp(argv[1], "qkc_perf") == 0) {
        test_qkc_hash_perf();
    } else if (strcmp(argv[1], "slist_perf") == 0) {
        test_qkc_hash_slist_perf();
    } else if (strcmp(argv[1], "slist_test") == 0) {
        test_sorted_list();
    } else if (strcmp(argv[1], "llrb_perf") == 0) {
        test_qkc_hash_llrb_perf();
    } else if (strcmp(argv[1], "llrb_test") == 0) {
        test_qkc_hash_llrb();
    } else {
        std::cout << "Unrecognized command: " << argv[1] << std::endl;
        return -1;
    }

    return 0;
}
