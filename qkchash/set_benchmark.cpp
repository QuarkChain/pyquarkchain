#include <cstdint>
#include <cassert>

#include <array>
#include <chrono>
#include <iostream>
#include <limits>
#include <random>
#include <set>

#define ELEMENT_TYPE uint64_t

int main() {

    std::set<ELEMENT_TYPE> oset;

    uint32_t key_size = 64 * 1024;

    std::uniform_int_distribution<ELEMENT_TYPE>
        dist(0, std::numeric_limits<ELEMENT_TYPE>::max());
    std::uniform_int_distribution<uint64_t>
        dist64(0, std::numeric_limits<uint64_t>::max());
    std::default_random_engine generator(475);

    // Prepare a search tree
    std::cout << "Inserting to set ..." << std::endl;
    auto t_start = std::chrono::steady_clock::now();
    std::vector<ELEMENT_TYPE> inserted;
    for (uint32_t i = 0; i < key_size; i++) {
        ELEMENT_TYPE v = dist(generator);
        inserted.push_back(v);
        oset.insert(v);
    }
    auto used_time = std::chrono::steady_clock::now() - t_start;
    std::cout << "Insert time: "
              << std::chrono::duration<double, std::milli>(used_time).count()
              << std::endl;

    // Performance random delete and insert
    uint32_t count = 1 * 1024 * 1024;
    t_start = std::chrono::steady_clock::now();
    uint64_t total_count = 0;
    while (1) {
        for (uint32_t i = 0; i < count; i++) {
            uint64_t p = dist64(generator) % oset.size();
            oset.erase(inserted[p]);
            inserted[p] = dist(generator);
            oset.insert(inserted[p]);
        }

        total_count += count;
        used_time = std::chrono::steady_clock::now() - t_start;
        auto used_time_count =
            (uint64_t)std::chrono::duration<double, std::milli>(
                used_time).count();
        std::cout << total_count * 1000 / used_time_count << std::endl;
    }

    return 0;
}
