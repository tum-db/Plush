//
// Created by Lukas Vogel on 10.03.2021.
//

#include <cassert>
#include <iostream>
#include "PibenchWrapperVar.h"

extern "C" tree_api* create_tree(const tree_options_t& opt) {
    assert(opt.key_size == 8);
    assert(opt.value_size == 8);

    return new PibenchWrapperVar();
}

bool PibenchWrapperVar::remove(const char *key, size_t key_sz) {
    table.remove(std::span(reinterpret_cast<const std::byte*>(key), key_sz));

    return true;
}

bool PibenchWrapperVar::find(const char *key, size_t key_sz, char *value_out) {
    return table.lookup(std::span(reinterpret_cast<const std::byte*>(key), key_sz), reinterpret_cast<uint8_t*>(value_out));
}

bool PibenchWrapperVar::insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
    table.insert(std::span(reinterpret_cast<const std::byte*>(key), key_sz), std::span(reinterpret_cast<const std::byte*>(value), value_sz));
    return true;
}

bool PibenchWrapperVar::update(const char *key, size_t key_sz, const char *value, size_t value_sz) {
    return insert(key, key_sz, value, value_sz);
}

int PibenchWrapperVar::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) {
    return false;
}

long PibenchWrapperVar::get_size() {
    typedef  Hashtable<uint64_t, uint64_t, Hash> HT;

    long directory_size = 0;

    for (int layer = 0; layer < *table.cur_pmem_levels; ++layer) {
        directory_size += table.PMEM_DIRECTORY_SIZES[layer] * sizeof(HT::PMEMDirectoryEntryWithFP);
    }

    long log_size = HT::LOG_MEMORY_SIZE * table.LOG_NUM;
    long total_size = table.next_empty_bucket_idx * sizeof(HT::Bucket);

    total_size += log_size;
    total_size += directory_size;

    return total_size;
}