//
// Created by Lukas Vogel on 10.03.2021.
//

#include <cassert>
#include <iostream>
#include "PibenchWrapper.h"

extern "C" tree_api* create_tree(const tree_options_t& opt) {
    assert(opt.key_size == 8);
    assert(opt.value_size == 8);

    return new PibenchWrapper();
}

bool PibenchWrapper::remove(const char *key, size_t key_sz) {
    table.remove(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)));
    return true;
}

bool PibenchWrapper::find(const char *key, size_t key_sz, char *value_out) {
    return table.lookup(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), reinterpret_cast<uint8_t*>(value_out));
}

bool PibenchWrapper::insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
    table.insert(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), *reinterpret_cast<uint64_t*>(const_cast<char*>(value)), false, true);
    return true;
}

bool PibenchWrapper::update(const char *key, size_t key_sz, const char *value, size_t value_sz) {
    return insert(key, key_sz, value, value_sz);
}

int PibenchWrapper::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) {


    //table.count();
    std::map<uint64_t, uint64_t> scan_result;
    table.scan(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), scan_sz, scan_result);



    results.clear();
    results.reserve(scan_sz);


    for(auto elem : scan_result) {
        results.emplace_back(elem);
    }

    values_out = reinterpret_cast<char *>(results.data());

    return results.size();
}

long PibenchWrapper::get_size() {
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