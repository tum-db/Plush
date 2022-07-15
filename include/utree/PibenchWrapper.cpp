//
// Created by Lukas Vogel on 10.03.2021.
//

#include <cassert>
#include "PibenchWrapper.h"

extern "C" tree_api* create_tree(const tree_options_t& opt) {
    assert(opt.key_size == 8);
    assert(opt.value_size == 8);

    return new PibenchWrapper();
}

bool PibenchWrapper::remove(const char *key, size_t key_sz) {
    return utree.remove(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)));
}

bool PibenchWrapper::find(const char *key, size_t key_sz, char *value_out) {
    utree.search(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), value_out);
    return true;
}

bool PibenchWrapper::insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
    // TODO: This just persists the pointer, right?
    return utree.insert(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), const_cast<char *>(value));
}

bool PibenchWrapper::update(const char *key, size_t key_sz, const char *value, size_t value_sz) {
    return insert(key, key_sz, value, value_sz);
}

int PibenchWrapper::scan(const char *key, size_t key_sz, int scan_sz, char *&values_out) {
    return 0;
}

long PibenchWrapper::get_size() {
    return utree.total_size;
}