//
// Created by Lukas Vogel on 10.03.2021.
//

#ifndef LSM_PIBENCHWRAPPER_H
#define LSM_PIBENCHWRAPPER_H


#include "../../include/pibench/tree_api.h"
#include "../hashtable/Hashtable.h"
static thread_local std::vector<std::pair<uint64_t,uint64_t>> results;

class PibenchWrapper : public tree_api {

public:
    inline bool find(const char* key, size_t key_sz, char* value_out) override;
    inline bool insert(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    inline bool update(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    inline bool remove(const char* key, size_t key_sz) override;
    inline int scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) override;
    virtual long get_size() override;

private:

    Hashtable<uint64_t, uint64_t, PartitionType::Hash> table = Hashtable<uint64_t, uint64_t, PartitionType::Hash>("/mnt/pmem0/vogel/tabletest", true);
};


#endif //LSM_PIBENCHWRAPPER_H
