//
// Created by lukie on 01.06.2021.
//

#ifndef LSM_MULTITHREADER_H
#define LSM_MULTITHREADER_H


//
// Created by lukie on 01.06.2021.
//

#include "../src/hashtable/Hashtable.h"
#include <condition_variable>

template <class KeyType, class ValType, PartitionType pType>
class Multithreader {

private:

    enum Operation {
        INSERT,
        LOOKUP,
        MIXED,
        UPDATE,
        REMOVE
    };

    bool finished = false;
    std::mutex mtx;
    std::condition_variable cv;
    int bar_a, bar_b, bar_c;

    void concurr_insert(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                        std::chrono::time_point<std::chrono::high_resolution_clock> *time);

    void concurr_lookup(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                        std::chrono::time_point<std::chrono::high_resolution_clock> *time, bool expect_miss, bool expect_crash);

    void concurr_mixed(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                        std::chrono::time_point<std::chrono::high_resolution_clock> *time);

    void concurr_update(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                        std::chrono::time_point<std::chrono::high_resolution_clock> *time);

    void concurr_remove(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                        std::chrono::time_point<std::chrono::high_resolution_clock> *time);


    double do_something(Hashtable<KeyType, ValType, pType> &table, Operation op, int num_threads, long start, long end, bool expect_miss = false, bool expect_crash = false);

public:
    double insert(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end);

    double remove(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end);

    double lookup(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end, bool expect_miss = false, bool expect_crash = false);

    double mixed(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end);

    double update(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end);

};

#endif //LSM_MULTITHREADER_H
