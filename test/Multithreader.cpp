
#include <iostream>
#include "Multithreader.h"
//#include "doctest.h"
template <class KeyType, class ValType, PartitionType pType>
double Multithreader<KeyType, ValType, pType>::insert(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end) {
    return do_something(table, Operation::INSERT, num_threads, start, end);
}
template <class KeyType, class ValType, PartitionType pType>
double Multithreader<KeyType, ValType, pType>::remove(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end) {
    return do_something(table, Operation::REMOVE, num_threads, start, end);
}

template <class KeyType, class ValType, PartitionType pType>
double Multithreader<KeyType, ValType, pType>::lookup(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end, bool expect_miss, bool expect_crash) {
    return do_something(table, Operation::LOOKUP, num_threads, start, end, expect_miss, expect_crash);
}

template <class KeyType, class ValType, PartitionType pType>
double Multithreader<KeyType, ValType, pType>::mixed(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end) {
    return do_something(table, Operation::MIXED, num_threads, start, end);
}

template <class KeyType, class ValType, PartitionType pType>
double Multithreader<KeyType, ValType, pType>::update(Hashtable<KeyType, ValType, pType> &table, int num_threads, long start, long end) {
    return do_something(table, Operation::UPDATE, num_threads, start, end);
}

template <class KeyType, class ValType, PartitionType pType>
void Multithreader<KeyType, ValType, pType>::concurr_insert(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                                   std::chrono::time_point<std::chrono::high_resolution_clock> *time) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(id, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);

    __atomic_sub_fetch(&bar_b, 1, __ATOMIC_SEQ_CST);
    while (__atomic_load_n(&bar_a, __ATOMIC_SEQ_CST) == 1); /*spinning*/


    if constexpr (std::is_integral_v<KeyType>) {
        for (uint64_t i = begin; i < end; ++i) {
            table->insert(i, i);
        }
    } else {
        for (uint64_t i = begin; i < end; ++i) {
            std::span<std::byte> key{reinterpret_cast<std::byte*>(&i), 8};
            std::span<std::byte> val{reinterpret_cast<std::byte*>(&i), 8};

            table->insert(key, val);
        }
    }

    *time = std::chrono::high_resolution_clock::now();

    if (__atomic_sub_fetch(&bar_c, 1, __ATOMIC_SEQ_CST) == 0) {
        std::unique_lock<std::mutex> lck(mtx);
        finished = true;
        cv.notify_one();
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Multithreader<KeyType, ValType, pType>::concurr_remove(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                                                     std::chrono::time_point<std::chrono::high_resolution_clock> *time) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(id, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);

    __atomic_sub_fetch(&bar_b, 1, __ATOMIC_SEQ_CST);
    while (__atomic_load_n(&bar_a, __ATOMIC_SEQ_CST) == 1); /*spinning*/


    if constexpr (std::is_integral_v<KeyType>) {
        for (uint64_t i = begin; i < end; ++i) {
            table->remove(i);
        }
    } else {
        for (uint64_t i = begin; i < end; ++i) {
            std::span<std::byte> key{reinterpret_cast<std::byte*>(&i), 8};
            table->remove(key);
        }
    }

    *time = std::chrono::high_resolution_clock::now();

    if (__atomic_sub_fetch(&bar_c, 1, __ATOMIC_SEQ_CST) == 0) {
        std::unique_lock<std::mutex> lck(mtx);
        finished = true;
        cv.notify_one();
    }
}


template <class KeyType, class ValType, PartitionType pType>
void Multithreader<KeyType, ValType, pType>::concurr_lookup(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                                   std::chrono::time_point<std::chrono::high_resolution_clock> *time, bool expect_miss, bool expect_crash) {

    bool expect_hit = !expect_miss;

    uint64_t pointer_val;
    uint64_t misses = 0;
    uint64_t wrong_vals = 0;

    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(id, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);

    __atomic_sub_fetch(&bar_b, 1, __ATOMIC_SEQ_CST);
    while (__atomic_load_n(&bar_a, __ATOMIC_SEQ_CST) == 1); /*spinning*/

    bool hit = false;

    for (uint64_t i = begin; i < end; ++i) {
        if constexpr (std::is_integral_v<KeyType>) {
            hit = table->lookup(i, (uint8_t*)&pointer_val);
        } else {
            std::span<std::byte> key{reinterpret_cast<std::byte*>(&i), 8};
            hit = table->lookup(key, (uint8_t*)&pointer_val);
        }

        if (hit != expect_hit) {
            if (!hit && expect_hit && expect_crash) {
                expect_hit = false;
                std::cout << "Switch of thread " << id << " at: " << i - begin << ", " << end - i << " to go" << std::endl;
            } else {
                ++misses;
            }
        } else if (hit && expect_hit && pointer_val != i) {
            ++wrong_vals;
            std::cout << std::endl << "Wrong: Value of " << i << " should be: " << i << ", but is: " << pointer_val << std::endl << std::endl;
        }
    }

    *time = std::chrono::high_resolution_clock::now();


    if (wrong_vals != 0) {
        throw std::runtime_error("wrong_vals == " + std::to_string(wrong_vals));
    }

    if (misses != 0) {
        throw std::runtime_error("Misses == " + std::to_string(misses));
    }

    //CHECK(misses == 0);
    //CHECK(wrong_vals == 0);
    if (__atomic_sub_fetch(&bar_c, 1, __ATOMIC_SEQ_CST) == 0) {
        std::unique_lock<std::mutex> lck(mtx);
        finished = true;
        cv.notify_one();
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Multithreader<KeyType, ValType, pType>::concurr_mixed(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                                   std::chrono::time_point<std::chrono::high_resolution_clock> *time) {

    uint64_t pointer_val;
    uint64_t misses = 0;
    uint64_t wrong_vals = 0;

    uint64_t batch_size = 1e6;

    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(id, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);

    __atomic_sub_fetch(&bar_b, 1, __ATOMIC_SEQ_CST);
    while (__atomic_load_n(&bar_a, __ATOMIC_SEQ_CST) == 1); /*spinning*/

    for (uint64_t i = begin; i < end; i += batch_size) {

        if constexpr (std::is_integral_v<KeyType>) {
            for (uint64_t j = i; j < end && j < i + batch_size; j++) {
                table->insert(j, j);
            }
        } else {
            for (uint64_t j = i; j < end && j < i + batch_size; j++) {
                std::span<std::byte> key{reinterpret_cast<std::byte*>(&j), 8};
                std::span<std::byte> val{reinterpret_cast<std::byte*>(&j), 8};

                table->insert(key, val);
            }
        }

        bool hit = false;

        for (uint64_t j = i; j < end && j < i + batch_size; j++) {
            if constexpr (std::is_integral_v<KeyType>) {
                hit = table->lookup(j, (uint8_t*)&pointer_val);
            } else {
                std::span<std::byte> key{reinterpret_cast<std::byte*>(&j), 8};
                hit = table->lookup(key, (uint8_t*)&pointer_val);
            }

            if (!hit) {
                ++misses;
                //std::cout << "Missing: " << j << std::endl;
            } else if (pointer_val != j) {
                ++wrong_vals;
                std::cout << "Wrong: Value of " << j << " should be: " << j << ", but is: " << pointer_val << std::endl;
            }
        }
    }

    *time = std::chrono::high_resolution_clock::now();

    if (misses != 0) {
        throw std::runtime_error("Misses == " + std::to_string(misses));
    }
    if (wrong_vals != 0) {
        throw std::runtime_error("wrong_vals == " + std::to_string(misses));
    }

    //CHECK(misses == 0);
    //CHECK(wrong_vals == 0);
    if (__atomic_sub_fetch(&bar_c, 1, __ATOMIC_SEQ_CST) == 0) {
        std::unique_lock<std::mutex> lck(mtx);
        finished = true;
        cv.notify_one();
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Multithreader<KeyType, ValType, pType>::concurr_update(uint64_t begin, uint64_t end, Hashtable<KeyType, ValType, pType> *table, int id,
                                  std::chrono::time_point<std::chrono::high_resolution_clock> *time) {

    uint64_t pointer_val;
    uint64_t misses = 0;
    uint64_t wrong_vals = 0;

    uint64_t batch_size = 1e3;

    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(id, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);

    __atomic_sub_fetch(&bar_b, 1, __ATOMIC_SEQ_CST);
    while (__atomic_load_n(&bar_a, __ATOMIC_SEQ_CST) == 1); /*spinning*/

    uint64_t n = 0;

    for (uint64_t i = begin; i <= end; i += batch_size) {

        int curr_end = std::min(end, i + batch_size);
        bool hit = false;

        if constexpr (std::is_integral_v<KeyType>) {

            for (uint64_t j = i; j <= curr_end ; ++j) {
                ++n;
                table->insert(id, n);
            }
            hit = table->lookup(id, (uint8_t*)&pointer_val);
        } else {
            std::span<std::byte> key{reinterpret_cast<std::byte*>(&id), 4};

            for (uint64_t j = i; j <= curr_end ; ++j) {
                ++n;
                std::span<std::byte> val{reinterpret_cast<std::byte*>(&n), 8};
                table->insert(key, val);
            }
            hit = table->lookup(key, (uint8_t*)&pointer_val);
        }

        if (!hit) {
            ++misses;
        } else if (pointer_val != n) {
            //std::cout << "Wrong val in thread: " << id << ", " << pointer_val << " instead of " << curr_end << std::endl;
            ++wrong_vals;
        }
    }

    *time = std::chrono::high_resolution_clock::now();

    if (misses != 0) {
        throw std::runtime_error("Misses == " + std::to_string(misses));
    }
    if (wrong_vals != 0) {
        throw std::runtime_error("wrong_vals == " + std::to_string(misses));
    }

    //CHECK(misses == 0);
    //CHECK(wrong_vals == 0);
    if (__atomic_sub_fetch(&bar_c, 1, __ATOMIC_SEQ_CST) == 0) {
        std::unique_lock<std::mutex> lck(mtx);
        finished = true;
        cv.notify_one();
    }
}


template <class KeyType, class ValType, PartitionType pType>
double Multithreader<KeyType, ValType, pType>::do_something(Hashtable<KeyType, ValType, pType> &table, Operation op, int num_threads, long start, long end, bool expect_miss, bool expect_crash) {
    bar_a = 1;
    bar_b = num_threads;
    bar_c = num_threads;


    std::thread *thread_array[num_threads];
    std::chrono::time_point<std::chrono::high_resolution_clock> time[num_threads];

    finished = false;

    int morsel_size = (end - start) / num_threads;

    for (uint64_t i = 0; i < num_threads; ++i) {
        uint64_t local_start = start + morsel_size * i;
        uint64_t local_end = start + morsel_size * (i + 1);

        if (i == num_threads - 1) {
            local_end = end;
        }

        if (op == Operation::INSERT) {
            thread_array[i] = new std::thread(&Multithreader::concurr_insert, this, local_start, local_end, &table, i, time + i);
        } else if (op == Operation::LOOKUP) {
            thread_array[i] = new std::thread(&Multithreader::concurr_lookup, this, local_start, local_end, &table, i, time + i, expect_miss, expect_crash);
        } else if (op == Operation::MIXED) {
            thread_array[i] = new std::thread(&Multithreader::concurr_mixed, this, local_start, local_end, &table, i, time + i);
        } else if (op == Operation::UPDATE) {
            thread_array[i] = new std::thread(&Multithreader::concurr_update, this, local_start, local_end, &table, i, time + i);
        } else if (op == Operation::REMOVE) {
            thread_array[i] = new std::thread(&Multithreader::concurr_remove, this, local_start, local_end, &table, i, time + i);

        }
    }

    while (__atomic_load_n(&bar_b, __ATOMIC_SEQ_CST) != 0);    // Spin
    std::unique_lock<std::mutex> lck(mtx);  // get the lock of condition variable

    auto lookup_begin = std::chrono::high_resolution_clock::now();

    __atomic_store_n(&bar_a, 0, __ATOMIC_SEQ_CST);  // start test
    while (!finished) {
        cv.wait(lck);  // go to sleep and wait for the wake-up from child threads
    }
    //auto lookup_end = chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        thread_array[i]->join();
        delete thread_array[i];
    }

    uint64_t longest_us = std::chrono::duration_cast<std::chrono::microseconds>(time[0] - lookup_begin).count();
    double duration = longest_us;
    for (int i = 1; i < num_threads; ++i) {
        uint64_t curr_us = std::chrono::duration_cast<std::chrono::microseconds>(time[i] - lookup_begin).count();
        duration += curr_us;
        if (curr_us > longest_us) {
            longest_us = curr_us;
        }
    }

    duration = (duration * 1.0) / (num_threads * 1e6);

    uint64_t num_ops = end - start;

    if (op == Operation::MIXED) {
        num_ops *= 2;
    }

    printf("%d threads, Time = %f s, throughput = %f ops/s\n", num_threads, duration,  num_ops / duration);

    return num_ops / duration;
}

template class Multithreader<uint64_t, uint64_t, PartitionType::Hash>;
template class Multithreader<uint64_t, uint64_t, PartitionType::Range>;

template class Multithreader<std::span<const std::byte>, std::span<const std::byte>, PartitionType::Hash>;
