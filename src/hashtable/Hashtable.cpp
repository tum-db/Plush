//
// Created by Lukas Vogel on 19.01.2021.
//
#include "Hashtable.h"
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <immintrin.h>
#include <iostream>
#include <sys/mman.h>
#include <thread>
#include <unordered_map>
#include <mutex>

#define ALIGN(ptr, align) (((ptr) + (align) - 1) & ~((align) - 1))

void *fastMemcpy(uint8_t *dest, const uint8_t *src, size_t n)
{
    char * d = reinterpret_cast<char*>(dest);
    const char * s = reinterpret_cast<const char*>(src);

    /* fall back to memcpy() if misaligned */
    if ((reinterpret_cast<uintptr_t>(d) & 31) != (reinterpret_cast<uintptr_t>(s) & 31))
        return memcpy(d, s, n);

    if (reinterpret_cast<uintptr_t>(d) & 31) {
        uintptr_t header_bytes = 32 - (reinterpret_cast<uintptr_t>(d) & 31);
        assert(header_bytes < 32);

        memcpy(d, s, std::min(header_bytes, n));

        d = reinterpret_cast<char *>(ALIGN(reinterpret_cast<uintptr_t>(d), 32));
        s = reinterpret_cast<char *>(ALIGN(reinterpret_cast<uintptr_t>(s), 32));
        n -= std::min(header_bytes, n);
    }

    for (; n >= 64; s += 64, d += 64, n -= 64) {
        __m256i *dest_cacheline = (__m256i *)d;
        __m256i *src_cacheline = (__m256i *)s;

        __m256i temp1 = _mm256_stream_load_si256(src_cacheline + 0);
        __m256i temp2 = _mm256_stream_load_si256(src_cacheline + 1);

        _mm256_stream_si256(dest_cacheline + 0, temp1);
        _mm256_stream_si256(dest_cacheline + 1, temp2);
    }

    if (n > 0)
        memcpy(d, s, n);

    _mm_mfence();

    return dest;
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::insert(KeyType key, ValType value, bool tombstone, bool log) {


    uint64_t subdivision_idx;
    uint64_t entry_idx = get_dram_directory_entry_idx(key, &subdivision_idx);

    //std::cout << entry_idx << std::endl;
    uint64_t subdivision_start = subdivision_idx * BUCKETS_PER_SUBDIVISION;
    uint64_t subdivision_end = subdivision_idx * BUCKETS_PER_SUBDIVISION + BUCKETS_PER_SUBDIVISION - 1;

    DRAMDirectoryEntry &directory_entry = dram_table[entry_idx];

    PayloadLocator val_loc;
    uint64_t key_val;

RETRY_INSERT:

    if constexpr (std::is_integral_v<KeyType>) {
        if (tombstone) {
            val_loc.pos = TOMBSTONE_MARKER;
        } else {
            val_loc.pos = value;
        }
        key_val = key;
    } else {
        if (tombstone) {
            std::span<const std::byte> tmbval{reinterpret_cast<const std::byte*>(&TOMBSTONE_MARKER), 8};
            val_loc = log_payload(key, tmbval);
        } else {
            val_loc = log_payload(key, value);
        }
        key_val = hash_key(key);
    }


    // We need to lock the mutex in exclusive mode to try for insertion
    std::unique_lock<std::mutex> lock(directory_entry.m);


    if constexpr (!std::is_integral_v<KeyType>) {
        // Maybe our log got compacted while we tried to acquire the lock? - in that case we have lost our payload
        // log entry - retry!
        if (!is_alive(val_loc, key)) {
            goto RETRY_INSERT;
        }
    }

    if (directory_entry.sizes[subdivision_end] >= KEYS_PER_BUCKET) {
        migrateDRAM(entry_idx);
    }

    int epoch = directory_entry.epoch.load(std::memory_order_relaxed);

    if (log) {
        log_to_pmem(key, val_loc, epoch);
    }

    uint64_t bucket_idx = subdivision_start;
    while (directory_entry.sizes[bucket_idx] >= KEYS_PER_BUCKET && bucket_idx < subdivision_end) {
        ++bucket_idx;
    }
    assert(bucket_idx <= subdivision_end);
    assert(directory_entry.sizes[bucket_idx] < KEYS_PER_BUCKET);
    insert_into_DRAM_bucket(entry_idx, bucket_idx, directory_entry.sizes[bucket_idx], key_val, val_loc);
    directory_entry.sizes[bucket_idx].fetch_add(1, std::memory_order::relaxed);
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::checkpoint_runner(uint64_t start_idx, uint64_t end_idx) {
    for (int i = start_idx; i < end_idx; ++i) {
        DRAMDirectoryEntry &directory_entry = dram_table[i];
        std::unique_lock<std::mutex> lock(directory_entry.m);
        migrateDRAM(i);
    }
}


template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::checkpoint(int thread_count) {
    //TODO: thread_count must currently be a power of 2.
    std::chrono::time_point<std::chrono::high_resolution_clock> start = std::chrono::high_resolution_clock::now();

    std::thread *thread_array[thread_count];

    for (uint64_t i = 0; i < thread_count ; ++i) {
        uint64_t morsel_start = (DRAM_DIRECTORY_SIZE / thread_count) * i;
        uint64_t morsel_end = (DRAM_DIRECTORY_SIZE / thread_count) * (i + 1);
        thread_array[i] = new std::thread(&Hashtable::checkpoint_runner, this, morsel_start, morsel_end);
    }

    for (auto & t : thread_array) {
        t->join();
        delete t;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> end = std::chrono::high_resolution_clock::now();
    uint64_t checkpoint_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    std::cout << "Checkpointing: " << checkpoint_us / 1000 << " ms" << std::endl;
}


template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::reinsert(uint64_t key, uint64_t val) {

    uint64_t entry_idx;
    uint64_t subdivision_idx;

    if constexpr (!std::is_integral_v<KeyType>) {
        assert(pType == PartitionType::Hash);
        entry_idx = key & (DRAM_DIRECTORY_SIZE - 1);
        subdivision_idx = (key & ((NUM_SUBDIVISIONS - 1) << DRAM_BITS)) >> DRAM_BITS;
    } else {
        entry_idx = get_dram_directory_entry_idx(key, &subdivision_idx);
    }

    uint64_t subdivision_start = subdivision_idx * BUCKETS_PER_SUBDIVISION;
    uint64_t subdivision_end = subdivision_idx * BUCKETS_PER_SUBDIVISION + BUCKETS_PER_SUBDIVISION - 1;

    DRAMDirectoryEntry &directory_entry = dram_table[entry_idx];

    std::unique_lock<std::mutex> lock(directory_entry.m);

    uint64_t bucket_idx = subdivision_start;
    // Let's check all existing buckets
    while (directory_entry.sizes[bucket_idx] > 0 && bucket_idx <= subdivision_end) {
        Bucket &bucket = dram_buckets[entry_idx * BUCKETS_PER_DIRECTORY_ENTRY + bucket_idx];
        for (int i = directory_entry.sizes[bucket_idx] - 1; i >= 0; --i) {
            if (bucket.keys[i] == key) {
                // We update and eliminate duplicates in place!
                bucket.val_ptrs[i] = val;
                return;
            }
        }
        ++bucket_idx;
    }
    // Go back to the last non-full, non-empty bucket
    if (bucket_idx > subdivision_start && directory_entry.sizes[bucket_idx-1] < KEYS_PER_BUCKET) {
        --bucket_idx;
    }
    assert(bucket_idx <= subdivision_end);
    assert(directory_entry.sizes[bucket_idx] < KEYS_PER_BUCKET);

    PayloadLocator val_loc(val);
    insert_into_DRAM_bucket(entry_idx, bucket_idx, directory_entry.sizes[bucket_idx], key, val_loc);
    directory_entry.sizes[bucket_idx].fetch_add(1, std::memory_order::relaxed);
}

template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::hash_key(const uint64_t &key) {
    uint64_t hash = std::_Hash_bytes(&key, sizeof(KeyType), 0xDEADBEEF);
    return hash;
}

template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::range_partition_key(const uint64_t &key, uint64_t level) const {
    uint64_t step = std::max(1ul,(MAX-MIN) / PMEM_DIRECTORY_SIZES[level]);
    return key / step;
}



template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::hash_key(const std::span<const std::byte> &key) {
    uint64_t hash = std::_Hash_bytes(key.data(), key.size(), 0xDEADBEEF);
    return hash;
}

template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::get_key_representation(const KeyType &key) {

    if constexpr (std::is_integral_v<KeyType>) {
        return key;
    } else {
        return hash_key(key);
    }
    //TODO: We don't check for partitioning here, as we only support hash partitioning for variable sized keys for now
}




template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::log_to_pmem(KeyType key, PayloadLocator value, int epoch) {
    //TODO: We still want to hash-partition the log so compaction is faster if we are skewed
    uint64_t log_idx = get_log_entry_idx(key);
    Log& log = logs[log_idx];

    PersistentLogState* p_state = log.persistent_state;

RETRY_LOG:

    int write_chunk_idx = p_state->write_chunk.load();
    LogChunk& cur_chunk = log.chunks[write_chunk_idx];

    size_t pos = cur_chunk.reserved.fetch_add(1, std::memory_order_relaxed);


    if (pos < MAX_LOG_ENTRIES) {
        uint64_t subdivision_idx;
        int epoch_idx = get_dram_directory_entry_idx(key, &subdivision_idx) >> LOG_NUM_BITS;
        // We don't need a more sophisticated concurrency control as we know that we have
        // a lock on that specific max epoch entry!
        if (cur_chunk.max_epochs[epoch_idx] < epoch) {
            cur_chunk.max_epochs[epoch_idx] = epoch;
        }

        auto &entry = cur_chunk.entries[pos];
        bool valid_bit = p_state->valid_bits[write_chunk_idx];

        // For ints: Just log them! For variable sized keys: First get the hash and log that!
        entry.persist(get_key_representation(key), value.pos, epoch, valid_bit);

        assert(entry.get_key() == get_key_representation(key));
        assert(entry.get_value() == value.pos);
        assert(entry.get_epoch() == epoch);
        cur_chunk.size.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    bool our_turn = !log.is_compacting.test_and_set();

    if (!our_turn) {
        log.persistent_state->write_chunk.wait(write_chunk_idx);
        goto RETRY_LOG;
    }

    if (write_chunk_idx != p_state->write_chunk.load()) {
        //Someone else already finished compacting the log while we were waiting to acquire the lock - restart!
        log.is_compacting.clear();
        log.is_compacting.notify_all();
        goto RETRY_LOG;
    }

    //Start a new chunk so all other waiters can continue ASAP
    if (p_state->next_of[p_state->write_chunk] == -1) {
        int first_free_chunk = p_state->first_free_chunk.load();
        p_state->next_of[p_state->write_chunk] = first_free_chunk;

        p_state->write_chunk = first_free_chunk;
        _mm_clflush(&p_state->write_chunk);
        _mm_sfence();

        p_state->first_free_chunk = p_state->next_of[p_state->first_free_chunk].load();
        p_state->next_of[p_state->write_chunk] = -1;
        _mm_clflush(&p_state->write_chunk);
        _mm_sfence();


        p_state->write_chunk.notify_all();

        // Let's first check if we have to prepend a new chunk where we should compact to
        if (p_state->compact_target == -1) {
            // Get new chunk and update free list
            p_state->compact_target = p_state->first_free_chunk.load();
            p_state->first_free_chunk = p_state->next_of[p_state->first_free_chunk].load();

            p_state->next_of[p_state->compact_target] = p_state->first_chunk.load();
            _mm_clflush(&p_state->write_chunk);
            _mm_sfence();

            p_state->first_chunk = p_state->compact_target.load();
            _mm_clflush(&p_state->write_chunk);
            _mm_sfence();
        }

        int chunk_to_compact_idx = p_state->next_of[p_state->compact_target];
        LogChunk& chunk_to_compact = log.chunks[chunk_to_compact_idx];

        uint64_t reserved = chunk_to_compact.reserved.load();
        assert(reserved > 0);

        if (reserved >= MAX_LOG_ENTRIES) {
            while (chunk_to_compact.size < MAX_LOG_ENTRIES);
            compact_log(log_idx);
        }

    } else {
        p_state->write_chunk = p_state->next_of[p_state->write_chunk].load();
        _mm_clflush(&p_state->write_chunk);
        _mm_sfence();
#if LOG_DEBUG
        std::cout << "Started new chunk: " << p_state->write_chunk << std::endl;
#endif
        p_state->write_chunk.notify_all();

    }
    log.is_compacting.clear();
    log.is_compacting.notify_all();
    goto RETRY_LOG;
}

template <class KeyType, class ValType, PartitionType pType>
class Hashtable<KeyType, ValType, pType>::PayloadLocator Hashtable<KeyType, ValType, pType>::log_payload
        (std::span<const std::byte> key, std::span<const std::byte> value) {


    uint64_t log_idx = get_payloadlog_entry_idx(key);
    PayloadLog& log = payload_logs[log_idx];
    uint64_t entry_size = sizeof(PayloadLogEntry) + key.size() + value.size();

    PersistentPayloadLogState* p_state = log.persistent_state;

RETRY_PAYLOAD_LOG:

    int write_chunk_idx = p_state->write_chunk.load();
    PayloadLogChunk& cur_chunk = log.chunks[write_chunk_idx];

    size_t pos = cur_chunk.reserved.fetch_add(entry_size);

    if (pos + entry_size < PAYLOAD_CHUNK_SIZE) {
        auto *entry = reinterpret_cast<PayloadLogEntry *>(cur_chunk.entries + pos);
        entry->key_len = key.size();
        entry->val_len = value.size();
        entry->flags = std::byte{0};
        memcpy(reinterpret_cast<uint8_t *>(entry + 1), (uint8_t *) key.data(), key.size());
        memcpy(reinterpret_cast<uint8_t *>(entry + 1) + key.size(), (uint8_t *) value.data(), value.size());

        for (int i = 0; i < entry_size / 64; ++i) {
            _mm_clflush(cur_chunk.entries + pos + i * 64);
        }
        _mm_sfence();
        cur_chunk.size += entry_size;

        return {log_idx, static_cast<uint64_t>(write_chunk_idx), static_cast<uint64_t>(log.persistent_state->log_epochs[write_chunk_idx].load()), pos};
    }

    if (pos >= PAYLOAD_CHUNK_SIZE) {
        //Someone else ought to compact the log
        p_state->write_chunk.wait(write_chunk_idx);
        goto RETRY_PAYLOAD_LOG;
    }

    log.m.lock();

    while (cur_chunk.size < pos) {
        //std::cout << cur_chunk.size << " is smaller than: " << pos << std::endl;
    };

    // First assign a new chunk to write to and a chunk we compact to so that we don't block for too long here.
    int next_chunk_idx = (write_chunk_idx + 1) & ((1 << PAYLOAD_CHUNK_NUM_BITS) - 1);
    while (!p_state->free[next_chunk_idx]) {
        if (next_chunk_idx == write_chunk_idx) {
            throw std::runtime_error("Payload log full! No free chunks left.");
        }
        next_chunk_idx = (next_chunk_idx +1) & ((1 << PAYLOAD_CHUNK_NUM_BITS) - 1);
    }


    // Now unfree the new log entry so that if we crash, it is guaranteed that no one else could unfree this chunk again
    // after we have already written data to it
    p_state->free[next_chunk_idx] = false;
    _mm_clflush(p_state);
    _mm_sfence();

    // Now it is ensured that nobody will overwrite any data, we can update the active chunk
    // We don't have to prepare this chunk, as we make sure it was already prepared correctly when unlinked
    p_state->write_chunk = next_chunk_idx;
    _mm_clflush(p_state);
    _mm_sfence();
    p_state->write_chunk.notify_all();

    log.free_chunk_count--;
    log.compact_order.push(write_chunk_idx);

    if (log.free_chunk_count == 0) {
        compact_payload_log(log_idx);
    }

    log.m.unlock();
    goto RETRY_PAYLOAD_LOG;
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::compact_log(uint64_t log_idx) {
    Log& log = logs[log_idx];
    PersistentLogState* p_state = log.persistent_state;

    int chunk_to_compact_idx = p_state->next_of[p_state->compact_target];

    LogChunk& chunk_to_compact = log.chunks[chunk_to_compact_idx];
    LogChunk* target_chunk = &log.chunks[p_state->compact_target];

    uint64_t read_pos = 0;
    uint64_t num_writes = 0;

    bool can_skip = true;

    // Check if we have the fast case: ALL entries of the given log are already persisted
    for (int i = 0; i < (1 << (DRAM_BITS - LOG_NUM_BITS)); ++i) {
        int dram_idx = (i << LOG_NUM_BITS) | log_idx;
        if (chunk_to_compact.max_epochs[i] >= dram_table[dram_idx].epoch) {
            can_skip = false;
            break;
        }
    }

    while (!can_skip && read_pos < chunk_to_compact.size) {
        // Check whether entry at read pos is still in volatile memory
        auto &entry = chunk_to_compact.entries[read_pos];
        uint64_t key = entry.get_key();
        uint64_t entry_idx;

        if constexpr (!std::is_integral_v<KeyType>) {
            assert(pType == PartitionType::Hash);
            entry_idx = key & (DRAM_DIRECTORY_SIZE - 1);
        } else {
            uint64_t subdivision_idx;
            entry_idx = get_dram_directory_entry_idx(key, &subdivision_idx);
        }

        bool expected_valid_bit = log.persistent_state->valid_bits[chunk_to_compact_idx];
        if (entry.get_epoch() < dram_table[entry_idx].epoch || !entry.is_valid(expected_valid_bit)) {
            // Either the epoch in DRAM is larger - this entry's content is already persisted.
            // Or it is an invalid entry because the system crashed.
            // We can ignore it in both cases.
            ++read_pos;
            continue;
        }

        // Move the (still required) log entry to the new chunk
        bool moved = move_log_entry(chunk_to_compact, *target_chunk, read_pos, log.persistent_state->valid_bits[p_state->compact_target]);

        if (!moved) {
            // We need to start a new chunk
            int next = p_state->next_of[p_state->compact_target];
            int old_target = p_state->compact_target.load();

            // Get new chunk and update free list
            int cur_first_free_chunk = p_state->first_free_chunk.load();

            p_state->first_free_chunk = p_state->next_of[p_state->first_free_chunk].load();
            _mm_clflush(p_state);
            _mm_sfence();

            p_state->compact_target = cur_first_free_chunk;
            _mm_clflush(p_state);
            _mm_sfence();

            // Add next pointer
            p_state->next_of[p_state->compact_target] = next;
            _mm_clflush(p_state);
            _mm_sfence();

            // Add previous pointer
            p_state->next_of[old_target] = p_state->compact_target.load();
            _mm_clflush(p_state);
            _mm_sfence();

            target_chunk = &log.chunks[p_state->compact_target];

            // Finally move the log entry
            move_log_entry(chunk_to_compact, *target_chunk, read_pos, log.persistent_state->valid_bits[p_state->compact_target]);
        } else {
            ++read_pos;
        }
        ++num_writes;
    }

    chunk_to_compact.size = 0;
    chunk_to_compact.reserved = 0;
    for (int i = 0; i < (1 << (DRAM_BITS - LOG_NUM_BITS)); ++i) {
        chunk_to_compact.max_epochs[i] = 0;
    }

    //RAWL -> Flip valid bit
    log.persistent_state->valid_bits[chunk_to_compact_idx] = !log.persistent_state->valid_bits[chunk_to_compact_idx];

    if (p_state->next_of[chunk_to_compact_idx] == p_state->write_chunk) {
#if LOG_DEBUG
        std::cout << "Restarting compact target!" << std::endl;
#endif
        //We've caught up with the log writing, restart compaction from the front
        //TODO: Use some metric to abort earlier
        //TODO: What about half filled log chunks?
        p_state->compact_target = -1;
    } else {
        p_state->next_of[p_state->compact_target] = p_state->next_of[chunk_to_compact_idx].load();
    }
    _mm_clflush(p_state);
    _mm_sfence();

    // Insert the now emptied chunk into the free list
    p_state->next_of[chunk_to_compact_idx] = p_state->first_free_chunk.load();
    p_state->first_free_chunk = chunk_to_compact_idx;
    _mm_clflush(p_state);
    _mm_sfence();
#if LOG_DEBUG
    std:: cout << "Compacted log chunk from: " << read_pos << " to: " << num_writes << " elements!" << std::endl;
#endif
}

template <class KeyType, class ValType, PartitionType pType>
bool Hashtable<KeyType, ValType, pType>::move_log_entry(const Hashtable<KeyType, ValType, pType>::LogChunk &source, LogChunk &target, uint64_t read_pos, bool target_valid_bit) {
    if (target.size >= MAX_LOG_ENTRIES) {
        return false;
    }

    auto &write_entry = target.entries[target.size];
    auto &read_entry = target.entries[read_pos];
    write_entry.persist(read_entry.get_key(), read_entry.get_value(), read_entry.get_epoch(), target_valid_bit);
    _mm_clflush(write_entry.content);
    _mm_sfence();
    ++target.size;
    ++target.reserved;
    return true;
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::compact_payload_log(uint64_t log_idx) {

    uint64_t read_pos = 0;

    PayloadLog& log = payload_logs[log_idx];
    size_t chunk_idx_to_compact = log.compact_order.front();
    log.compact_order.pop();

    PersistentPayloadLogState* p_state = log.persistent_state;

    PayloadLogChunk* old_chunk = &log.chunks[chunk_idx_to_compact];
    PayloadLogChunk* new_chunk = &log.chunks[p_state->compact_chunk];

#if LOG_DEBUG
    std::cout << "Compacting to chunk: " << p_state->compact_chunk << std::endl;
#endif

    while (read_pos < old_chunk->size) {
        auto *source_entry = reinterpret_cast<PayloadLogEntry *>(old_chunk->entries + read_pos);
        size_t entry_size = sizeof(PayloadLogEntry) + source_entry->key_len + source_entry->val_len;

        if (IMM_MARK_INVALID && (static_cast<uint8_t>(source_entry->flags) & 0b1)) {
                // Entry is invalid, we can skip it
                read_pos += entry_size;
                continue;
        }

        if constexpr (std::is_integral_v<KeyType>) {
            assert(false);
        } else {
            std::span<std::byte> key_span{reinterpret_cast<std::byte *>(source_entry + 1), source_entry->key_len};
            uint64_t hash = hash_key(key_span);
            uint64_t entry_idx = hash & (DRAM_DIRECTORY_SIZE - 1);
            DRAMDirectoryEntry* entry = &dram_table[entry_idx];
            //Make sure nobody migrates anything while we are compacting
            entry->m.lock();
            std::optional<LookupResult> result= lookup_internal(key_span);

            assert(result.has_value());

            if (result && result->locator.get_offset() == read_pos) {
                // We want to keep this entry
                size_t size = sizeof(PayloadLogEntry) + source_entry->key_len + source_entry->val_len;
                if (new_chunk->size + size >= PAYLOAD_CHUNK_SIZE) {
                    // We need to start a new chunk to compact to
                    int next_chunk_idx = (p_state->compact_chunk + 1) & ((1 << PAYLOAD_CHUNK_NUM_BITS) - 1);

                    while (!p_state->free[next_chunk_idx]) {
                        if (next_chunk_idx == p_state->compact_chunk) {
                            throw std::runtime_error("Payload log full! No free chunks left.");
                        }
                        next_chunk_idx = (next_chunk_idx + 1) & ((1 << PAYLOAD_CHUNK_NUM_BITS) - 1);
                    }
                    p_state->free[next_chunk_idx] = false;
                    _mm_clflush(p_state);
                    _mm_sfence();

                    p_state->compact_chunk = next_chunk_idx;
                    _mm_clflush(p_state);
                    _mm_sfence();

                    new_chunk = &log.chunks[next_chunk_idx];
                }

                auto *target_entry = reinterpret_cast<PayloadLogEntry *>(new_chunk->entries + new_chunk->size);
                move_payload_log_entry(source_entry, target_entry);

                PayloadLocator new_locator(log_idx, p_state->compact_chunk, log.persistent_state->log_epochs[p_state->compact_chunk], new_chunk->size);

                if (result->is_volatile) {
                    //Is tombstone has to be false since otherwise the lookup wouldn't have found the element!
                    log_to_pmem(key_span, new_locator, entry->epoch);
                }

                if (!result->storage_location->val_ptrs[result->offset].compare_exchange_strong(result->locator.pos, new_locator.pos)) {
                    throw std::runtime_error("Someone changed a payload entry while not allowed to!");
                }
                if (!result->is_volatile) {
                    _mm_clflush(result->storage_location->val_ptrs + result->offset);
                }

                new_chunk->size += entry_size;
                new_chunk->reserved += entry_size;
            }
            read_pos += entry_size;
            entry->m.unlock();
        }
    }
    _mm_sfence();


    //Invalidate all old entries
    ++log.persistent_state->log_epochs[chunk_idx_to_compact];
    _mm_clflush(log.persistent_state);
    _mm_sfence();

    //Zero log, since it's mmap'd and the size was hopefully set responsibly, it should be 4KB-aligned.
    __m512i zero = _mm512_set1_epi64(0);
    for (size_t offset = 0; offset < PAYLOAD_CHUNK_SIZE; offset += 64) {
        _mm512_stream_si512(reinterpret_cast<__m512i *>(old_chunk->entries + offset), zero);
    }
    _mm_sfence();
    std:: cout << "Compacted log " << log_idx << " from: " << old_chunk->size / (1024.0 * 1024) << "MiB to: " << new_chunk->size / (1024.0 * 1024)  << " MiB!" << std::endl;

    // Finally release the old chunk
    log.persistent_state->free[chunk_idx_to_compact] = true;
    log.free_chunk_count++;
    old_chunk->size = 0;
    old_chunk->reserved = 0;
    _mm_clflush(log.persistent_state);
    _mm_sfence();
}


template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::move_payload_log_entry(PayloadLogEntry* source, PayloadLogEntry* target) {
    size_t size = sizeof(PayloadLogEntry) + source->key_len + source->val_len;
    memcpy(reinterpret_cast<uint8_t *>(target),
               reinterpret_cast<uint8_t *>(source),
               size);

    for (int i = 0; i < size / 64; ++i) {
        _mm_clflush(target + i * 64);
    }
    _mm_sfence();
}


template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::recover_from_log() {

    std::chrono::time_point<std::chrono::high_resolution_clock> start = std::chrono::high_resolution_clock::now();
    // Recalculate the epochs for DRAM
    for (int dram_idx = 0; dram_idx < DRAM_DIRECTORY_SIZE; ++dram_idx) {
        int min_epoch = 10000;
        int max_epoch = 0;

        for (long counter = 0; counter < (1l << (PMEM_BITS - DRAM_BITS)); ++counter) {
            long directory_idx = (dram_idx | (counter << DRAM_BITS)) & (PMEM_DIRECTORY_SIZES[0] - 1);

            PMEMDirectoryEntry *entry = get_directory_entry(0, directory_idx);
            if (entry->epoch < min_epoch) {
                min_epoch = entry->epoch;
            }
            if (entry->epoch > max_epoch) {
                max_epoch = entry->epoch;
            }
        }

        dram_table[dram_idx].epoch = min_epoch + 1;
    }

    // This is actually quite hacky, but also elegant in a weird way
    // Since we can distinguish between garbage and a real payload log entry when compacting
    // (payload log entry real <=> a living payload locator points to it)
    // We just assume that each chunk currently in  use is filled to the max.
    // We then filter out all the chunk entries with the next compaction.
    // The space wastage stays acceptable as we only overestimate the size of two entries:
    // The one we last inserted into and the one we compacted to.
    if constexpr (!std::is_integral_v<KeyType>) {
        for (int log_idx = 0; log_idx < PAYLOAD_LOG_NUM; ++log_idx) {
            PayloadLog &log = payload_logs[log_idx];
            PersistentPayloadLogState *p_state = log.persistent_state;

            for (int chunk_idx = 0; chunk_idx < CHUNKS_PER_PAYLOAD_LOG; ++chunk_idx) {
                if (!p_state->free[chunk_idx]) {
                    log.chunks[chunk_idx].size = PAYLOAD_CHUNK_SIZE-1;
                    log.chunks[chunk_idx].reserved = PAYLOAD_CHUNK_SIZE-1;
                }
            }
        }
    }

    std::thread *thread_array[LOG_NUM];
    std::thread *filter_recovery_thread_array[FILTER_RECOVERY_THREAD_NUM];
    uint64_t max_bucket_ids[FILTER_RECOVERY_THREAD_NUM];

    for (uint64_t i = 0; i < LOG_NUM; ++i) {
        thread_array[i] = new std::thread(&Hashtable::recover_single_log, this, i);
    }

    for (auto & t : thread_array) {
        t->join();
        delete t;
    }
    std::chrono::time_point<std::chrono::high_resolution_clock> log_end = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < FILTER_RECOVERY_THREAD_NUM; ++i) {
        filter_recovery_thread_array[i] = new std::thread(&Hashtable::recover_fingerprints_and_allocator_status, this, i, &*max_bucket_ids);
    }

    for (auto & t : filter_recovery_thread_array) {
        t->join();
        delete t;
    }


    for (int i = 0; i < FILTER_RECOVERY_THREAD_NUM; ++i) {
        if (max_bucket_ids[i] > next_empty_bucket_idx) {
            next_empty_bucket_idx = max_bucket_ids[i];
        }
    }

    if (next_empty_bucket_idx > 0) {
        next_empty_bucket_idx.fetch_add(1, std::memory_order::relaxed);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> filter_recovery_end = std::chrono::high_resolution_clock::now();

    uint64_t log_us = std::chrono::duration_cast<std::chrono::microseconds>(log_end - start).count();
    uint64_t filter_us = std::chrono::duration_cast<std::chrono::microseconds>(filter_recovery_end - log_end).count();
    uint64_t total_us = std::chrono::duration_cast<std::chrono::microseconds>(filter_recovery_end - start).count();

#if LOG_METRICS
    std::cout << "[Recovery]";
    std::cout << "(Logs: " << log_us / 1000 << " ms), ";
    std::cout << "(Filters: " << filter_us / 1000 << " ms), ";
    std::cout << "(Total: " << total_us / 1000 << " ms)" << std::endl;
#endif

}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::recover_single_log(uint64_t log_idx) {
    Log &log = logs[log_idx];

    // Just replay ALL chunks.
    // This is fine, even if we crashed during compaction:
    // If we were compacting, a valid entry might be duplicated: At its old position and at the compaction target.
    // This, however, is not an issue, as the reinsert()-Method will check all existing DRAM entries and will just skip
    // duplicates.
    // If we crashed during recovery, this is also not an issue, as we don't write anything to persistent memory
    // during recovery. Recovery therefore is idempotent by design.
    int chunk_idx = log.persistent_state->first_chunk;

    while (chunk_idx != -1) {
        LogChunk& cur_chunk = log.chunks[chunk_idx];
        cur_chunk.reserved = 0;
        cur_chunk.size = 0;

        for (int i = 0; i < (1 << (DRAM_BITS - LOG_NUM_BITS)); ++i) {
            cur_chunk.max_epochs[i] = INT_MAX; // We assume everything has to be recovered -> amortize over runtime
        }
        bool expected_valid_bit = log.persistent_state->valid_bits[chunk_idx];

        for (int read_pos = 0; read_pos < MAX_LOG_ENTRIES; ++read_pos) {
            auto &log_entry = cur_chunk.entries[read_pos];
            uint64_t key = log_entry.get_key();

            uint64_t entry_idx;
            if constexpr (!std::is_integral_v<KeyType>) {
                assert(pType == PartitionType::Hash);
                entry_idx = key & (DRAM_DIRECTORY_SIZE - 1);
            } else {
                uint64_t subdivision_idx;
                entry_idx = get_dram_directory_entry_idx(key, &subdivision_idx);
            }


            if (log_entry.is_valid(expected_valid_bit)) {
                if (log_entry.get_epoch() >= dram_table[entry_idx].epoch) {
                    reinsert(log_entry.get_key(), log_entry.get_value());
                }
                ++cur_chunk.reserved;
                ++cur_chunk.size;
            }
        }

        if (cur_chunk.reserved > 0) {
#if LOG_DEBUG
            std::cout << "Recovered " << cur_chunk.reserved << " entries @ " << log_idx << ", " << chunk_idx << std::endl;
#endif
        }
        chunk_idx = log.persistent_state->next_of[chunk_idx];
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::recover_fingerprints_and_allocator_status(uint64_t thread_idx, uint64_t* allocator_status_array) {


    int max_bucket_idx = 0;
    int level = 0;

    while (level < cur_pmem_levels->load() && level <= MAX_DRAM_FILTER_LEVEL) {
        int step = PMEM_DIRECTORY_SIZES[level] / FILTER_RECOVERY_THREAD_NUM;

        uint64_t start_idx = thread_idx * step;
        uint64_t end_idx = (thread_idx + 1) * step;
        for (uint64_t directory_idx = start_idx; directory_idx < end_idx; ++directory_idx) {
            PMEMDirectoryEntry *entry = get_directory_entry(level, directory_idx);

            // Check if we have to populate DRAM filters
            int elems_remaining = entry->size;
            int bucket_idx = 0;

            while (elems_remaining > 0) {
                Bucket *bucket;
                int count = std::min(elems_remaining, KEYS_PER_BUCKET);

                if (level <= MAX_BUCKET_PREALLOC_LEVEL) {
                    bucket = &get_prealloced_bucket(level, directory_idx, bucket_idx);
                } else {
                    uint64_t bucket_pointer = entry->bucket_pointers[bucket_idx];
                    bucket = &get_bucket(bucket_pointer);
                }
                insert_into_filter(reinterpret_cast<const uint64_t *>(bucket->keys), count, level, directory_idx, bucket_idx);
                ++bucket_idx;
                elems_remaining -= count;
            }
            assert(elems_remaining == 0);


            // Check if we have to find the largest used bucket idx
            if (level > MAX_BUCKET_PREALLOC_LEVEL) {
                for (uint64_t cur_idx: entry->bucket_pointers) {
                    if (cur_idx > max_bucket_idx) {
                        max_bucket_idx = cur_idx;
                    }
                }
            }
        }
        ++level;
    }

    // We now only have to find the largest bucket for the remaining levels
    while (level < cur_pmem_levels->load()) {
        if (level > MAX_BUCKET_PREALLOC_LEVEL) {
            int step = PMEM_DIRECTORY_SIZES[level] / FILTER_RECOVERY_THREAD_NUM;

            uint64_t start_idx = thread_idx * step;
            uint64_t end_idx = (thread_idx + 1) * step;
            for (uint64_t directory_idx = start_idx; directory_idx < end_idx; ++directory_idx) {
                PMEMDirectoryEntry *entry = get_directory_entry(level, directory_idx);

                for (uint64_t cur_idx: entry->bucket_pointers) {
                    if (cur_idx > max_bucket_idx) {
                        max_bucket_idx = cur_idx;
                    }

                }
            }

        }
        ++level;
    }

    allocator_status_array[thread_idx] = max_bucket_idx;
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::insert_into_DRAM_bucket(uint64_t entry_idx, int bucket_idx, int pos, uint64_t key_hash,
                                        PayloadLocator value) {

    Bucket &bucket = dram_buckets[entry_idx * BUCKETS_PER_DIRECTORY_ENTRY + bucket_idx];
    assert(entry_idx < DRAM_DIRECTORY_SIZE);
    assert(bucket_idx < BUCKETS_PER_DIRECTORY_ENTRY);
    bucket.keys[pos].store(key_hash, std::memory_order_relaxed);
    bucket.val_ptrs[pos].store(value.pos, std::memory_order_relaxed);
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::migrateDRAM(uint64_t entry_idx) {
    const int fanout = 1 << (PMEM_BITS - DRAM_BITS);
    uint64_t keys[fanout * MAX_VALUES_PER_BUCKET_AFTER_REHASH];
    uint64_t values[fanout * MAX_VALUES_PER_BUCKET_AFTER_REHASH];
    int sizes[fanout];
    memset(sizes, 0, sizeof(int) * fanout);

    DRAMDirectoryEntry *entry = &dram_table[entry_idx];
    int epoch = entry->epoch.load(std::memory_order_relaxed);

    for (int bucket_idx = 0; bucket_idx < BUCKETS_PER_DIRECTORY_ENTRY; ++bucket_idx) {
        Bucket &bucket = dram_buckets[entry_idx * BUCKETS_PER_DIRECTORY_ENTRY + bucket_idx];
        rehash(bucket, entry->sizes[bucket_idx],0, keys, values, sizes);
    }

    bulk_level_insert(0, epoch, keys, values, sizes);
    entry->epoch.fetch_add(1, std::memory_order_relaxed);

    for (auto idx = 0; idx < BUCKETS_PER_DIRECTORY_ENTRY; ++idx) {
        entry->sizes[idx].store(0);
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::migrate(uint64_t directory_entry_idx, int source_level, int target_level) {
    PMEMDirectoryEntry *entry = get_directory_entry(source_level, directory_entry_idx);

    uint64_t keys[BUCKETS_PER_DIRECTORY_ENTRY * MAX_VALUES_PER_BUCKET_AFTER_REHASH];
    uint64_t values[BUCKETS_PER_DIRECTORY_ENTRY * MAX_VALUES_PER_BUCKET_AFTER_REHASH];
    int sizes[BUCKETS_PER_DIRECTORY_ENTRY] = {0};

    int rehashed = 0;
    int bucket_idx = 0;
    int epoch = entry->epoch.load();

    while (rehashed < entry->size) {
        int to_rehash = std::min(KEYS_PER_BUCKET, entry->size - rehashed);

        Bucket *bucket;

        if (source_level <= MAX_BUCKET_PREALLOC_LEVEL) {
            bucket = &get_prealloced_bucket(source_level,directory_entry_idx, bucket_idx);
        } else {
            uint64_t bucket_pointer = entry->bucket_pointers[bucket_idx];
            bucket = &get_bucket(bucket_pointer);
        }

        rehash(*bucket, to_rehash, target_level, keys, values, sizes);

        rehashed += to_rehash;
        ++bucket_idx;
    }

    // Try to bulk insert all values into the new level
    bulk_level_insert(target_level, epoch, keys, values, sizes);


    DirectoryFingerprint *fingerprint;

    if (source_level <= MAX_DRAM_FILTER_LEVEL) {
        fingerprint = &dram_fingerprints[source_level][directory_entry_idx];
    } else {
        fingerprint = &static_cast<PMEMDirectoryEntryWithFP*>(entry)->fingerprint;
    }


    // Zero the fingerprints
    __m512i zero = _mm512_set1_epi64(0);
    _mm512_stream_si512(reinterpret_cast<__m512i *>(fingerprint->bucket_fingerprints), zero);
    _mm512_stream_si512(reinterpret_cast<__m512i *>(fingerprint->bucket_fingerprints + 4), zero);
    _mm512_stream_si512(reinterpret_cast<__m512i *>(fingerprint->bucket_fingerprints + 8), zero);
    _mm512_stream_si512(reinterpret_cast<__m512i *>(fingerprint->bucket_fingerprints + 12), zero);



    entry->size = 0;
    _mm_clflushopt(&entry->size);

    _mm_sfence();

    //We don't need to clear the tombstones as we overwrite them when inserting anyway
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::bulk_level_insert(int level, int epoch, const uint64_t *keys, const uint64_t *values, const int *sizes) {

    if (level >= *cur_pmem_levels) {
        bool bla = cur_pmem_levels->compare_exchange_strong(level, level +1);

        if (bla) {
#if LOG_DEBUG
            std::cout << "Increased level number to: " << *cur_pmem_levels << std::endl;
#endif
        }
    }

    int fanout = level == 0 ? 1 << (PMEM_BITS - DRAM_BITS) : 1 << FANOUT_BITS;

    for (int i = 0; i < fanout; ++i) {
        if (sizes[i] == 0) {
            continue;
        }
        assert (sizes[i] <= MAX_VALUES_PER_BUCKET_AFTER_REHASH);


        const uint64_t *key = keys + i * MAX_VALUES_PER_BUCKET_AFTER_REHASH;
        uint64_t directory_entry_idx = get_pmem_directory_entry_idx(level, *key);
        PMEMDirectoryEntry *directory_entry = get_directory_entry(level, directory_entry_idx);

        if (directory_entry->size + sizes[i] > BUCKETS_PER_DIRECTORY_ENTRY * KEYS_PER_BUCKET) {
            migrate(directory_entry_idx, level, level + 1);
        }



        int elems_inserted = try_bulk_insert(level, directory_entry_idx, epoch,
                                             keys + i * MAX_VALUES_PER_BUCKET_AFTER_REHASH,
                                             values + i * MAX_VALUES_PER_BUCKET_AFTER_REHASH, sizes[i]);
        assert (elems_inserted == sizes[i]);
    }
}
template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::get_key_or_hash(const uint64_t key) {
    if constexpr (std::is_integral_v<KeyType>) {
        return hash_key(key);
    } else {
        return key;
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::rehash(Bucket &bucket, int size, int level, uint64_t *keys, uint64_t *values, int *sizes) {
    for (int key_idx = 0; key_idx < size; ++key_idx) {

        uint64_t key = *reinterpret_cast<const uint64_t *>(bucket.keys + key_idx);


        uint64_t directory_idx = get_pmem_directory_entry_idx(level, key);
        if constexpr (pType == PartitionType::Hash) {
            int shift = level == 0 ? DRAM_BITS : PMEM_BITS + FANOUT_BITS * (level - 1);
            directory_idx = directory_idx >> shift;
        } else {
            uint64_t divisor = level == 0 ? 1 << (PMEM_BITS - DRAM_BITS) : 1 << FANOUT_BITS;

            uint64_t base_bucket = (directory_idx / divisor) * divisor;
            if (level >= 1 && base_bucket % 16 != 0) {
                std::cout << "Error!" << std::endl;
            }
            //std::cout << level << ", " << directory_idx << std::endl;
            directory_idx -= base_bucket;
            assert(directory_idx <= 15);
        }


        uint64_t *first = keys + (directory_idx * MAX_VALUES_PER_BUCKET_AFTER_REHASH);
        uint64_t *last = keys + (directory_idx * MAX_VALUES_PER_BUCKET_AFTER_REHASH + sizes[directory_idx]);
        uint64_t *pos = std::find(first, last, bucket.keys[key_idx]);


        if (pos != last) {
            long offset = pos - first;

            if constexpr (std::is_integral_v<KeyType>) {

                values[directory_idx * MAX_VALUES_PER_BUCKET_AFTER_REHASH + offset] = bucket.val_ptrs[key_idx];
                continue;
            } else {
                PayloadLocator locatorA(bucket.val_ptrs[key_idx]);
                PayloadLocator locatorB(values[directory_idx * MAX_VALUES_PER_BUCKET_AFTER_REHASH + offset]);

                if (locatorA.get_epoch() != payload_logs[locatorA.get_log_id()].persistent_state->log_epochs[locatorA.get_chunk_id()]) {
                    // Entry is no longer reachable as it has been garbage collected in the log -> skip it
                    continue;
                }
                // locatorB must point to a valid entry as otherwise it wouldn't have been added to the values to migrate in the first place

                auto *entryA = reinterpret_cast<PayloadLogEntry *>(payload_logs[locatorA.get_log_id()].chunks[locatorA.get_chunk_id()].entries + locatorA.get_offset());
                auto *entryB = reinterpret_cast<PayloadLogEntry *>(payload_logs[locatorB.get_log_id()].chunks[locatorB.get_chunk_id()].entries + locatorB.get_offset());


                if (entryA->key_len == entryB->key_len && memcmp(entryA+1, entryB+1, entryA->key_len) == 0) {
                    values[directory_idx * MAX_VALUES_PER_BUCKET_AFTER_REHASH + offset] = bucket.val_ptrs[key_idx];

                    if (IMM_MARK_INVALID) {
                        // We DON'T add a persistency barrier here BY DESIGN.
                        // If we crash, we have to look at all unmarked tuples anyway.
                        // If it turns out that one flag wasn't updated, we will notice when we try to look it up during compaction.
                        // This saves us a lot of cost during runtime at only a really small cost during recovery
                        entryA->flags |= std::byte(0b1);
                    }
                    continue;
                }
            }
        }

        if constexpr (!std::is_integral_v<KeyType>) {
            PayloadLocator locator(bucket.val_ptrs[key_idx]);
            if (locator.get_epoch() != payload_logs[locator.get_log_id()].persistent_state->log_epochs[locator.get_chunk_id()]) {
                // Entry is no longer reachable as it has been garbage collected in the log -> skip it
                continue;
            }
        }

        keys[directory_idx * MAX_VALUES_PER_BUCKET_AFTER_REHASH + sizes[directory_idx]] = bucket.keys[key_idx];
        values[directory_idx * MAX_VALUES_PER_BUCKET_AFTER_REHASH + sizes[directory_idx]] = bucket.val_ptrs[key_idx];
        ++sizes[directory_idx];

        assert (sizes[directory_idx] <= MAX_VALUES_PER_BUCKET_AFTER_REHASH);
    }
}

template <class KeyType, class ValType, PartitionType pType>
int Hashtable<KeyType, ValType, pType>::try_bulk_insert(int level, uint64_t directory_entry_idx, int epoch,
                               const uint64_t *keys,
                               const uint64_t *values,
                               int size) {

    assert(size > 0);
    PMEMDirectoryEntry *directory_entry = get_directory_entry(level, directory_entry_idx);
    int elems_inserted = 0;
    int n = get_free_bucket_idx(directory_entry->size);

    if (n == -1) {
        // All buckets are already full
        return 0;
    }

    bool allocated_new_bucket = false;

    while (n < BUCKETS_PER_DIRECTORY_ENTRY && elems_inserted < size) {

        Bucket *bucket;
        if (level <= MAX_BUCKET_PREALLOC_LEVEL) {
            bucket = &get_prealloced_bucket(level, directory_entry_idx, n);
        } else if (directory_entry->bucket_pointers[n] == 0) {
            // Allocate new bucket
            uint64_t new_bucket_id = allocate_empty_bucket();
            directory_entry->bucket_pointers[n] = new_bucket_id;
            allocated_new_bucket = true;
            bucket = &get_bucket(new_bucket_id);
        } else {
            bucket = &get_bucket(directory_entry->bucket_pointers[n]);
        }

        int bucket_size = get_size_of_bucket(directory_entry->size, n);
        int elems_to_insert = std::min(KEYS_PER_BUCKET - bucket_size, size - elems_inserted);



        if (elems_to_insert > 0) {

            insert_into_filter(keys + elems_inserted, elems_to_insert, level, directory_entry_idx, n);

            for (int i = 0; i < elems_to_insert; ++i) {
                int offset = elems_inserted + i;


                _mm_stream_si64((long long*)(bucket->keys + bucket_size + i), *(keys + offset));
                _mm_stream_si64((long long*)(bucket->val_ptrs + bucket_size + i), *(values + offset));
            }
//            reinterpret_cast<uint16_t*>(directory_entry->tombstones + (n / 4))[(n & 0b11)] = tombstones;

            elems_inserted += elems_to_insert;
        }
        ++n;
    }

    if (level > MAX_DRAM_FILTER_LEVEL) {
        // 4 Fingerprints fit into the same cache line
        for (int i = 0; i < BUCKETS_PER_DIRECTORY_ENTRY; i += 4) {
            _mm_clflushopt(&static_cast<PMEMDirectoryEntryWithFP*>(directory_entry)->fingerprint.bucket_fingerprints[i]);
        }
    }
    if (allocated_new_bucket && level < MAX_BUCKET_PREALLOC_LEVEL) {
        // 8 Bucket pointers fit into the same cache line
        for (int i = 0; i < BUCKETS_PER_DIRECTORY_ENTRY; i += 8) {
            _mm_clflushopt(&directory_entry->bucket_pointers[i]);
        }
    }

    _mm_sfence();


    // Persistency barrier between size and epoch is not required as only the size indicates whether migration was valid.
    // If epoch isn't updated, at worst we insert a few duplicate values from the log during recovery.
    // This can only happen in the first PMEM-layer. Since this is migrated quite often, we will soon discover those
    // duplicates during migration.
    directory_entry->size.fetch_add(elems_inserted, std::memory_order::relaxed); // += elems_inserted;
    directory_entry->epoch.store(epoch, std::memory_order::relaxed); // = epoch;
    _mm_clflushopt(&directory_entry->size);
    _mm_sfence();

    return elems_inserted;
}


//Source: https://github.com/FastFilter/fastfilter_cpp/blob/master/src/bloom/simd-block.h
inline __m128i MakeMask(const uint32_t hash) noexcept {
    const __m128i ones = _mm_set1_epi32(1);
    // Odd contants for hashing:
    const __m128i rehash = _mm_setr_epi32(0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
                                          0xa2b7289dU);
    // Load hash into a YMM register, repeated eight times
    __m128i hash_data = _mm_set1_epi32(hash);
    // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by four different
    // odd constants, then keep the 5 most significant bits from each product.
    hash_data = _mm_mullo_epi32(rehash, hash_data);
    hash_data = _mm_srli_epi32(hash_data, 27);
    // Use these 5 bits to shift a single bit to a location in each 32-bit lane
    return _mm_sllv_epi32(ones, hash_data);
}

template <class KeyType, class ValType, PartitionType pType>
bool Hashtable<KeyType, ValType, pType>::lookup(KeyType key, uint8_t *data) {

    std::optional<LookupResult> result = lookup_internal(key);
    if (result && !result->deleted) {
        if constexpr (std::is_integral_v<KeyType>) {
            memcpy(data, &result->locator.pos, sizeof(ValType));
        } else {
            auto *entry = reinterpret_cast<PayloadLogEntry *>(payload_logs[result->locator.get_log_id()].chunks[result->locator.get_chunk_id()].entries + result->locator.get_offset());
            fastMemcpy(data, reinterpret_cast<uint8_t*>(entry+1) + entry->key_len, entry->val_len);
        }
        return true;
    }
    return false;
}

template <class KeyType, class ValType, PartitionType pType>
std::optional<class Hashtable<KeyType, ValType, pType>::LookupResult> Hashtable<KeyType, ValType, pType>::lookup_internal(KeyType key) {

    uint64_t subdivision_idx;
    uint64_t entry_idx = get_dram_directory_entry_idx(key, &subdivision_idx);

    uint64_t subdivision_start = subdivision_idx * BUCKETS_PER_SUBDIVISION;
    uint64_t subdivision_end = subdivision_idx * BUCKETS_PER_SUBDIVISION + BUCKETS_PER_SUBDIVISION - 1;

    DRAMDirectoryEntry &directory_entry = dram_table[entry_idx];

RETRY:
    int epoch = directory_entry.epoch;

    for (int idx = subdivision_start; idx <= subdivision_end; ++idx) {
        Bucket &bucket = dram_buckets[entry_idx * BUCKETS_PER_DIRECTORY_ENTRY + idx];
        auto result = lookup_in_DRAM_bucket(entry_idx, idx, key);

        if (result) {
            if (directory_entry.epoch == epoch) {
                return result;
            } else {
                goto RETRY;
            }
        }
    }

    // We didn't have a hit in DRAM, so let's look in the PMEM layers.
    int level = 0;

    while (level < *cur_pmem_levels) {
        auto result = lookup_in_level(level, key);
        if (result) {
            return result;
        }
        ++level;
    }
    return {};
}


template <class KeyType, class ValType, PartitionType pType>
std::optional<class Hashtable<KeyType, ValType, pType>::LookupResult> Hashtable<KeyType, ValType, pType>::lookup_in_level(
        int level, KeyType key) {
    uint64_t directory_entry_idx = get_pmem_directory_entry_idx(level, get_key_representation(key));
    const __m128i mask = MakeMask(hash_key(key) >> 32);

RETRY:
    BucketFingerprint *fingerprint;

    for (int i = BUCKETS_PER_DIRECTORY_ENTRY - 1; i >= 0; --i) { //TODO: Only lock in buckets with elements!
        if (level <= MAX_DRAM_FILTER_LEVEL) {
            fingerprint = &dram_fingerprints[level][directory_entry_idx].bucket_fingerprints[i];
        } else {
            PMEMDirectoryEntryWithFP *directory_entry = static_cast<PMEMDirectoryEntryWithFP *>(get_directory_entry(level, directory_entry_idx));
            fingerprint = &directory_entry->fingerprint.bucket_fingerprints[i];
        }

        if (_mm_testc_si128(*reinterpret_cast<__m128i *>(fingerprint), mask)) {
            PMEMDirectoryEntry *directory_entry = get_directory_entry(level, directory_entry_idx);
            Bucket *bucket;
            if (level <= MAX_BUCKET_PREALLOC_LEVEL) {
                bucket = &get_prealloced_bucket(level, directory_entry_idx, i);
            } else {
                bucket = &get_bucket(directory_entry->bucket_pointers[i]);
            }

            //We read the size before reading the keys inside the buckets.
            //This ensures, that we only read entries that are completely persisted at this point in time.
            int epoch = directory_entry->epoch.load(std::memory_order_relaxed);
            int size = directory_entry->size.load(std::memory_order_relaxed);

            auto result = lookup_in_bucket(*directory_entry, *bucket, i, key);
            if (result) {
                if (directory_entry->epoch == epoch && directory_entry->size == size) {
                    return result;
                } else {
                    goto RETRY;
                }
            }
        }
    }
    return {};
}

template <class KeyType, class ValType, PartitionType pType>
std::optional<class Hashtable<KeyType, ValType, pType>::LookupResult> Hashtable<KeyType, ValType, pType>::lookup_in_bucket(
        Hashtable<KeyType, ValType, pType>::PMEMDirectoryEntry &entry,
        Hashtable<KeyType, ValType, pType>::Bucket &bucket,
        uint64_t bucket_idx, KeyType key) {
    int size = get_size_of_bucket(entry.size.load(std::memory_order_relaxed), bucket_idx);

    __m512i key_vec;
    if constexpr (std::is_integral_v<KeyType>) {
        key_vec = _mm512_set1_epi64(key);
    } else {
        uint64_t key_hash = hash_key(key);
        key_vec = _mm512_set1_epi64(key_hash);
    }


    for (short offset = KEYS_PER_BUCKET - 8; offset >= 0; offset -= 8) {
        __m512i val_vec = _mm512_load_si512(bucket.keys + offset);
        auto result = _mm512_cmpeq_epi64_mask(val_vec, key_vec);

        while (result != 0) {
            unsigned char index =  7 - (__builtin_clz(result) - 24);
            if (index + offset < size) {
                if constexpr (std::is_integral_v<KeyType>) {
                    return LookupResult{is_deleted(bucket, index + offset), PayloadLocator(bucket.val_ptrs[index + offset]), &bucket, false, static_cast<short>(index + offset) };
                } else {
                    PayloadLocator locator(bucket.val_ptrs[index + offset]);
                    bool deleted = is_deleted(bucket, index + offset);

                    if (deleted || is_alive(locator, key)) {
                        return LookupResult{deleted, locator, &bucket, false, static_cast<short>(index + offset) };
                    }
                }
            }
            result -= (1 << index);
        }
    }
    return {};
}

template <class KeyType, class ValType, PartitionType pType>
bool Hashtable<KeyType, ValType, pType>::is_alive(PayloadLocator locator, KeyType key) {
    if constexpr (!std::is_integral_v<KeyType>) {
        // Find out whether the log has been compacted since we've been inserted.
        if (payload_logs[locator.get_log_id()].persistent_state->log_epochs[locator.get_chunk_id()] == locator.get_epoch()) {
            auto *entryA = reinterpret_cast<PayloadLogEntry *>(payload_logs[locator.get_log_id()].chunks[locator.get_chunk_id()].entries + locator.get_offset());
            if (entryA->key_len == key.size() && memcmp(entryA+1, key.data(), entryA->key_len) == 0) {
                return true;
            }
        }
    }
    return false;
}



template <class KeyType, class ValType, PartitionType pType>
std::optional<class Hashtable<KeyType, ValType, pType>::LookupResult> Hashtable<KeyType, ValType, pType>::lookup_in_DRAM_bucket(
        uint64_t entry_idx, uint64_t bucket_idx, KeyType key) {
    DRAMDirectoryEntry &entry = dram_table[entry_idx];
    Bucket &bucket = dram_buckets[entry_idx * BUCKETS_PER_DIRECTORY_ENTRY + bucket_idx];
    uint8_t size = entry.sizes[bucket_idx];

    for (short i = size - 1; i >= 0; --i) {
        if constexpr (std::is_integral_v<KeyType>) {
            if (bucket.keys[i] == key) {
                return LookupResult{is_deleted(bucket, i), PayloadLocator(bucket.val_ptrs[i]), &bucket, true, i };
            }
        } else {
            PayloadLocator locator(bucket.val_ptrs[i]);
            uint64_t key_hash = hash_key(key);
            if (bucket.keys[i] == key_hash) {
                bool deleted = is_deleted(bucket, i);
                if (deleted || is_alive(locator, key)) {
                    return LookupResult{deleted, PayloadLocator(bucket.val_ptrs[i]), &bucket, true, i };
                }
            }
        }
    }
    return {};
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::update_keyset(Bucket &bucket, int bucket_size, int num_items, KeyType lower_bound, std::map<KeyType, ValType> &results) {

    if constexpr (std::is_integral_v<KeyType>) {

        int result_size = results.size();
        uint64_t upper_bound = UINT64_MAX;

        if (result_size > 0) {
            upper_bound = prev(results.end())->first;
        }

        for (short i = bucket_size - 1; i >= 0; --i) {
        if (bucket.keys[i] >= lower_bound &&
            (result_size < num_items || bucket.keys[i] < upper_bound)) {

            const auto [it, success] = results.insert(std::make_pair<uint64_t, uint64_t>(bucket.keys[i], bucket.val_ptrs[i]));

            if (success) {
                ++result_size;
            }

            if (result_size > num_items) {
                // Delete the largest element
                results.erase(prev(results.end()));
                upper_bound = prev(results.end())->first;
                --result_size;
            }
        }
    }
    } else {
        assert(false);
    }

}


template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::scan_dram_directory_entry(
        uint64_t entry_idx, int num_items, KeyType lower_bound, std::map<KeyType, ValType> &results) {


    DRAMDirectoryEntry &entry = dram_table[entry_idx];
    for (int bucket_idx = BUCKETS_PER_DIRECTORY_ENTRY; bucket_idx > 0; --bucket_idx) {
        Bucket &bucket = dram_buckets[entry_idx * BUCKETS_PER_DIRECTORY_ENTRY + bucket_idx];
        uint8_t size = entry.sizes[bucket_idx];

        update_keyset(bucket, size, num_items, lower_bound, results);
    }

    //std::cout << results.size() << std::endl;

    if (*cur_pmem_levels > 0) {
        uint64_t pmem_entry_idx = get_pmem_directory_entry_idx(0, get_key_representation(lower_bound));
        scan_pmem_directory_entry(pmem_entry_idx, 0, num_items, lower_bound, results);
    }

    // If we didn't find enough elements in this partition, repeat for the next one, tail recursion style
    ++entry_idx;
    if (results.size() < num_items) {
        //std::cout << "Going to next entry on DRAM: " << entry_idx << std::endl;
        uint64_t step = std::max(1ul,(MAX-MIN) / DRAM_DIRECTORY_SIZE);
        uint64_t bucket_min_value = entry_idx * step;

        if constexpr (std::is_integral_v<KeyType>) {
            scan_dram_directory_entry(entry_idx, num_items, bucket_min_value, results);
        }
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::scan_pmem_directory_entry(
        uint64_t entry_idx, int level, int num_items, KeyType lower_bound, std::map<KeyType, ValType> &results) {


    PMEMDirectoryEntry *directory_entry = get_directory_entry(level, entry_idx);
    for (int bucket_idx = BUCKETS_PER_DIRECTORY_ENTRY - 1; bucket_idx >= 0; --bucket_idx) {
        int size = get_size_of_bucket(directory_entry->size.load(std::memory_order_relaxed), bucket_idx);
        if (size == 0) {
            continue;
        }

        Bucket *bucket;
        if (level <= MAX_BUCKET_PREALLOC_LEVEL) {
            bucket = &get_prealloced_bucket(level, entry_idx, bucket_idx);
        } else {
            bucket = &get_bucket(directory_entry->bucket_pointers[bucket_idx]);
        }
        update_keyset(*bucket, size, num_items, lower_bound, results);
    }

    //std::cout << "Cur level:" << level << std::endl;

    //std::cout << "Cur size:" << results.size() << std::endl;


    // Depth first: go until last level
    if (*cur_pmem_levels > level + 1) {
        //std::cout << "Descending recursively" << std::endl;
        uint64_t new_pmem_entry_idx = get_pmem_directory_entry_idx(level + 1, get_key_representation(lower_bound));
        scan_pmem_directory_entry(new_pmem_entry_idx, level + 1, num_items, lower_bound, results);
    }


    uint64_t step = std::max(1ul,(MAX-MIN) / PMEM_DIRECTORY_SIZES[level]);
    ++entry_idx;
    uint64_t bucket_min_value = entry_idx * step;

    //std::cout << "Test2, :" << entry_idx << std::endl;

    if constexpr (std::is_integral_v<KeyType>) {

        // If there might be lower values in adjacent buckets, we have to find them
        if (entry_idx % (1 << FANOUT_BITS) != 0 && (results.size() == 0 || bucket_min_value < prev(results.end())->first)) {
            //if (results.size() > 0) {
                //std::cout << bucket_min_value << " < " << prev(results.end())->first << std::endl;
            //}
            //std::cout << "Going to next pmem entry on level: " << level << ", entry: " << entry_idx << std::endl;
            scan_pmem_directory_entry(entry_idx, level, num_items, lower_bound, results);
        }
    } else {
        assert(false);
    }
}

template <class KeyType, class ValType, PartitionType pType>
bool Hashtable<KeyType, ValType, pType>::is_deleted(const Bucket& bucket, uint8_t pos) const {

    if constexpr (std::is_integral_v<KeyType>) {
        return bucket.val_ptrs[pos] == TOMBSTONE_MARKER;
    } else {
        auto locator = PayloadLocator(bucket.val_ptrs[pos]);
        auto *entry = reinterpret_cast<PayloadLogEntry *>(payload_logs[locator.get_log_id()].chunks[locator.get_chunk_id()].entries + locator.get_offset());
        return entry->key_len == 8 && *reinterpret_cast<uint64_t*>(reinterpret_cast<uint8_t*>(entry+1) + entry->key_len) == TOMBSTONE_MARKER;
    }

}

template <class KeyType, class ValType, PartitionType pType>
long Hashtable<KeyType, ValType, pType>::count() {
    long total_size = 0;
    long dram_size = 0;
    long dram_max_size = DRAM_DIRECTORY_SIZE * BUCKETS_PER_DIRECTORY_ENTRY * KEYS_PER_BUCKET;

    long total_max_size = 0;


    for (int i = 0; i < DRAM_DIRECTORY_SIZE; ++i) {
        for (int j = 0; j < BUCKETS_PER_DIRECTORY_ENTRY; ++j)
            dram_size += dram_table[i].sizes[j];
    }
    std::cout << "SIZES:" << std::endl;
    std::cout << "\tDRAM:  " << dram_size << " (" << ((dram_size * 1.0) / dram_max_size) * 100 << "%)" << std::endl;
    total_size += dram_size;

    for (int layer = 0; layer < *cur_pmem_levels; ++layer) {
        long layer_size = 0;
        long layer_max_size = PMEM_DIRECTORY_SIZES[layer] * BUCKETS_PER_DIRECTORY_ENTRY * KEYS_PER_BUCKET;
        for (int i = 0; i < PMEM_DIRECTORY_SIZES[layer]; ++i) {
            PMEMDirectoryEntry* entry = get_directory_entry(layer, i);
            assert(entry->size <= 256);
            layer_size += entry->size;
        }

        std::cout << "\tPMEM" << layer << ": " << layer_size << " (" << ((layer_size * 1.0) / layer_max_size) * 100 << "%)" << std::endl;
        total_size += layer_size;
        total_max_size += layer_max_size;
    }
    std::cout << "\tTotal: " << total_size << " (" << ((total_size * 1.0) / total_max_size) * 100 << "%)" << std::endl;

    return total_size;
}

template <class KeyType, class ValType, PartitionType pType>
Hashtable<KeyType, ValType, pType>::~Hashtable() {
    munmap(directories[0], max_directory_entries_size);
    munmap(buckets, max_num_buckets * sizeof(Bucket));

    close(directories_fd);
    close(buckets_fd);
    close(metadata_fd);


    for (int idx = 0; idx < LOG_NUM; ++idx) {
        munmap(logs[idx].chunks[0].entries, LOG_MEMORY_SIZE);
        int fd = log_fds[idx];
        close(fd);
    }

    if constexpr (!std::is_integral_v<KeyType>) {
        for (int idx = 0; idx < PAYLOAD_LOG_NUM; ++idx) {
            munmap(payload_logs[idx].chunks[0].entries, PAYLOAD_LOG_MEMORY_SIZE);
            int fd = payload_log_fds[idx];
            close(fd);
        }
    }
}

template <class KeyType, class ValType, PartitionType pType>
Hashtable<KeyType, ValType, pType>::Hashtable(const std::string &pmem_dir, bool reset) {


    std::string directories_file = pmem_dir + "/directories.dat";
    std::string buckets_file = pmem_dir + "/buckets.dat";

    std::string pmem_log_file_prefix = pmem_dir + "/log";
    std::string pmem_payload_log_file_prefix = pmem_dir + "/payload_log";

    std::string metadata_file = pmem_dir + "/metadata.dat";

    if (reset) {
        std::remove(directories_file.c_str());
        std::remove(buckets_file.c_str());
        std::remove(metadata_file.c_str());

        for (int i = 0; i < LOG_NUM; ++i) {
            std::string pmem_log_file = pmem_log_file_prefix + std::to_string(i);
            std::remove(pmem_log_file.c_str());
        }

        for (int i = 0; i < PAYLOAD_LOG_NUM; ++i) {
            std::string pmem_payload_log_file = pmem_payload_log_file_prefix + std::to_string(i);
            std::remove(pmem_payload_log_file.c_str());
        }
    }


    for (int i = 0; i < MAX_PMEM_LEVELS; ++i) {
        long level_size = 1l << (PMEM_BITS + FANOUT_BITS * i);
        PMEM_DIRECTORY_SIZES[i] = level_size;
        if (i <= MAX_DRAM_FILTER_LEVEL) {
            max_directory_entries_size += level_size * sizeof(PMEMDirectoryEntry);
        } else {
            max_directory_entries_size += level_size * sizeof(PMEMDirectoryEntryWithFP);
        }
        max_num_buckets += level_size * BUCKETS_PER_DIRECTORY_ENTRY;
    }


    long num_dram_fingerprints = 0;

    for (int i = 0; i <= MAX_DRAM_FILTER_LEVEL; ++i) {
        num_dram_fingerprints += PMEM_DIRECTORY_SIZES[i];
    }

#if LOG_DEBUG
    std::cout << "Num fingerprints: " <<  num_dram_fingerprints << std::endl;
    std::cout << "Fingerprint size (MB): " <<  num_dram_fingerprints  * 256 / (1024 * 1024)<< std::endl;
#endif
    dram_fingerprint_data = std::make_unique<DirectoryFingerprint[]>(num_dram_fingerprints);
    memset(dram_fingerprint_data.get(), 0, num_dram_fingerprints * sizeof(DirectoryFingerprint));



    long pos = 0;
    for (int i = 0; i <= MAX_DRAM_FILTER_LEVEL; ++i) {
        dram_fingerprints[i] = dram_fingerprint_data.get() + pos;
        pos += PMEM_DIRECTORY_SIZES[i];
    }

    pos = 0;
    for (int i = 0; i <= MAX_BUCKET_PREALLOC_LEVEL; ++i) {
        BUCKET_OFFSETS[i] = pos;
        pos += PMEM_DIRECTORY_SIZES[i] * BUCKETS_PER_DIRECTORY_ENTRY;
    }
    next_empty_bucket_idx = pos;

    directories_fd = mmap_pmem_file(directories_file, max_directory_entries_size, &directories[0]);
    metadata_fd = mmap_pmem_file(metadata_file, sizeof(int), reinterpret_cast<char **>(&cur_pmem_levels));
    buckets_fd = mmap_pmem_file(buckets_file, max_num_buckets * sizeof(Bucket), reinterpret_cast<char **>(&buckets));

    //memset(directories_data, 0, max_directory_entries_size);

    logs = std::make_unique<Log[]>(LOG_NUM);
    payload_logs = std::make_unique<PayloadLog[]>(PAYLOAD_LOG_NUM);

    if (reset) {
        *cur_pmem_levels = 1;
    }

    for (int i = 0; i < LOG_NUM; ++i) {
        std::string pmem_log_file = pmem_log_file_prefix + std::to_string(i);

        char* log_data;

        log_fds.push_back(mmap_pmem_file(pmem_log_file, LOG_MEMORY_SIZE, &log_data));

        logs[i].persistent_state = reinterpret_cast<PersistentLogState *>(log_data);

        for (int idx = 0; idx < CHUNKS_PER_LOG; ++idx) {
            logs[i].chunks[idx].entries = reinterpret_cast<LogEntry *>(log_data + sizeof(PersistentLogState) + idx * CHUNK_SIZE);
        }

        if (reset) {
            memset(log_data, 0, LOG_MEMORY_SIZE);
            // Initialize the persistent state
            // Keep chunks 0, 1 and 2 free for compaction
            logs[i].persistent_state->write_chunk = 3;
            logs[i].persistent_state->first_chunk = 3;
            logs[i].persistent_state->first_free_chunk = 0;
            logs[i].persistent_state->compact_target = -1;

            logs[i].persistent_state->next_of[0] = 1;
            logs[i].persistent_state->next_of[1] = 2;
            logs[i].persistent_state->next_of[2] = -1;


            // Initialize RAWL -> since we zeroed the memory, all log entries start as invalid
            for (int idx = 0; idx < CHUNKS_PER_LOG; ++idx) {
                logs[i].persistent_state->valid_bits[idx] = 1;
            }

            for (int idx = 3; idx < CHUNKS_PER_LOG - 1; ++idx) {
                logs[i].persistent_state->next_of[idx] = idx+1;
            }
            logs[i].persistent_state->next_of[CHUNKS_PER_LOG - 1] = -1;
        }
    }

    if constexpr (!std::is_integral_v<KeyType>) {
        for (int i = 0; i < PAYLOAD_LOG_NUM; ++i) {
            std::string pmem_payload_log_file = pmem_payload_log_file_prefix + std::to_string(i);

            char* payload_log_data;


            payload_log_fds.push_back(mmap_pmem_file(pmem_payload_log_file, PAYLOAD_LOG_MEMORY_SIZE + sizeof(PersistentPayloadLogState), &payload_log_data));

            payload_logs[i].persistent_state = reinterpret_cast<PersistentPayloadLogState *>(payload_log_data);

            for (int idx = 0; idx < CHUNKS_PER_PAYLOAD_LOG; ++idx) {
                payload_logs[i].chunks[idx].entries = reinterpret_cast<uint8_t *>(payload_log_data + sizeof(PersistentPayloadLogState) + idx * PAYLOAD_CHUNK_SIZE);
            }

            if (reset) {

                payload_logs[i].persistent_state->compact_chunk = 0;
                payload_logs[i].persistent_state->write_chunk = 1;

                payload_logs[i].persistent_state->free[0] = false;
                payload_logs[i].persistent_state->free[1] = false;
                payload_logs[i].persistent_state->log_epochs[0] = 1;
                payload_logs[i].persistent_state->log_epochs[1] = 1;

                payload_logs[i].chunks[0].size = 0;
                payload_logs[i].chunks[0].reserved = 0;
                payload_logs[i].chunks[1].size = 0;
                payload_logs[i].chunks[1].reserved = 0;

                payload_logs[i].free_chunk_count = CHUNKS_PER_PAYLOAD_LOG - 2; // TODO: Also calculate that in recovery

                for (int idx = 2; idx < CHUNKS_PER_PAYLOAD_LOG; ++idx) {
                    payload_logs[i].persistent_state->free[idx] = true;
                    payload_logs[i].persistent_state->log_epochs[idx] = 1;
                    payload_logs[i].chunks[idx].size = 0;
                    payload_logs[i].chunks[idx].reserved = 0;

                }
            }
        }

        if (reset) {
            std::vector<std::thread> workers;

            for (uint64_t i = 0; i < PAYLOAD_LOG_NUM ; ++i) {
                workers.push_back(std::thread([&,i]() {
                    //memset(payload_log_data[i], 0, PAYLOAD_LOG_MEMORY_SIZE + sizeof(PersistentPayloadLogState));
                    for (size_t idx = 0; idx < PAYLOAD_LOG_MEMORY_SIZE + sizeof(PersistentPayloadLogState); idx += 4096) {
                        payload_logs[i].chunks[0].entries[idx] = 0;
                   }

                }));
            }
            std::for_each(workers.begin(), workers.end(), [](std::thread &t)
                          {
                              t.join();
                          });
        }
    }



    for (int i = 1; i < MAX_PMEM_LEVELS; ++i) {
        if (i-1 <= MAX_DRAM_FILTER_LEVEL) {
            directories[i]  = directories[i-1] + PMEM_DIRECTORY_SIZES[i-1] * sizeof(PMEMDirectoryEntry);
        } else {
            directories[i]  = directories[i-1] + PMEM_DIRECTORY_SIZES[i-1] * sizeof(PMEMDirectoryEntryWithFP);
        }
    }

    for (int i = 0; i < DRAM_DIRECTORY_SIZE; ++i) {
        dram_table[i].epoch = 1;
    }

    if (!reset) {
        recover_from_log();
    }
}

template <class KeyType, class ValType, PartitionType pType>
int Hashtable<KeyType, ValType, pType>::mmap_pmem_file(const std::string &filename, size_t max_size, char** target) {
    int fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        throw std::runtime_error("Could not open file at storage location: " + filename);
    }


    if ((errno = ftruncate(fd, max_size)) != 0) {
        throw std::runtime_error(
                "Could not allocate " + std::to_string(max_size) + " bytes at storage location: " + filename);
    }

    *target = static_cast<char *>(mmap(nullptr, max_size, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE | MAP_POPULATE, fd, 0));

    madvise(target, max_size, MADV_SEQUENTIAL);
    return fd;
}

template <class KeyType, class ValType, PartitionType pType>
class Hashtable<KeyType, ValType, pType>::Bucket &Hashtable<KeyType, ValType, pType>::get_bucket(uint64_t bucket_idx) {
    return buckets[bucket_idx];
}


template <class KeyType, class ValType, PartitionType pType>
class Hashtable<KeyType, ValType, pType>::Bucket &Hashtable<KeyType, ValType, pType>::get_prealloced_bucket(uint64_t level, uint64_t directory_entry_idx, uint64_t bucket_idx) {
    assert (level <= MAX_BUCKET_PREALLOC_LEVEL);
    return *(buckets + BUCKET_OFFSETS[level] + directory_entry_idx * BUCKETS_PER_DIRECTORY_ENTRY + bucket_idx);
}


template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::allocate_empty_bucket() {
    uint64_t bucket_idx = ++next_empty_bucket_idx;
    new (&buckets[bucket_idx]) Bucket();
    return bucket_idx;
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::insert_into_filter(const uint64_t *keys, int num, int level, uint64_t directory_entry_idx, int bucket_idx) {

    BucketFingerprint *fingerprint;


    if (level <= MAX_DRAM_FILTER_LEVEL) {
        fingerprint = &dram_fingerprints[level][directory_entry_idx].bucket_fingerprints[bucket_idx];
    } else {
        PMEMDirectoryEntryWithFP *directory_entry = static_cast<PMEMDirectoryEntryWithFP *>(get_directory_entry(level, directory_entry_idx));
        fingerprint = &directory_entry->fingerprint.bucket_fingerprints[bucket_idx];
    }



    // Does not need to be threadsafe, as only one thread writes to this filter at a time
    // We however need to use atomics, as the value could be read from another thread looking up a value
    // while we do an insert
    __m128i acc = _mm_set_epi64x(fingerprint->fp_part[1].load(std::memory_order_relaxed),
                                 fingerprint->fp_part[0].load(std::memory_order_relaxed));

    for (int i = 0; i < num; ++i) {
        uint64_t hash = get_key_or_hash(*(keys + i));

        const __m128i mask = MakeMask(hash >> 32);
        acc = _mm_or_epi64(acc, mask);
    }

    fingerprint->fp_part[0].store(_mm_extract_epi64(acc, 0), std::memory_order_relaxed);
    fingerprint->fp_part[1].store(_mm_extract_epi64(acc, 1), std::memory_order_relaxed);
    //_mm_storeu_si128(reinterpret_cast<__m128i_u *>(filter), acc);
}

template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::get_pmem_directory_entry_idx(int level, uint64_t key) {

    uint64_t entry_idx;
    if constexpr (pType == PartitionType::Hash) {
        // We hash-partition: Calculate the hash and clamp to table size
        uint64_t key_hash = get_key_or_hash(key);
        entry_idx = key_hash & (PMEM_DIRECTORY_SIZES[level] - 1);
    } else {
        //pType == Range
        // We range-partition: Find the right bin
        entry_idx = range_partition_key(key, level);
    }

    return entry_idx;
}


template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::get_dram_directory_entry_idx(const KeyType key, uint64_t* subdivision_idx) {

    uint64_t entry_idx;
    if constexpr (pType == PartitionType::Hash) {
        // We hash-partition: Calculate the hash and clamp to table size

        uint64_t key_hash = hash_key(key);
        entry_idx = key_hash & (DRAM_DIRECTORY_SIZE - 1);
        *subdivision_idx = (key_hash & ((NUM_SUBDIVISIONS - 1) << DRAM_BITS)) >> DRAM_BITS;
    } else {
        //pType == Range
        // We range-partition: Find the right bin
        static uint64_t step = std::max(1ul,(MAX-MIN) / DRAM_DIRECTORY_SIZE);

        entry_idx = key / step;

        *subdivision_idx = 0 ;
    }

    return entry_idx;
}

template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::get_payloadlog_entry_idx(const std::span<const std::byte> key) {

    if constexpr (pType == PartitionType::Hash) {
        // We hash-partition: Calculate the hash and clamp to table size
        return hash_key(key) & (PAYLOAD_LOG_NUM - 1);
    } else {
        assert(false);
    }
}

template <class KeyType, class ValType, PartitionType pType>
uint64_t Hashtable<KeyType, ValType, pType>::get_log_entry_idx(KeyType key) {

    if constexpr (pType == PartitionType::Hash) {
        // We hash-partition: Calculate the hash and clamp to table size
        return hash_key(key) & (LOG_NUM - 1);
    } else {
        assert(std::is_integral_v<KeyType>); //TODO: We currently don't support range partitioning for variable sized keys
        //pType == Range
        // We range-partition: Find the right bin
        uint64_t step = (MAX-MIN) / LOG_NUM + 1;
        return key / step;
    }
}


template <class KeyType, class ValType, PartitionType pType>
int Hashtable<KeyType, ValType, pType>::get_size_of_last_bucket(int size) {
    return size & (KEYS_PER_BUCKET - 1);
}

template <class KeyType, class ValType, PartitionType pType>
int Hashtable<KeyType, ValType, pType>::get_free_bucket_idx(int size) {
    int free_bucket_idx = size >> KEYS_PER_BUCKET_BITS;
    return free_bucket_idx < BUCKETS_PER_DIRECTORY_ENTRY? free_bucket_idx : -1;
}

template <class KeyType, class ValType, PartitionType pType>
int Hashtable<KeyType, ValType, pType>::get_size_of_bucket(int size, int bucket_idx) {
    int first_free_bucket_idx = get_free_bucket_idx(size);

    if (first_free_bucket_idx == -1 || bucket_idx < first_free_bucket_idx) {
        return KEYS_PER_BUCKET;
    } else if (bucket_idx == first_free_bucket_idx) {
        return get_size_of_last_bucket(size);
    } else {
        return 0;
    }
}

template <class KeyType, class ValType, PartitionType pType>
class Hashtable<KeyType, ValType, pType>::PMEMDirectoryEntry* Hashtable<KeyType, ValType, pType>::get_directory_entry(
        int level, uint64_t directory_entry_idx) {
    if (level <= MAX_DRAM_FILTER_LEVEL) {
        return reinterpret_cast<PMEMDirectoryEntry*>(directories[level]) + directory_entry_idx;
    } else {
        return reinterpret_cast<PMEMDirectoryEntryWithFP*>(directories[level]) + directory_entry_idx;
    }
}

template <class KeyType, class ValType, PartitionType pType>
void Hashtable<KeyType, ValType, pType>::remove(KeyType key) {


    if constexpr (std::is_integral_v<KeyType>) {
        insert(key, 0, true);
    } else {
        insert(key, std::span<const std::byte>{}, true);
    }


}
template<class KeyType, class ValType, PartitionType pType>
int Hashtable<KeyType, ValType, pType>::scan(KeyType key, int num_items, std::map<KeyType, ValType> &results) {
    uint64_t subdivision_idx;
    uint64_t entry_idx = get_dram_directory_entry_idx(key, &subdivision_idx);


    scan_dram_directory_entry(entry_idx, num_items, key, results);

    return 0;
}

template class Hashtable<uint64_t, uint64_t, PartitionType::Hash>;
template class Hashtable<uint64_t, uint64_t, PartitionType::Range>;

template class Hashtable<std::span<const std::byte>, std::span<const std::byte>, PartitionType::Hash>;
