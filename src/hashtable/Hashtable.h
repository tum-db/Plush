//
// Created by Lukas Vogel on 19.01.2021.
//

#ifndef LSM_HASHTABLE_H
#define LSM_HASHTABLE_H


#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <immintrin.h>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <span>
#include <vector>
#include <map>

#define LOG_METRICS 1
#define LOG_DEBUG 0

enum PartitionType { Hash, Range };

template <class KeyType, class ValType, PartitionType pType>
class Hashtable {


private:

    friend class PibenchWrapper;
    friend class PibenchWrapperVar;

    // Only needed for range partitioning mode - ignore if you don't exactly know why you need to change it
    static constexpr uint64_t MIN = 0;
    //static constexpr uint64_t MAX = UINT64_MAX;
    static constexpr uint64_t MAX = 268435456l;


    static constexpr uint64_t TOMBSTONE_MARKER = 0xFEEDC0FFEE22AA77;

    // Constants
    static constexpr int DRAM_BITS = 16;
    static constexpr const int PMEM_BITS = 16;
    static constexpr const int FANOUT_BITS = 4;
    static constexpr const int DRAM_SUBDIVISION_BITS = 4;
    static constexpr const int LOG_NUM_BITS = 6;

    static constexpr int PAYLOAD_LOG_NUM_BITS = 5;
    static constexpr int PAYLOAD_CHUNK_NUM_BITS = 6;

    static constexpr int CHUNKS_PER_LOG = 6;

    static constexpr int CHUNK_SIZE = 5 * 1024 * 1024;
    static constexpr long PAYLOAD_CHUNK_SIZE = 50 * 1024 * 1024;

    static constexpr int MAX_DRAM_FILTER_LEVEL = 1;
    static constexpr int MAX_BUCKET_PREALLOC_LEVEL = 1;

    // If set to true, payload log entries are marked for garbage collection immediately, when
    // the hash table entry pointing to it is discovered as being invalid.
    static constexpr bool IMM_MARK_INVALID = true;

    static constexpr int KEYS_PER_BUCKET_BITS = 4;

    static constexpr int MAX_PMEM_LEVELS = 4;

    const int LOG_NUM = 1 << LOG_NUM_BITS;
    const int PAYLOAD_LOG_NUM = 1 << PAYLOAD_LOG_NUM_BITS;
    static constexpr long CHUNKS_PER_PAYLOAD_LOG = 1 << PAYLOAD_CHUNK_NUM_BITS;

    //Note: all pmem level sizes must be divisible by FILTER_RECOVERY_THREAD_NUM without remainder
    const int FILTER_RECOVERY_THREAD_NUM = 32;

    static constexpr int BUCKETS_PER_DIRECTORY_ENTRY = 16; // 256 Byte * 16 = 4 KiB
    static constexpr int KEYS_PER_BUCKET = 1 << KEYS_PER_BUCKET_BITS;
    const int DRAM_DIRECTORY_SIZE = 1 << DRAM_BITS; // 2^16 Buckets * 256 Byte = 16 MiB
    const int NUM_SUBDIVISIONS = 1 << DRAM_SUBDIVISION_BITS;
    const int BUCKETS_PER_SUBDIVISION = BUCKETS_PER_DIRECTORY_ENTRY >> DRAM_SUBDIVISION_BITS;

    static constexpr long PAYLOAD_LOG_MEMORY_SIZE = PAYLOAD_CHUNK_SIZE * CHUNKS_PER_PAYLOAD_LOG + 1024 * 1024;
    static constexpr long LOG_MEMORY_SIZE = CHUNK_SIZE * CHUNKS_PER_LOG + 1024 * 1024;

    static const int MAX_VALUES_PER_BUCKET_AFTER_REHASH = 256;

    std::atomic<int> *cur_pmem_levels;

    long PMEM_DIRECTORY_SIZES[MAX_PMEM_LEVELS];
    long BUCKET_OFFSETS[MAX_PMEM_LEVELS];

    int directories_fd;
    int buckets_fd;
    int metadata_fd;

    std::vector<int> log_fds;
    std::vector<int> payload_log_fds;

    // Internal structures


    struct alignas(256) Bucket {
       std::atomic<uint64_t> keys[KEYS_PER_BUCKET];
       std::atomic<uint64_t> val_ptrs[KEYS_PER_BUCKET];
    };

    struct alignas(8) PayloadLogEntry {
        uint64_t key_len;
        uint64_t val_len;
        std::byte flags; // Last bit: is this entry still valid?
    };


    struct alignas(32) LogEntry {
        // Layout:
        // content[0] = KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKB
        // content[1] = VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVB
        // content[2] = KV.............................EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEB
        std::atomic<uint64_t> content[3];

        void persist(uint64_t key, uint64_t value, int epoch,  bool valid_bit) {
            unsigned long bit = !!valid_bit;    // Booleanize to force 0 or 1

            content[0].store((key << 1), std::memory_order::relaxed);
            content[1].store((value << 1), std::memory_order::relaxed);
            content[2].store((epoch << 1), std::memory_order::relaxed);
            content[2].fetch_or(((key & (1UL << 63))), std::memory_order::relaxed);
            content[2].fetch_or(((value & (1UL << 63)) >> 1), std::memory_order::relaxed);


            // Set the bits
            content[0].fetch_xor((-bit ^ content[0]) & 1UL, std::memory_order::relaxed);
            content[1].fetch_xor((-bit ^ content[1]) & 1UL, std::memory_order::relaxed);
            content[2].fetch_xor((-bit ^ content[2]) & 1UL, std::memory_order::relaxed);

            _mm_clflushopt(&content[0]);
            _mm_sfence();
        }

        bool is_valid(bool expected_valid_bit) {
            unsigned long bit = !!expected_valid_bit;    // Booleanize to force 0 or 1
            return (content[0] & 0b1) == bit && (content[1] & 0b1) == bit && (content[2] & 0b1) == bit;
        }

        uint64_t get_key() {
            uint64_t key = (content[0] >> 1) | (content[2] & (1UL << 63));
            return key;
        }

        uint64_t get_value() {
            uint64_t value = (content[1] >> 1) | ((content[2] & (1UL << 62)) << 1);
            return value;

        }

        int get_epoch() {
            int epoch = (content[2] & ((1UL << 34)-1)) >> 1;
            return epoch;
        }
    };

    struct BucketFingerprint {
        std::atomic<uint64_t> fp_part[2];
    };

    struct alignas(64) DirectoryFingerprint {
        BucketFingerprint bucket_fingerprints[BUCKETS_PER_DIRECTORY_ENTRY];
    };

    struct alignas(256) PMEMDirectoryEntry {
        std::atomic<int> size;
        std::atomic<int> epoch;
        uint64_t bucket_pointers[BUCKETS_PER_DIRECTORY_ENTRY];
    };

    struct PMEMDirectoryEntryWithFP : public PMEMDirectoryEntry {
        DirectoryFingerprint fingerprint;
    };

    struct DRAMDirectoryEntry {
        std::atomic<uint8_t> sizes[BUCKETS_PER_DIRECTORY_ENTRY];
        std::atomic<int> epoch;
        std::mutex m;
    };

    size_t max_directory_entries_size = 0;
    size_t max_num_buckets = 0;

    std::unique_ptr<DRAMDirectoryEntry[]> dram_table = std::make_unique<DRAMDirectoryEntry[]>(DRAM_DIRECTORY_SIZE);
    std::unique_ptr<Bucket[]> dram_buckets = std::make_unique<Bucket[]>(DRAM_DIRECTORY_SIZE * BUCKETS_PER_DIRECTORY_ENTRY);
    std::unique_ptr<DirectoryFingerprint[]> dram_fingerprint_data;

    Bucket* buckets;

    char* directories[MAX_PMEM_LEVELS];
    DirectoryFingerprint* dram_fingerprints[MAX_PMEM_LEVELS];

    struct alignas(256) PersistentLogState {
        std::atomic<int> write_chunk; // The chunk we currently write new values to
        std::atomic<int> first_chunk; // The head pointer, i.e. the first chunk in the chain with values
        std::atomic<int> first_free_chunk; // The first chunk in the free list
        std::atomic<int> compact_target;

        std::atomic<bool> valid_bits[CHUNKS_PER_LOG];
        std::atomic<int> next_of[CHUNKS_PER_LOG];
    };

    struct alignas(512) PersistentPayloadLogState {
        std::atomic<int> write_chunk; // The chunk we currently write new values to
        std::atomic<int> compact_chunk;

        std::atomic<int> log_epochs[CHUNKS_PER_PAYLOAD_LOG];
        std::atomic<bool> free[CHUNKS_PER_PAYLOAD_LOG];
    };


    struct alignas(64) LogChunk {
        std::atomic<size_t> size;
        std::atomic<size_t> reserved;
        std::atomic<int> max_epochs[1 << (DRAM_BITS - LOG_NUM_BITS)];
        LogEntry* entries;

    };

    struct alignas(64) PayloadLogChunk {
        std::atomic<size_t> size;
        std::atomic<size_t> reserved;
        uint8_t* entries;
    };

    struct alignas(256) Log {
        PersistentLogState *persistent_state;
        std::atomic_flag is_compacting;
        LogChunk chunks[CHUNKS_PER_LOG];
    };

    struct alignas(256) PayloadLog {
        std::queue<size_t> compact_order;
        std::atomic<size_t> free_chunk_count;
        PersistentPayloadLogState *persistent_state;
        std::mutex m;
        PayloadLogChunk chunks[CHUNKS_PER_PAYLOAD_LOG];
    };

    static constexpr long MAX_LOG_ENTRIES = CHUNK_SIZE / sizeof(LogEntry);

    struct PayloadLocator {

        static constexpr uint64_t LOG_NUM_MASK = ((1ul << PAYLOAD_LOG_NUM_BITS) - 1) << (63 - PAYLOAD_LOG_NUM_BITS);
        static constexpr uint64_t LOG_CHUNK_MASK = ((1ul << PAYLOAD_CHUNK_NUM_BITS) - 1) << (63 - PAYLOAD_LOG_NUM_BITS - PAYLOAD_CHUNK_NUM_BITS);
        static constexpr uint64_t LOG_EPOCH_MASK = ((1ul << (32 - PAYLOAD_LOG_NUM_BITS - PAYLOAD_CHUNK_NUM_BITS)) - 1) << 31;

        // Layout: LLLLLCCCCCCEEEEEEEEEEEEEEEEEEEEOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
        uint64_t pos;


        PayloadLocator() {
            pos = 0;
        }

        explicit PayloadLocator(uint64_t pos) {
            this->pos = pos;
        }

        PayloadLocator(uint64_t log_id, uint64_t chunk_id, uint64_t log_epoch, uint64_t offset) {
            pos = 0;
            pos |= (log_id << (63 - PAYLOAD_LOG_NUM_BITS));
            pos |= (chunk_id << (63 - PAYLOAD_LOG_NUM_BITS - PAYLOAD_CHUNK_NUM_BITS));
            pos |= (log_epoch << 32);
            pos |= offset;
        }

        [[nodiscard]] uint64_t get_log_id() const {
            uint64_t log_id = (pos & LOG_NUM_MASK) >> (63 - PAYLOAD_LOG_NUM_BITS);
            return log_id;
        }

        [[nodiscard]] uint64_t get_epoch() const {
            uint64_t epoch = (pos & LOG_EPOCH_MASK) >> 32;
            return epoch;
        }

        [[nodiscard]] uint64_t get_chunk_id() const {
            uint64_t chunk_id = (pos & LOG_CHUNK_MASK) >> (63 - PAYLOAD_LOG_NUM_BITS - PAYLOAD_CHUNK_NUM_BITS);
            return chunk_id;
        }

        [[nodiscard]] uint64_t get_offset() const {
            return pos & ~LOG_NUM_MASK & ~LOG_CHUNK_MASK & ~LOG_EPOCH_MASK;
        }
    };

    struct LookupResult {
        bool deleted;
        PayloadLocator locator;
        Bucket* storage_location;
        bool is_volatile; //True if stored in DRAM, false if stored on PMEM?
        short offset;
    };

    std::unique_ptr<Log[]> logs;
    std::unique_ptr<PayloadLog[]> payload_logs;

    std::atomic<uint64_t> next_empty_bucket_idx;

public:

    explicit Hashtable(const std::string& pmem_dir, bool reset);

    ~Hashtable();

    void insert(KeyType key, ValType value, bool tombstone = false, bool log = true);

    void remove(KeyType key);

    bool lookup(KeyType key, uint8_t *data);

    //TODO: Only supports fixed-size values for now
    int scan(KeyType lower_bound, int num_items, std::map<KeyType, ValType> &results);

    void checkpoint(int thread_count);

    long count();


private:

    /**
     * Splits the bucket and migrates it to the next level.
     * @param bucket A reference to the bucket to be split. The bucket will be empty afterwards
     * @param level The level of the bucket in the hierarchy - i.e. it will be migrated to 'level+1'
     */
    void migrate(uint64_t directory_entry_idx, int source_level, int target_level);

    void migrateDRAM(uint64_t entry_idx);

    int try_bulk_insert(int level, uint64_t directory_entry_idx, int epoch,
                        const uint64_t* keys,
                        const uint64_t* values,
                        int size);

    void reinsert(uint64_t key, uint64_t val);

    std::optional<LookupResult> lookup_internal(KeyType key);

    inline std::optional<LookupResult> lookup_in_level(int level, KeyType key);

    inline std::optional<LookupResult> lookup_in_bucket(PMEMDirectoryEntry& entry, Bucket &bucket, uint64_t bucket_idx, KeyType key);

    inline std::optional<LookupResult> lookup_in_DRAM_bucket(uint64_t entry_idx, uint64_t bucket_idx, KeyType key);

    void scan_dram_directory_entry(uint64_t entry_idx, int num_items, KeyType lower_bound, std::map<KeyType, ValType> &results);

    void scan_pmem_directory_entry(uint64_t entry_idx, int level, int num_items, KeyType lower_bound, std::map<KeyType, ValType> &results);

    void update_keyset(Bucket &bucket, int bucket_size, int num_items, KeyType lower_bound, std::map<KeyType, ValType> &results);

    inline uint64_t get_pmem_directory_entry_idx(int level, uint64_t key);

    inline uint64_t get_dram_directory_entry_idx(KeyType key, uint64_t* subdivision_idx);

    inline uint64_t get_payloadlog_entry_idx(std::span<const std::byte> key);

    inline uint64_t get_log_entry_idx(KeyType key);

    Bucket &get_bucket(uint64_t bucket_idx);

    PMEMDirectoryEntry* get_directory_entry(int level, uint64_t directory_entry_idx);

    Bucket &get_prealloced_bucket(uint64_t level, uint64_t directory_entry_idx, uint64_t bucket_idx);

    uint64_t allocate_empty_bucket();

    void insert_into_filter(const uint64_t* keys, int num, int level, uint64_t directory_entry_idx, int bucket_idx);

    int get_free_bucket_idx(int size);

    int get_size_of_last_bucket(int size);

    int get_size_of_bucket(int size, int bucket_idx);

    /**
     * Rehashes all values of the given bucket for an insertion in the given level.
     * The keys and values will be inserted into the keys and values array at the new offset relative to the current hash
     * of the old level.
     * A new offset begins every MAX_VALUES_PER_BUCKET_AFTER_REHASH items. The sizes array gives the actual sizes per new bucket.
     **/
    void rehash(Bucket &bucket, int size, int level, uint64_t *keys, uint64_t *values, int *sizes);

    void bulk_level_insert(int level, int epoch, const uint64_t *keys, const uint64_t *values, const int *sizes);

    void insert_into_DRAM_bucket(uint64_t entry_idx, int bucket_idx, int pos, uint64_t key,
                                 PayloadLocator value);

    void log_to_pmem(KeyType key, PayloadLocator value, int epoch);

    PayloadLocator log_payload(std::span<const std::byte> key, std::span<const std::byte> value);

    void compact_log(uint64_t log_idx);

    void compact_payload_log(uint64_t log_idx);

    void recover_from_log();

    void recover_single_log(uint64_t log_idx);

    void recover_fingerprints_and_allocator_status(uint64_t thread_idx, uint64_t* allocator_status_array);

    void checkpoint_runner(uint64_t start_idx, uint64_t end_idx);

    /**
     * Returns whether the supplied payload locator fits the given key, i.e.:
     * The payload entry its pointing to is still living and has the same key
     * @param locator
     * @return
     */
    bool is_alive(PayloadLocator locator, KeyType key);

    bool is_deleted(const Bucket& bucket, uint8_t pos) const;

    [[nodiscard]] static uint64_t hash_key(const uint64_t &key);

    [[nodiscard]] uint64_t range_partition_key(const uint64_t &key, uint64_t level) const;

    [[nodiscard]] static uint64_t hash_key(const std::span<const std::byte> &key);

    uint64_t get_key_representation(const KeyType &key);

    static uint64_t get_key_or_hash(uint64_t key);

    static bool move_log_entry(const LogChunk &source, LogChunk &target,  uint64_t read_pos, bool target_valid_bit);

    static void move_payload_log_entry(PayloadLogEntry* source, PayloadLogEntry* target);

    static int mmap_pmem_file(const std::string &filename, size_t max_size, char** target);
};


#endif //LSM_HASHTABLE_H