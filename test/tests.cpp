//
// Created by Lukas Vogel on 17.05.2021.
//
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include <condition_variable>
#include "doctest.h"
#include "../src/hashtable/Hashtable.h"
#include "Multithreader.h"


TEST_CASE("Inserting single element") {

    Hashtable<uint64_t, uint64_t, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);

    REQUIRE(table.count() == 0);
    table.insert(1337, 42);


    SUBCASE("inserting an element increases the element count") {
        CHECK(table.count() == 1);
    }

    SUBCASE("an inserted element can be retrieved") {
        uint64_t found_val;
        bool result = table.lookup(1337, reinterpret_cast<uint8_t *>(&found_val));
        REQUIRE(result);
        REQUIRE(found_val == 42);
    }
}


TEST_CASE_TEMPLATE("Inserting 1000 elements", T, uint64_t, std::span<const std::byte>) {

    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;

    REQUIRE(table.count() == 0);



    multithreader.insert(table, 1, 0, 1000);
    CHECK(table.count() == 1000);
    multithreader.lookup(table, 1, 0, 1000);

}


TEST_CASE_TEMPLATE("Inserting 1000 elements, then scanning", T, uint64_t) {

    Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Range> multithreader;

    REQUIRE(table.count() == 0);



    multithreader.insert(table, 1, 0, 1000);
    CHECK(table.count() == 1000);

    std::map<T, T> scan_result;

    uint64_t start = 42;
    table.scan(start, 100, scan_result);

    CHECK(scan_result.size() == 100);

    for (int i = 0; i < 100; ++i) {
        CHECK(scan_result[start + i] == start + i);
    }


    std::cout << scan_result.begin()->first << std::endl;
}

TEST_CASE_TEMPLATE("Inserting 200M elements, then scanning", T, uint64_t) {

    Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Range> multithreader;

    REQUIRE(table.count() == 0);



    multithreader.insert(table, 48, 0, 200e6);
    CHECK(table.count() == 200e6);

    std::map<T, T> scan_result;

    uint64_t start = 1734231;
    table.scan(start, 100, scan_result);

    CHECK(scan_result.size() == 100);

    for (int i = 0; i < 100; ++i) {
        CHECK(scan_result[start + i] == start + i);
    }


    std::cout << scan_result.begin()->first << std::endl;
}



TEST_CASE_TEMPLATE("Inserting 1M elements", T, uint64_t, std::span<const std::byte>) {

    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;

    REQUIRE(table.count() == 0);

    multithreader.insert(table, 1, 0, 25e6);
    CHECK(table.count() == 25e6);
    multithreader.lookup(table, 1, 0, 25e6);

}


TEST_CASE_TEMPLATE("Inserting 200M elements", T, uint64_t, std::span<const std::byte>) {

    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;

    multithreader.insert(table, 48, 0, 200e6);
    CHECK(table.count() == 200e6);
    multithreader.lookup(table, 48, 0, 200e6);
}

TEST_CASE_TEMPLATE("Inserting 200M elements in range partition mode", T, uint64_t) {

    Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Range> multithreader;

    multithreader.insert(table, 48, 0, 200e6);
    CHECK(table.count() == 200e6);
    multithreader.lookup(table, 48, 0, 200e6);
}

 TEST_CASE_TEMPLATE("Multithreaded: Inserting and reading 200M elements, 24 threads", T, uint64_t, std::span<const std::byte>) {

     Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
     Multithreader<T, T, PartitionType::Hash> multithreader;

    std::cout << "INSERT: ";
    multithreader.insert(table, 48, 0, 10e6);
    std::cout << "LOOKUP: ";
    multithreader.lookup(table, 48, 0, 10e6);

    CHECK(table.count() == 10e6);
    std::cout << "INSERT: ";
    multithreader.insert(table, 48, 10e6, 200e6);
    CHECK(table.count() == 200e6);

    std::cout << "LOOKUP: ";
    multithreader.lookup(table, 48, 0, 200e6);
}

TEST_CASE_TEMPLATE("Multithreaded: Mixed workload with 200M elements, 24 threads", T, uint64_t, std::span<const std::byte>) {

    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;

    multithreader.mixed(table, 48, 0, 200e6);
    CHECK(table.count() == 200e6);
    multithreader.lookup(table, 48, 0, 200e6);

}



TEST_CASE_TEMPLATE("Multithreaded: Mixed workload with 200M elements, 48 threads in range partition mode", T, uint64_t) {

    Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Range> multithreader;

    multithreader.mixed(table, 48, 0, 200e6);
    CHECK(table.count() == 200e6);
    multithreader.lookup(table, 48, 0, 200e6);

}
//TEST_CASE_TEMPLATE("Burn test: Mixed workload with 3 Billion elements, 1000 threads", T, uint64_t, std::span<const std::byte>) {
//
//    Hashtable<T, T> table("/mnt/pmem0/vogel/tabletest", true);
//    Multithreader<T, T> multithreader;
//
//    multithreader.mixed(table, 1000, 0, 3e9l);
//    CHECK(table.count() == 3e9l);
//    multithreader.lookup(table, 24, 0, 3e9l);
//}

TEST_CASE("After updating a key, its new value is returned") {
    Hashtable<uint64_t, uint64_t, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    uint64_t val;
    bool found;

    table.insert(42, 99);
    found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
    CHECK(found);
    CHECK(val == 99);

    table.insert(42, 100);
    found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
    CHECK(found);
    CHECK(val == 100);

    table.insert(42, 99);
    found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
    CHECK(found);
    CHECK(val == 99);
}

TEST_CASE("After updating a key, its new value is returned in range partition mode") {
    Hashtable<uint64_t, uint64_t, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
    uint64_t val;
    bool found;

    table.insert(42, 99);
    found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
    CHECK(found);
    CHECK(val == 99);

    table.insert(42, 100);
    found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
    CHECK(found);
    CHECK(val == 100);

    table.insert(42, 99);
    found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
    CHECK(found);
    CHECK(val == 99);
}

TEST_CASE_TEMPLATE("After updating a key 100M times, its newest value is returned", T, uint64_t, std::span<const std::byte>) {
    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    bool found;

    uint64_t key = 42;


    for (int i = 0; i <= 10e6; ++i) {
        if constexpr (std::is_integral_v<T>) {
            table.insert(key, i);
        } else {
            std::span<std::byte> key_span{reinterpret_cast<std::byte *>(&key), 8};
            std::span<std::byte> val{reinterpret_cast<std::byte *>(&i), 8};

            table.insert(key_span, val);
        }
    }

    uint64_t pointer_val;

    if constexpr (std::is_integral_v<T>) {
        found = table.lookup(key, (uint8_t*)&pointer_val);
    } else {
        std::span<std::byte> key_span{reinterpret_cast<std::byte*>(&key), 8};
        found = table.lookup(key_span, (uint8_t*)&pointer_val);
    }

    CHECK(pointer_val == 10e6);

    CHECK(found);
}



TEST_CASE_TEMPLATE("After updating a key 100M times, its newest value is returned in range partition mode", T, uint64_t) {
    Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
    bool found;

    uint64_t key = 42;


    for (int i = 0; i <= 10e6; ++i) {
        if constexpr (std::is_integral_v<T>) {
            table.insert(key, i);
        } else {
            std::span<std::byte> key_span{reinterpret_cast<std::byte *>(&key), 8};
            std::span<std::byte> val{reinterpret_cast<std::byte *>(&i), 8};

            table.insert(key_span, val);
        }
    }

    uint64_t pointer_val;

    if constexpr (std::is_integral_v<T>) {
        found = table.lookup(key, (uint8_t*)&pointer_val);
    } else {
        std::span<std::byte> key_span{reinterpret_cast<std::byte*>(&key), 8};
        found = table.lookup(key_span, (uint8_t*)&pointer_val);
    }

    CHECK(pointer_val == 10e6);

    CHECK(found);
}

//TEST_CASE_TEMPLATE("Concurrent updates: 48 threads updating 48 keys 100M times, its newest value is returned", T, uint64_t, std::span<const std::byte>) {
//    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
//    Multithreader<T, T, PartitionType::Hash> multithreader;

//    multithreader.update(table, 48, 0, 200e6);
//    table.count();
//}


TEST_CASE("A Value in DRAM survives recovery") {

    uint64_t val;
    bool found;

    {
        Hashtable<uint64_t, uint64_t, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
        table.insert(42, 99);
        found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
        CHECK(found);
        CHECK(val == 99);
    }

    {
        Hashtable<uint64_t, uint64_t, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", false);
        table.count();
        found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
        CHECK(found);
        CHECK(val == 99);

    }
}

TEST_CASE("A Value in DRAM survives recovery in range partition mode") {

    uint64_t val;
    bool found;

    {
        Hashtable<uint64_t, uint64_t, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
        table.insert(42, 99);
        found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
        CHECK(found);
        CHECK(val == 99);
    }

    {
        Hashtable<uint64_t, uint64_t, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", false);
        table.count();
        found = table.lookup(42, reinterpret_cast<uint8_t *>(&val));
        CHECK(found);
        CHECK(val == 99);

    }
}


TEST_CASE_TEMPLATE("100M values in DRAM survive recovery", T, uint64_t, std::span<const std::byte>) {



    {
        Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
        Multithreader<T, T, PartitionType::Hash> multithreader;

        multithreader.insert(table, 48, 0, 100e6);
        table.count();
    }


    {
        Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Hash> multithreader;

        table.count();

        multithreader.lookup(table, 48, 0, 100e6, false, false);
    }

}

TEST_CASE_TEMPLATE("100M values in DRAM survive recovery in range partition mode", T, uint64_t) {



    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
        Multithreader<T, T, PartitionType::Range> multithreader;

        multithreader.insert(table, 24, 0, 100e6);
        table.count();
    }


    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Range> multithreader;

        table.count();

        multithreader.lookup(table, 24, 0, 100e6, false, false);
    }

}


TEST_CASE_TEMPLATE("Recover 100M values twice", T, uint64_t, std::span<const std::byte>) {

    uint64_t val;
    bool found;

    {
        Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
        Multithreader<T, T, PartitionType::Hash> multithreader;
        multithreader.insert(table, 48, 0, 100e6);
        table.count();
    }

    {
        Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Hash> multithreader;
        multithreader.lookup(table, 48, 0, 100e6);
        table.count();
    }

    {
        Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Hash> multithreader;
        multithreader.lookup(table, 48, 0, 100e6);
        table.count();
    }
}

TEST_CASE_TEMPLATE("Recover 100M values twice in range partition mode", T, uint64_t) {

    uint64_t val;
    bool found;

    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
        Multithreader<T, T, PartitionType::Range> multithreader;
        multithreader.insert(table, 48, 0, 100e6);
        table.count();
    }

    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Range> multithreader;
        multithreader.lookup(table, 48, 0, 100e6);
        table.count();
    }

    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Range> multithreader;
        multithreader.lookup(table, 48, 0, 100e6);
        table.count();
    }
}

TEST_CASE_TEMPLATE("Insert ontop of recovered values works", T, uint64_t, std::span<const std::byte>) {

uint64_t val;
bool found;

{
    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;
    multithreader.insert(table, 48, 0, 100e6);
    std::cout << "COUNT AFTER FIRST INSERT" << std::endl;
    table.count();
    std::cout << std::endl;
}

{
    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", false);
    Multithreader<T, T, PartitionType::Hash> multithreader;
    std::cout << "COUNT AFTER FIRST RECOVERY" << std::endl;
    table.count();
    std::cout << std::endl;
    multithreader.insert(table, 48, 100e6, 200e6);
    std::cout << "COUNT AFTER SECOND INSERT" << std::endl;
    table.count();
    std::cout << std::endl;
}

{
    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", false);
    Multithreader<T, T, PartitionType::Hash> multithreader;
    std::cout << "COUNT AFTER SECOND RECOVERY" << std::endl;
    table.count();
    std::cout << std::endl;
    multithreader.lookup(table, 48, 0, 200e6);
}
}

TEST_CASE_TEMPLATE("Insert ontop of recovered values works in range partition mode", T, uint64_t) {

    uint64_t val;
    bool found;

    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);
        Multithreader<T, T, PartitionType::Range> multithreader;
        multithreader.insert(table, 48, 0, 100e6);
        std::cout << "COUNT AFTER FIRST INSERT" << std::endl;
        table.count();
        std::cout << std::endl;
    }

    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Range> multithreader;
        std::cout << "COUNT AFTER FIRST RECOVERY" << std::endl;
        table.count();
        std::cout << std::endl;
        multithreader.insert(table, 48, 100e6, 200e6);
        std::cout << "COUNT AFTER SECOND INSERT" << std::endl;
        table.count();
        std::cout << std::endl;
    }

    {
        Hashtable<T, T, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", false);
        Multithreader<T, T, PartitionType::Range> multithreader;
        std::cout << "COUNT AFTER SECOND RECOVERY" << std::endl;
        table.count();
        std::cout << std::endl;
        multithreader.lookup(table, 48, 0, 200e6);
    }
}


TEST_CASE("Deleting single element") {

    Hashtable<uint64_t, uint64_t, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);

    REQUIRE(table.count() == 0);
    table.insert(1337, 42);

    uint64_t found_val;
    bool result = table.lookup(1337, reinterpret_cast<uint8_t *>(&found_val));
    REQUIRE(result);
    REQUIRE(found_val == 42);


    table.remove(42);
    result = table.lookup(1337, reinterpret_cast<uint8_t *>(&found_val));
    REQUIRE(result);
    REQUIRE(found_val == 42);


    table.remove(1337);
    result = table.lookup(1337, reinterpret_cast<uint8_t *>(&found_val));
    REQUIRE(!result);
}

TEST_CASE("Deleting single element in range partition mode") {

    Hashtable<uint64_t, uint64_t, PartitionType::Range> table("/mnt/pmem0/vogel/tabletest", true);

    REQUIRE(table.count() == 0);
    table.insert(1337, 42);

    uint64_t found_val;
    bool result = table.lookup(1337, reinterpret_cast<uint8_t *>(&found_val));
    REQUIRE(result);
    REQUIRE(found_val == 42);


    table.remove(42);
    result = table.lookup(1337, reinterpret_cast<uint8_t *>(&found_val));
    REQUIRE(result);
    REQUIRE(found_val == 42);


    table.remove(1337);
    result = table.lookup(1337, reinterpret_cast<uint8_t *>(&found_val));
    REQUIRE(!result);
}

TEST_CASE_TEMPLATE("Deleting 1000 elements", T, uint64_t, std::span<const std::byte>) {
    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;



    multithreader.insert(table, 1, 0, 1000);
    CHECK(table.count() == 1000);
    multithreader.remove(table, 1, 0, 1000);

    multithreader.lookup(table, 1, 0, 1000, true);

}


TEST_CASE_TEMPLATE("Deleting 1 million elements", T, uint64_t, std::span<const std::byte>) {

    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;

    multithreader.insert(table, 48, 0, 200e6);
    table.count();
    multithreader.remove(table, 48, 0, 200e6);
    table.count();

    multithreader.lookup(table, 48, 0, 200e6, true);
    table.count();
}

TEST_CASE_TEMPLATE("Deleting only some elements", T, uint64_t, std::span<const std::byte>) {

    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;

    multithreader.insert(table, 48, 0, 200e6);
    table.count();
    multithreader.remove(table, 48, 0, 100e6);
    table.count();

    multithreader.lookup(table, 48, 100e6, 200e6);
    multithreader.lookup(table, 48, 0, 100e6, true);

    table.count();
}

//TEST_CASE_TEMPLATE("Overwriting formerly deleted elements", T, uint64_t, std::span<const std::byte>) {
//
//    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
//    Multithreader<T, T, PartitionType::Hash> multithreader;

//    multithreader.insert(table, 48, 0, 100e6);
//    table.count();
//    multithreader.remove(table, 48, 0, 100e6);
//    table.count();
//    multithreader.lookup(table, 48, 0, 100e6, true);
//    multithreader.insert(table, 48, 0, 100e6);
//    multithreader.lookup(table, 48, 0, 100e6);

//    table.count();
//}


TEST_CASE_TEMPLATE("Checkpointing", T, uint64_t, std::span<const std::byte>) {

    Hashtable<T, T, PartitionType::Hash> table("/mnt/pmem0/vogel/tabletest", true);
    Multithreader<T, T, PartitionType::Hash> multithreader;

    multithreader.insert(table, 48, 0, 100e6);
    table.checkpoint(1);
    table.count();
}