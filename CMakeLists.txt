set(CMAKE_CXX_COMPILER "g++-11")

cmake_minimum_required(VERSION 3.16)
project(Plush)
include(ExternalProject)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native -pthread -gdwarf-4")

add_library(hashtable SHARED
        src/hashtable/Hashtable.h
        src/hashtable/Hashtable.cpp
        include/pibench/tree_api.h
        src/benchmarking/PibenchWrapper.cpp
        src/benchmarking/PibenchWrapper.h)

add_library(hashtable_var SHARED
        src/hashtable/Hashtable.h
        src/hashtable/Hashtable.cpp
        include/pibench/tree_api.h
        src/benchmarking/PibenchWrapperVar.cpp
        src/benchmarking/PibenchWrapperVar.h)


add_library(utree SHARED
        include/pibench/tree_api.h
        include/utree/PibenchWrapper.cpp
        include/utree/PibenchWrapper.h)
target_link_libraries(utree /usr/local/lib/libpmemobj.so /usr/local/lib/libpmem.so )


add_executable(gutenberg
        examples/gutenberg.cpp
        src/hashtable/Hashtable.h
        )


add_executable(demo
        src/hashtable/Hashtable.h
        examples/example.cpp
        )


target_link_libraries(gutenberg PRIVATE hashtable)
target_link_libraries(demo PRIVATE hashtable)


add_subdirectory(test)

add_subdirectory(dash)
add_subdirectory(viper)
add_subdirectory(pmemkv)
add_subdirectory(dptree)
add_subdirectory(fptree)
add_subdirectory(pibench)
#add_subdirectory(faster/cc)

#missing rocksdb

# FASTFAIR is make-based
ExternalProject_Add(fast_fair
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/fast_fair/concurrent_pmdk/
        BUILD_IN_SOURCE true
        CONFIGURE_COMMAND ""
        BUILD_COMMAND make fastfairlib
        INSTALL_COMMAND ""
        )
