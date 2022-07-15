#!/bin/bash


# Builds plush, pibench (the benchmarking framework) and all other data structures - comment out the ones you don't need
rm -rf build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make utree
make dptree_pibench_wrapper
make hashtable # <- this is Plush
make hashtable_var # <- this is Plush for variable sized keys
make fast_fair
make pmemkv
make fptree_pibench_wrapper
make viperlib
make viperlibvarsize
make dash
make dash_var
make pibench-bin