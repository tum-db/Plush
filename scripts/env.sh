#/bin/bash

export NAMES=("PLUSH" "DPTree" "utree" "Viper" "FASTFAIR" "DASH"  "FPTree" "pmemkv")
export LOCATIONS=(
           "../build/libhashtable.so" #<-- Plush
           "../build/dptree/libdptree_pibench_wrapper.so"
           "../build/libutree.so"
           "../build/viper/libviperlib.so"
           "../fast_fair/concurrent_pmdk/fastfairlib.so"
           "../build/dash/libdash.so"
           "../build/src/libfptree_pibench_wrapper.so"
           "../build/pmemkv/libpmemkv.so.1")

NAMES_VAR=("Dash" "Viper" "Plush" "PMEMKV") #TODO: FASTER
LOCATIONS_VAR=("../build/dash/libdash_var.so"
           "../build/viper/libviperlibvarsize.so"
           "../build/libhashtable_var.so" # <-- Plush
           "../build/pmemkv/libpmemkv.so.1"
           #FASTER <-- TODO
           )
