#!/bin/bash


rm out.txt
rm /mnt/pmem0/vogel/fptree
rm /mnt/pmem0/vogel/pmem_hash.data
rm /mnt/pmem0/vogel/pmem_hash_var.data
rm -rf /mnt/pmem0/vogel/viper/
rm /mnt/pmem0/vogel/fastfair
rm /mnt/pmem0/vogel/pmemkv
rm -rf /mnt/pmem0/vogel/rocksdb/
rm /mnt/pmem0/vogel/utree
rm -rf /mnt/pmem0/vogel/tabletest
rm /mnt/pmem0/vogel/dptree.dat

mkdir /mnt/pmem0/vogel/tabletest
