#!/bin/bash
PRELOAD=100000000
STEP=100000000
THREADS=48

#Note: You might want to adjust the maximum pool size for the data structures using PMDK

source ./env.sh
rm size.csv

printf "datastructure,elemcount,dram_size,pmem_size\n" >> size.csv

for i in $(seq 0 1 7);
do
  ./delete_state.sh
  ../build/pibench/src/PiBench ${LOCATIONS[i]} -S  --name ${NAMES[i]} -t $THREADS -k 8 -v 8 -n $PRELOAD -p $STEP -r 0 -i 1 -d 0
done