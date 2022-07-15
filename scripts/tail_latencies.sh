#!/bin/bash


PRELOAD=100000000
SIZE=100000000
THREADS=23
rm latencies.csv

printf "percentile,operation,datastructure,latency_ns\n" >> latencies.csv

source ./env.sh

for i in $(seq 0 1 7);
do
  ./delete_state.sh

  # Insert tail latency
  ../build/pibench/src/PiBench  ${LOCATIONS[i]} -t $THREADS -k 8 -v 8 -n $PRELOAD -p $SIZE -r 0 -i 1 --latency_sampling=0.1 | tee out.txt


  cat out.txt | tail -8 | sed -e "s/: /,insert,${NAMES[i]},/g" | sed -e 's/\t//g' >> latencies.csv

  ./delete_state.sh

  # Read tail latency
  ../build/pibench/src/PiBench ${LOCATIONS[i]} -t $THREADS -k 8 -v 8 -n $PRELOAD -p $SIZE -r 1 -i 0 --latency_sampling=0.1 | tee out.txt

  cat out.txt | tail -8 | sed -e "s/: /,lookup,${NAMES[i]},/g" | sed -e 's/\t//g' >> latencies.csv

done
