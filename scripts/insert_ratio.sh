#!/bin/bash


PRELOAD=100000000
SIZE=100000000
rm insertratio.csv

printf "datastructure,preload,size,insertratio,throughput\n" >> insertratio.csv

source ./env.sh

for RATIO in $(seq 0.0 0.1 1.0)
do
  for i in $(seq 0 1 7);
  do
    ./delete_state.sh

    ../build/pibench/src/PiBench ${LOCATIONS[i]} -t 48 -k 8 -v 8 -n $PRELOAD -p $SIZE -r $(echo 1- $RATIO | bc) -i $RATIO | tee out.txt

    ((throughput=$(cat out.txt | grep -- "- Completed" | cut -d" " -f3 | cut -d "." -f 1)))

    printf "${NAMES[i]}," >> insertratio.csv
    printf "$PRELOAD," >> insertratio.csv
    printf "$SIZE," >> insertratio.csv
    printf "$RATIO," >> insertratio.csv
    printf "$throughput\n" >> insertratio.csv

  done
done