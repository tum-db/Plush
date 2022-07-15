#!/bin/bash


PRELOAD=100000000
SIZE=100000000
rm skew.csv

printf "datastructure,skew,preload,size,throughput\n" >> skew.csv

source ./env.sh


SKEWS=(0.1 0.2 0.5 0.7 0.9 0.99 0.999)

for SKEW in "${SKEWS[@]}"
do
  for i in $(seq 0 1 7);
  do
    ./delete_state.sh

    # Read throughput
    ../build/pibench/src/PiBench ${LOCATIONS[i]} -t 48 -k 8 -v 8 -n $PRELOAD -p $SIZE -r 1 -i 0 --distribution ZIPFIAN --skew $SKEW | tee out.txt

    ((throughput=$(cat out.txt | grep "Read completed" | cut -d" " -f4 | cut -d "." -f 1)))

    printf "${NAMES[i]}," >> skew.csv
    printf "$SKEW," >> skew.csv
    printf "$PRELOAD," >> skew.csv
    printf "$SIZE," >> skew.csv
    printf "$throughput\n" >> skew.csv

  done
done