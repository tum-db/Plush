#!/bin/bash


PRELOAD=100000000
SIZE=100000000
rm throughput.csv

printf "datastructure,threadcount,preload,size,operation,throughput\n" >> throughput.csv

source ./env.sh

for THREADS in $(seq 48 -1 1)
do
  for i in $(seq 0 1 7);
  do
    ./delete_state.sh


    # Write throughput
    ../build/pibench/src/PiBench ${LOCATIONS[i]} -t $THREADS -k 8 -v 8 -n $PRELOAD -p $SIZE -r 0 -i 1 | tee out.txt

    ((throughput=$(cat out.txt | grep "Insert completed" | cut -d" " -f4 | cut -d "." -f 1)))

    printf "${NAMES[i]}," >> throughput.csv
    printf "$THREADS," >> throughput.csv
    printf "$PRELOAD," >> throughput.csv
    printf "$SIZE," >> throughput.csv
    printf "insert," >> throughput.csv
    printf "$throughput\n" >> throughput.csv


    ./delete_state.sh


    # Read throughput
    ../build/pibench/src/PiBench ${LOCATIONS[i]} -t $THREADS -k 8 -v 8 -n $PRELOAD -p $SIZE -r 1 -i 0 | tee out.txt

    ((throughput=$(cat out.txt | grep "Read completed" | cut -d" " -f4 | cut -d "." -f 1)))

    printf "${NAMES[i]}," >> throughput.csv
    printf "$THREADS," >> throughput.csv
    printf "$PRELOAD," >> throughput.csv
    printf "$SIZE," >> throughput.csv
    printf "lookup," >> throughput.csv
    printf "$throughput\n" >> throughput.csv

    ./delete_state.sh


    # Delete throughput
    ../build/pibench/src/PiBench ${LOCATIONS[i]} -t $THREADS -k 8 -v 8 -n $PRELOAD -p $SIZE -r 0 -i 0 -d 1 | tee out.txt

    ((throughput=$(cat out.txt | grep "Remove completed" | cut -d" " -f4 | cut -d "." -f 1)))

    printf "${NAMES[i]}," >> throughput.csv
    printf "$THREADS," >> throughput.csv
    printf "$PRELOAD," >> throughput.csv
    printf "$SIZE," >> throughput.csv
    printf "delete," >> throughput.csv
    printf "$throughput\n" >> throughput.csv


    ./delete_state.sh


  done
done