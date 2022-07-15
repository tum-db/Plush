#!/bin/bash


PRELOAD=40000000
SIZE=40000000
KEY_SIZE=16
VAL_SIZE=1000

rm throughput_variable_sized.csv

printf "datastructure,threadcount,preload,size,workload,key_size,val_size,throughput\n" >> throughput_variable_sized.csv

source ./env.sh

function r90w10 () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $KEY_SIZE -v $VAL_SIZE -n $PRELOAD -p $SIZE -r 0.9 -i 0.1 | tee out.txt
}

function r50w50 () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $KEY_SIZE -v $VAL_SIZE -n $PRELOAD -p $SIZE -r 0.5 -i 0.5 | tee out.txt
}

function r10w90 () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $KEY_SIZE -v $VAL_SIZE -n $PRELOAD -p $SIZE -r 0.1 -i 0.9 | tee out.txt
}

function r90w10zipf () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $KEY_SIZE -v $VAL_SIZE -n $PRELOAD -p $SIZE -r 0.9 -i 0.1 --distribution=ZIPFIAN | tee out.txt
}

function r50w50zipf () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $KEY_SIZE -v $VAL_SIZE -n $PRELOAD -p $SIZE -r 0.5 -i 0.5 --distribution=ZIPFIAN| tee out.txt
}

function r10w90zipf () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $KEY_SIZE -v $VAL_SIZE -n $PRELOAD -p $SIZE -r 0.1 -i 0.9 --distribution=ZIPFIAN| tee out.txt
}



WORKLOADS=(r90w10 r50w50 r10w90 r90w10zipf r50w50zipf r10w90zipf)

for THREADS in 48 36 24 16 8 4 2 1
do
  for i in $(seq 0 1 3);
  do
    for wl in $(seq 0 1 2);
    do
      ./delete_state.sh


      ${WORKLOADS[wl]}


      ((throughput=$(cat out.txt | grep -- "- Completed" | cut -d" " -f3 | cut -d "." -f 1)))

      printf "${NAMES_VAR[i]}," >> throughput_variable_sized.csv
      printf "$THREADS," >> throughput_variable_sized.csv
      printf "$PRELOAD," >> throughput_variable_sized.csv
      printf "$SIZE," >> throughput_variable_sized.csv
      printf "${WORKLOADS[wl]}," >> throughput_variable_sized.csv
      printf "$KEY_SIZE," >> throughput_variable_sized.csv
      printf "$VAL_SIZE," >> throughput_variable_sized.csv
      printf "$throughput\n" >> throughput_variable_sized.csv

    done
  done
done