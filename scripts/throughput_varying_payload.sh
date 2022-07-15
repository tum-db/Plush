#!/bin/bash


THREADS=48


rm throughput_varying_payload.csv

printf "datastructure,threadcount,preload,size,workload,key_size,val_size,throughput\n" >> throughput_varying_payload.csv

source ./env.sh

function r90w10 () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $1 -v $2 -n $3 -p $4 -r 0.9 -i 0.1 | tee out.txt
}

function r50w50 () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $1 -v $2 -n $3 -p $4 -r 0.5 -i 0.5 | tee out.txt
}

function r10w90 () {
  ../build/pibench/src/PiBench ${LOCATIONS_VAR[i]} -t $THREADS -k $1 -v $2 -n $3 -p $4 -r 0.1 -i 0.9 | tee out.txt
}


WORKLOADS=(r90w10 r50w50 r10w90)
KEY_SIZES=(8   16 16   32  64  128)
VALUE_SIZES=(8 16 128 256 512 1000)
PREFILL_SIZES=(1280000000 320000000 160000000 80000000 40000000 20000000)
BENCHMARK_SIZES=(640000000 160000000 80000000 40000000 20000000 10000000)

for size_idx in $(seq 0 1 5)
do
  for i in $(seq 0 1 4);
  do
    for wl in $(seq 1 1 1);
    do
      ./delete_state.sh

      ${WORKLOADS[wl]} ${KEY_SIZES[size_idx]} ${VALUE_SIZES[size_idx]} ${PREFILL_SIZES[size_idx]} ${BENCHMARK_SIZES[size_idx]}
      ((throughput=$(cat out.txt | grep -- "- Completed" | cut -d" " -f3 | cut -d "." -f 1)))

      printf "${NAMES_VAR[i]}," >> throughput_varying_payload.csv
      printf "48," >> throughput_varying_payload.csv
      printf "${BENCHMARK_SIZES[size_idx]}," >> throughput_varying_payload.csv
      printf "${BENCHMARK_SIZES[size_idx]}," >> throughput_varying_payload.csv
      printf "${WORKLOADS[wl]}," >> throughput_varying_payload.csv
      printf "${KEY_SIZES[size_idx]}," >> throughput_varying_payload.csv
      printf "${VALUE_SIZES[size_idx]}," >> throughput_varying_payload.csv
      printf "$throughput\n" >> throughput_varying_payload.csv
    done
  done
done