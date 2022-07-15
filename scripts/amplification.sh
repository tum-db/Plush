#!/bin/bash

#This script takes data that is created MANUALLY - you need to parse the dimmstate.csv file before and after each
#benchmark to create the numbers for type==device as seen in the provided amplification.csv -
#TODO: Automate this!

SIZE=100000000
#SIZE=20
rm amplification.csv

printf "datastructure,elemcount,type,operation,medium,rw,bytes\n" >> amplification.csv

source ./env.sh

for i in $(seq 0 1 7);
do
    ./delete_state.sh


    # Read amplification
    ../build/pibench/src/PiBench ${LOCATIONS[i]} -l --name ${NAMES[i]} -t 1 -k 8 -v 8 -n $SIZE -p $SIZE -r 1 -i 0 | tee out.txt

    ((read_dram_read_bytes=$(cat out.txt | grep "DRAM Reads" | cut -d" " -f4)))
    ((read_dram_write_bytes=$(cat out.txt | grep "DRAM Writes" | cut -d" " -f4)))
    ((read_pmem_read_bytes=$(cat out.txt | grep "NVM Reads" | cut -d" " -f4)))
    ((read_pmem_write_bytes=$(cat out.txt | grep "NVM Writes" | cut -d" " -f4)))


    printf "${NAMES[i]}," >> amplification.csv
    printf "$SIZE," >> amplification.csv
    printf "lookup,DRAM,read,logical," >> amplification.csv
    printf "$read_dram_read_bytes\n" >> amplification.csv

    printf "${NAMES[i]}," >> amplification.csv
    printf "$SIZE," >> amplification.csv
    printf "lookup,DRAM,write,logical," >> amplification.csv
    printf "$read_dram_write_bytes\n" >> amplification.csv

    printf "${NAMES[i]}," >> amplification.csv
    printf "$SIZE," >> amplification.csv
    printf "lookup,PMEM,read,logical," >> amplification.csv
    printf "$read_pmem_read_bytes\n" >> amplification.csv

    printf "${NAMES[i]}," >> amplification.csv
    printf "$SIZE," >> amplification.csv
    printf "lookup,PMEM,write,logical," >> amplification.csv
    printf "$read_pmem_write_bytes\n" >> amplification.csv


    ./delete_state.sh


  # Write amplification
  ../build/pibench/src/PiBench ${LOCATIONS[i]} -l --name -t 1 -k 8 -v 8 -n $SIZE -p $SIZE -r 0 -i 1 | tee out.txt

  ((write_dram_read_bytes=$(cat out.txt | grep "DRAM Reads" | cut -d" " -f4)))
  ((write_dram_write_bytes=$(cat out.txt | grep "DRAM Writes" | cut -d" " -f4)))
  ((write_pmem_read_bytes=$(cat out.txt | grep "NVM Reads" | cut -d" " -f4)))
  ((write_pmem_write_bytes=$(cat out.txt | grep "NVM Writes" | cut -d" " -f4)))


  printf "${NAMES[i]}," >> amplification.csv
  printf "$SIZE," >> amplification.csv
  printf "insert,DRAM,read,logical," >> amplification.csv
  printf "$write_dram_read_bytes\n" >> amplification.csv

  printf "${NAMES[i]}," >> amplification.csv
  printf "$SIZE," >> amplification.csv
  printf "insert,DRAM,write,logical," >> amplification.csv
  printf "$write_dram_write_bytes\n" >> amplification.csv

  printf "${NAMES[i]}," >> amplification.csv
  printf "$SIZE," >> amplification.csv
  printf "insert,PMEM,read,logical," >> amplification.csv
  printf "$write_pmem_read_bytes\n" >> amplification.csv

  printf "${NAMES[i]}," >> amplification.csv
  printf "$SIZE," >> amplification.csv
  printf "insert,PMEM,write,logical," >> amplification.csv
  printf "$write_pmem_write_bytes\n" >> amplification.csv
done