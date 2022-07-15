#!/bin/bash

#pmwatch -l -f pmwatch.csv

ipmctl show -performance > pmwatch.csv

READ_DIMM0=$(awk 'NR==6' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM0=$(awk 'NR==7' pmwatch.csv | cut -d '=' -f2)

READ_DIMM1=$(awk 'NR==15' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM1=$(awk 'NR==16' pmwatch.csv | cut -d '=' -f2)

READ_DIMM2=$(awk 'NR==24' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM2=$(awk 'NR==25' pmwatch.csv | cut -d '=' -f2)

READ_DIMM3=$(awk 'NR==33' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM3=$(awk 'NR==34' pmwatch.csv | cut -d '=' -f2)

READ_DIMM4=$(awk 'NR==42' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM4=$(awk 'NR==43' pmwatch.csv | cut -d '=' -f2)

READ_DIMM5=$(awk 'NR==51' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM5=$(awk 'NR==52' pmwatch.csv | cut -d '=' -f2)

READ_DIMM0_LOGICAL=$(awk 'NR==8' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM0_LOGICAL=$(awk 'NR==9' pmwatch.csv | cut -d '=' -f2)

READ_DIMM1_LOGICAL=$(awk 'NR==17' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM1_LOGICAL=$(awk 'NR==18' pmwatch.csv | cut -d '=' -f2)

READ_DIMM2_LOGICAL=$(awk 'NR==26' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM2_LOGICAL=$(awk 'NR==27' pmwatch.csv | cut -d '=' -f2)

READ_DIMM3_LOGICAL=$(awk 'NR==35' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM3_LOGICAL=$(awk 'NR==36' pmwatch.csv | cut -d '=' -f2)

READ_DIMM4_LOGICAL=$(awk 'NR==44' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM4_LOGICAL=$(awk 'NR==45' pmwatch.csv | cut -d '=' -f2)

READ_DIMM5_LOGICAL=$(awk 'NR==53' pmwatch.csv | cut -d '=' -f2)
WRITE_DIMM5_LOGICAL=$(awk 'NR==54' pmwatch.csv | cut -d '=' -f2)


TOTAL_READS_DEVICE=$((READ_DIMM0 + READ_DIMM1 + READ_DIMM2 + READ_DIMM3 + READ_DIMM4 + READ_DIMM5))
TOTAL_WRITES_DEVICE=$((WRITE_DIMM0 + WRITE_DIMM1 + WRITE_DIMM2 + WRITE_DIMM3 + WRITE_DIMM4 + WRITE_DIMM5))

TOTAL_READS_LOGICAL=$((READ_DIMM0_LOGICAL + READ_DIMM1_LOGICAL + READ_DIMM2_LOGICAL + READ_DIMM3_LOGICAL + READ_DIMM4_LOGICAL + READ_DIMM5_LOGICAL))
TOTAL_WRITES_LOGICAL=$((WRITE_DIMM0_LOGICAL + WRITE_DIMM1_LOGICAL + WRITE_DIMM2_LOGICAL + WRITE_DIMM3_LOGICAL + WRITE_DIMM4_LOGICAL + WRITE_DIMM5_LOGICAL))


READ_BYTES_DEVICE=$(((TOTAL_READS_DEVICE - TOTAL_WRITES_DEVICE) * 64))
WRITE_BYTES_DEVICE=$((TOTAL_WRITES_DEVICE * 64))

READ_BYTES_LOGICAL=$(((TOTAL_READS_LOGICAL - TOTAL_WRITES_LOGICAL) * 64))
WRITE_BYTES_LOGICAL=$((TOTAL_WRITES_LOGICAL * 64))

# $1 is name, $2 is operation type, $3 is pre (before start) or post (after finish)
printf "$1,$2,$3,$READ_BYTES_DEVICE,$WRITE_BYTES_DEVICE,$READ_BYTES_LOGICAL,$WRITE_BYTES_LOGICAL\n" >> dimmstate.csv