#!/bin/bash

# Array of GC options
gc_options=("greedy") # "2r-greedy" "2r-fifo"

# Array of skew factors
#zone_factors=("s0.5 f0.5 s0.5 f0.5" "s0.6 f0.4 s0.4 f0.6" "s0.7 f0.3 s0.3 f0.7" "s0.8 f0.2 s0.2 f0.8" "s0.9 f0.1 s0.1 f0.9" "s0.95 f0.05 s0.05 f0.95")


cd ../build/
OVERWRITE=4
# Loop over each GC option
for PS in 4K ; do
for ZNS_ACTIVE in 16 ; do  # 2 4 8
for ZNS_SIZE in 16M ; do   #64K 256K 1M  8G  256K 1M 4M 16M 64M 256M 1G 4G 
for gc in "${gc_options[@]}"; do
    # Loop over each skew factor
    # Run the command with current GC and skew factor
    GC="$gc" PATTERN="zns-noshuffle" ZNS_ACTIVE_ZONES="$ZNS_ACTIVE" ZNS_ZONE_SIZE="$ZNS_SIZE" WRITES=10 PAGE="$PS" BLOCKSZ=16M CAPACITY=16G SSDFILL=0.875 ./sim/sim | tee trx.csv
done
done
done
done