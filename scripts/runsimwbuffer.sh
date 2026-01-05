#!/bin/bash
set -euo pipefail

# Array of GC options
gc_options=("greedy")

# Array of zone factors
zone_factors=(
  "s0.999 f0.001 s0.001 f0.999"
  "s0.995 f0.005 s0.005 f0.995"
  "s0.99 f0.01 s0.01 f0.99"
  "s0.95 f0.05 s0.05 f0.95"
  "s0.9 f0.1 s0.1 f0.9"
)

cd ../build/

export CAPACITY=64G
export ERASE=8M
export PAGE=4k
export SSDFILL=0.875
export WRITES=20

  # zones
  for zone in "${zone_factors[@]}"; do
    pattern="zones"
    szone="$(echo "$zone" | tr ' ' '_')"

    for gc in "${gc_options[@]}"; do
      out_name="sim-${gc}-${pattern}-${szone}"
      echo $out_name
      PATTERN="$pattern" ZONES="$zone" GC="$gc" \
        sim/sim "$out_name" | tee "${out_name}.csv"
    done
  done
