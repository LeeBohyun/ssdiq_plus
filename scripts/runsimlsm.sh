#!/bin/bash
set -euo pipefail

# GC options to test
gc_options=("greedy") # "2r-greedy" "2r-fifo"

# LSM pattern notes (based on your implementation):
# - Data region: ZNS-like writes with fixed 512KiB zones (internal), so ZNS_ZONE_SIZE loop is not meaningful here.
# - WAL region: sequential append for 1GiB at an offset (internal).
# - Data:WAL frequency is fixed at 8:1 (internal).
# The only ZNS knob that still matters externally is ZNS_ACTIVE_ZONES.

cd ../build/ || exit 1

OVERWRITE=4

# Experiment parameters
PAGE_SIZES=("4K")
ZNS_ACTIVES=(16)          # e.g., (2 4 8 16)
WRITES_PCT=20
BLOCKSZ="16M"
CAPACITY="64G"
SSDFILL="0.875"

# Output directory + file naming
OUTDIR="results_lsm"
mkdir -p "${OUTDIR}"

for PS in "${PAGE_SIZES[@]}"; do
  for ZNS_ACTIVE in "${ZNS_ACTIVES[@]}"; do
    for gc in "${gc_options[@]}"; do

      OUT="${OUTDIR}/trx_lsm_gc-${gc}_ps-${PS}_active-${ZNS_ACTIVE}.csv"

      echo "=== Running LSM: GC=${gc} PAGE=${PS} ZNS_ACTIVE_ZONES=${ZNS_ACTIVE} -> ${OUT}"

      GC="${gc}" PATTERN="lsmnowa-noshuffle" \
        ZNS_ACTIVE_ZONES="${ZNS_ACTIVE}" ZNS_ZONE_SIZE="${BLOCKSZ}" \
        WRITES="${WRITES_PCT}" PAGE="${PS}" BLOCKSZ="${BLOCKSZ}" \
        CAPACITY="${CAPACITY}" SSDFILL="${SSDFILL}" OVERWRITE="${OVERWRITE}" \
        ./sim/sim | tee "${OUT}"

    done
  done
done
