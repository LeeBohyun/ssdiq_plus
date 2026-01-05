#!/bin/bash
set -x

export FILENAME="$1"
export PREFIX="$2"
UNI_BS="${3:-4K}"
SEQ_BS="${4:-512K}"

export IOENGINE=io_uring
export FILL=1

cd ../build/
cmake -DCMAKE_BUILD_TYPE=Release ..
make iob

# Get controller device by removing namespace suffix (nvme0n1 -> nvme0)
CONTROLLER_NAME="$(echo "$FILENAME" | sed -E 's/n[0-9]+(p[0-9]+)?$//')"

sanitize_nvme() {
    echo "sanitize"
    sudo nvme sanitize --sanact=2 "$FILENAME"
    sleep 1m
    sudo nvme sanitize-log "$CONTROLLER_NAME"
    sleep 1m
    sudo blkdiscard -v "$FILENAME"
    sleep 1m
}

sanitize_nvme_and_init() {
	sanitize_nvme
	sudo -E INIT=disable IO_SIZE_PERCENT_FILE=1 IO_DEPTH=1  FILL=0.99  BS=$SEQ_BS THREADS=1 PATTERN=sequential-noshuffle RW=1 SSDFILL= iob >> iob-output-$PREFIX.csv
}

SHUFFLE="-noshuffle"

zone_factors=(
  "s0.98 f0.02 s0.02 f0.98"
  "s0.97 f0.03 s0.03 f0.97"
  "s0.96 f0.04 s0.04 f0.96"
)

OVERWRITE=4
IOD=128
pattern="zones${SHUFFLE}"

for zone in "${zone_factors[@]}"; do
  for BS in 512K; do
    for THR in 8; do
	  	sanitize_nvme
      	echo "Let's test the zoned pattern with BS=${BS} and ZONES=${zone}!"
		FILENAME="$FILENAME" INIT=yes FILL=1 IO_SIZE_PERCENT_FILE="$OVERWRITE" \
		IO_DEPTH="$IOD" BS="$BS" THREADS="$THR" PATTERN="zones${SHUFFLE}" \
		ZONES="$zone" RATE=0 RW=1 \
		sudo -E ./iob/iob >> "iob-output-${PREFIX}.csv"
    done
  done
done

