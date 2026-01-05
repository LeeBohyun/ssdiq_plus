<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="logo/logo.svg">
    <source media="(prefers-color-scheme: dark)" srcset="logo/logo-dark.svg">
    <img alt="ZLeanStore logo" src="logo/logo.svg" height="80">
  </picture>
</div>

# SSD-iq for ZLeanStore

This repository contains benchmarking and simulation tools for **ZLeanStore**.

The ZNS-like write pattern, the **NOWA (Non-Overwriting Write Access)** pattern, and an LSM-engine-like write pattern have been added.  
Furthermore, a naive wear-leveling algorithm has been added to the SSD simulator.

---

## Repository Contents

- `iob/` — I/O benchmarking tool with support for ZNS and zone-aware patterns  
- `sim/` — Simulator for zone-based GC and cleaning behavior  
- `shared/` — Shared utilities and workload generators  
  - `patterngen.hpp` — Implementation of access patterns (uniform, zipf, zones, NOWA, etc.)  
- `scripts/` — Automation for running experiments (both simulated and real SSDs)  
- `paper/` — Plotting and analysis scripts  

---

## Access Patterns

### Uniform

Random uniform access over the logical address space.  
Overwrites freely, used as a baseline.

```sh
PATTERN=uniform
```

---

### Zipf

Skewed random access following a Zipfian distribution.  
Models hot/cold separation, overwrite-heavy.

```sh
PATTERN=zipf
ZIPF=0.9
```

---

### Zones

Multiple logical zones with independent sequential write pointers.  
Zones can be interleaved and allow overwrites after resets.

```sh
PATTERN=zones
ZONES="s0.9 f0.1 s0.1 f0.9"
```
---

### ZNS-like pattern (Non-Overwriting Write Access)

Strictly ZNS-like workload:

- Sequential writes per zone  
- Models host-managed ZNS behavior

```sh
PATTERN=zns
```
#### Optional Parameters

| Variable | Description |
|----------|-------------|
| `ZNS_ACTIVE_ZONES` | Number of concurrently open zones |
| `ZNS_ZONE_SIZE` | Size of each zone |

Example:

```sh
PATTERN=zns ZNS_ACTIVE_ZONES=14 ZNS_ZONE_SIZE=1024M RATE=0
```

---

### NOWA (No Write Amplification)

Write pattern that guarantees SSD WAF ~= 1:

- Sequential writes per zone  
- Estimate multiplexing within concurrently open zones and issue compensation writes
- Targets WAF ≈ 1  

```sh
PATTERN=nowa
```

#### Optional Parameters

| Variable | Description |
|----------|-------------|
| `ZNS_ACTIVE_ZONES` | Number of concurrently open zones |
| `ZNS_ZONE_SIZE` | Size of each zone |

Example:

```sh
PATTERN=nowa ZNS_ACTIVE_ZONES=64 ZNS_ZONE_SIZE=128M RATE=0
```

---

## Building

### Dependencies

```sh
apt install cmake build-essential nvme-cli libaio-dev liburing-dev libnvme-dev
```

### Build

```sh
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```

---

## Running IOB

### Simple NOWA Example


```sh
PATTERN=nowa ZNS_ACTIVE_ZONES=64 ZNS_ZONE_SIZE=128M RATE=0 \
iob/iob --filename=/blk/w0 --rw=1
```

---

## Simulator

```sh
sim/sim --capacity=20G --erase=1M --page=4k --ssdfill=0.875 \
  --pattern=zones --zones="s0.9 f0.1 s0.1 f0.9" --gc=greedy --writes=10
```

---

## Benchmarks & Reproducibility

### Scripts

- `benchwa.sh` — Write amplification experiments  
- `benchseq.sh` — Sequential and ZNS/NOWA workloads  
- `benchlat.sh` — Latency under load  
- `benchbench.sh` — Summary table generation  

### Analysis

- `paper.R` — Throughput and WAF plots  
- `latency.R` — Latency plots  
- `plotsim.R` — Simulator plots  

---

## Citation
- For original SSD-iq paper:
```bibtex
@article{DBLP:journals/pvldb/HaasLBL25,
  author       = {Gabriel Haas and Bohyun Lee and Philippe Bonnet and Viktor Leis},
  title        = {SSD-iq: Uncovering the Hidden Side of SSD Performance},
  journal      = {Proc. VLDB Endow.},
  volume       = {18},
  number       = {11},
  pages        = {4295--4308},
  year         = {2025}
}
```

---

## License

This project is licensed under the MIT License.
