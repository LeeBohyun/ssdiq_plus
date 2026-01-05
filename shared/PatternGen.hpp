#pragma once

#include "Exceptions.hpp"
#include "Env.hpp"
#include "../traces/src/ParseTraces.hpp"
#include "RejectionInversionZipf.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace iob {

using std::cout;
using std::endl;
using std::string;
namespace fs = std::filesystem;

class PatternGen {
public:
    enum class Pattern {
        Sequential,
        Uniform,
        Beta,
        Zones,
        Zipf,
        FioZipf,
        Traces,
        DBOutOfPlace,
        ZNS,
        NoWA,
        SeqZones,
        LSM,
        LSMNoWA,
        Undefined
    };

    struct Options {
        string patternString;
        string zonesString;

        uint64_t logicalPages = 0;
        double alpha = 1.0;
        double beta = 1.0;
        double skewFactor = 1.0;

        uint64_t totalWrites = 0;
        uint64_t pageSize = 4096;
        uint64_t sectorSize = 512;

        uint64_t znsActiveZones = 4;
        uint64_t znsPagesPerZone = 0;
    };

    Options options;
    const Pattern pattern;
    bool shuffle = false;

    // Shuffle
    std::vector<uint64_t> updatePattern;

    // Sequential
    std::atomic<uint64_t> seq{0};

    // Uniform
    std::uniform_int_distribution<uint64_t> rndPage;

    // Zones / SeqZones
    struct AccessZone {
        uint64_t offset = 0;
        uint64_t count = 0;
        double rel_size = 1.0;
        double freq = 100.0;
        Pattern pattern = Pattern::Uniform;
        double skewFactor = 0.0;
        bool shuffle = false;
        std::unique_ptr<PatternGen> subGen;

        AccessZone(double rel_size, double rel_freq, Pattern pattern, double skewFactor, bool shuffle)
            : rel_size(rel_size)
            , freq(rel_freq)
            , pattern(pattern)
            , skewFactor(skewFactor)
            , shuffle(shuffle)
        {}

        void print() const {
            std::cout << "{o:" << offset << " c:" << count << " f:" << freq
                      << " rels:" << rel_size << " p:" << (uint64_t)pattern
                      << " shuffle:" << shuffle << "}";
        }
    };

    // Zipf for page selection
    RejectionInversionZipfSampler zipfSampler;

    // Traces
    std::vector<uint64_t> inputTraces;
    size_t traceIndex = 0;
    std::string traceFilePath;
    const size_t chunkSize = 100000;

    // ---------------- ZNS / NoWA state ----------------
    uint64_t znsZones = 0;
    std::mutex znsMutex;

    // Per active slot: zone id and offset in zone
    std::vector<int64_t>  znsActiveZoneIds;
    std::vector<uint64_t> znsActiveZoneOffset;

    // Per zone invalidation count
    std::vector<uint64_t> znsZoneInvalidationCount;

    // Group history: list of zones that were active together
    std::vector<std::vector<uint64_t>> znsZoneGroups;

    // groupEligible[g] == 1 -> consider this group
    std::vector<uint8_t> groupEligible;

    uint64_t reorganized_gidx = 0;
    uint64_t curmaxopenzonecnt = 0;

    // Slot selection distribution (built once)
    std::discrete_distribution<int> activeSlotDist;
    bool activeSlotDistInit = false;

    // Zipf sampler for zone selection (cannot be assigned; keep via ptr)
    std::unique_ptr<RejectionInversionZipfSampler> zipfZoneSampler;

    // Cached active slot count
    uint64_t activeCount = 0;

    // ---------------- Fast group metadata ----------------
    struct GroupMeta {
        uint32_t n = 0;
        uint64_t minF = 0;
        uint64_t maxF = 0;
        bool initialized = false;
        std::unordered_map<uint64_t, uint32_t> bucket; // freq -> count
    };

    std::vector<GroupMeta> groupMeta;

    // reverse index: zone -> groups that contain it
    std::vector<std::vector<uint32_t>> zoneToGroups;

    // imbalanced group bookkeeping (threshold==0 => maxF>minF)
    std::vector<uint8_t> groupImbalanced;
    std::vector<uint32_t> imbalancedGroups;
    std::vector<int64_t> posInImbalanced; // gid -> position in imbalancedGroups or -1

    // ---------------- LSM state ----------------
    uint64_t lsmDataPages = 0;     // region 0
    uint64_t lsmWalPages = 0;      // region 1 length
    uint64_t lsmWalOffset = 0;     // start of WAL region in page-address space
    std::atomic<uint64_t> lsmWalSeq{0};
    std::bernoulli_distribution lsmPickWal{1.0 / 9.0}; // 1 WAL : 8 data

    // ===========================
// 2) Add LSMNoWA state (near your existing LSM state)
// ===========================

uint64_t lsmSuperblockPages = 0;   // superblock size in pages (from ZNS_ZONE_SIZE)
uint64_t lsmSuperblockCnt   = 0;   // number of superblocks in (logical - WAL) region

uint64_t lsmRegion0Pages    = 0;   // [0, region1Offset)
uint64_t lsmRegion1Offset   = 0;   // (sbCnt-2)*sbPages
uint64_t lsmRegion1Pages    = 0;   // exactly 1 superblock
uint64_t lsmWalOffset2      = 0;   // (sbCnt-1)*sbPages

std::atomic<uint64_t> lsmRegion0Writes{0}; // cumulative writes to region0

// compaction (region1) burst control
std::atomic<uint64_t> lsmR1Pos{0};
std::atomic<uint64_t> lsmR1Remaining{0};

public:
    PatternGen(Options options)
        : options(options)
        , pattern(stringToPattern(options.patternString))
        , zipfSampler(options.logicalPages, options.skewFactor)
    {
        shuffle = !options.patternString.contains("-noshuffle");
        if (pattern == Pattern::Sequential
            || pattern == Pattern::SeqZones
            || pattern == Pattern::ZNS
            || pattern == Pattern::NoWA
            || pattern == Pattern::LSM
            || pattern == Pattern::LSMNoWA ) {
            shuffle = options.patternString.contains("-shuffle");
        }

        rndPage = std::uniform_int_distribution<uint64_t>(0, options.logicalPages - 1);
        std::cout << "PatternGen: shuffle=" << shuffle
                  << " totalWrites=" << options.totalWrites << std::endl;
        init();
    }

    PatternGen(Pattern pattern, uint64_t logicalPages, double skewFactor = 1.0, bool shuffle = true)
        : options{}
        , pattern(pattern)
        , shuffle(shuffle)
        , zipfSampler(logicalPages, skewFactor)
    {
        options.logicalPages = logicalPages;
        options.skewFactor = skewFactor;
        rndPage = std::uniform_int_distribution<uint64_t>(0, options.logicalPages - 1);
        init();
    }

    std::string patternDetails() const {
        std::string details;
        if (pattern == Pattern::Zones || pattern == Pattern::SeqZones) {
            details = options.zonesString;
        } else if (pattern == Pattern::Beta) {
            details = "a:" + std::to_string(options.alpha) + " b:" + std::to_string(options.beta);
        } else if (pattern == Pattern::Zipf || pattern == Pattern::FioZipf) {
            details = std::to_string(options.skewFactor);
        } else if (pattern == Pattern::ZNS || pattern == Pattern::NoWA) {
            details = "activeZones:" + std::to_string(options.znsActiveZones) +
                      " pagesPerZone:" + std::to_string(options.znsPagesPerZone);
        } else if (pattern == Pattern::LSM) {
            details = "LSM(dataPages:" + std::to_string(lsmDataPages) +
                      " walPages:" + std::to_string(lsmWalPages) +
                      " walOffset:" + std::to_string(lsmWalOffset) + ")";
        }  else if (pattern == Pattern::LSMNoWA) {
            uint64_t r0Start = 0;
            uint64_t r0End   = lsmRegion1Offset;

            uint64_t r1Start = lsmRegion1Offset;
            uint64_t r1End   = lsmRegion1Offset + lsmRegion1Pages;

            uint64_t walStart = lsmWalOffset2;
            uint64_t walEnd   = lsmWalOffset2 + lsmWalPages;

            details =
                "LSMNoWA "
                "sbPages=" + std::to_string(lsmSuperblockPages) +
                " sbCnt="   + std::to_string(lsmSuperblockCnt) +
                " | r0:["   + std::to_string(r0Start) + ".." + std::to_string(r0End - 1) + "]" +
                " | r1:["   + std::to_string(r1Start) + ".." + std::to_string(r1End - 1) + "]" +
                " | wal:["  + std::to_string(walStart) + ".." + std::to_string(walEnd - 1) + "]";
        } else if (pattern == Pattern::Sequential) {
            details = "Sequential init begin" ;
        }

        return details;
    }

    static Options loadOptionsFromEnv(uint64_t logicalPages, uint64_t pageSize) {
        Options pgOptions;
        pgOptions.logicalPages = logicalPages;
        pgOptions.pageSize = pageSize;

        pgOptions.patternString = getEnv("PATTERN", "uniform");
        pgOptions.zonesString = getEnv("ZONES", "s0.9 f0.1 uniform s0.1 f0.9 uniform");

        pgOptions.alpha = std::stod(getEnv("ALPHA", "1"));
        pgOptions.beta  = std::stod(getEnv("BETA",  "1"));

        uint64_t writePercentage = std::stoull(getEnv("WRITES", "10"));
        pgOptions.totalWrites = static_cast<uint64_t>(writePercentage * logicalPages);

        pgOptions.sectorSize = std::stoull(getEnv("SECTOR_SIZE", "512"));
        pgOptions.znsActiveZones = std::stoull(getEnv("ZNS_ACTIVE_ZONES", "4"));
        pgOptions.znsPagesPerZone = getBytesFromString(getEnv("ZNS_ZONE_SIZE", "1G")) / pageSize;

        pgOptions.skewFactor = 1.0;
        if (pgOptions.patternString.contains("zipf")) {
            pgOptions.skewFactor = std::stod(getEnv("ZIPF", "1.0"));
        }
        if (!pgOptions.patternString.contains("zones")) {
            pgOptions.zonesString.clear();
        }
        return pgOptions;
    }

    static Pattern stringToPattern(std::string pattern) {
        if (pattern.contains("sequential")) return Pattern::Sequential;
        if (pattern.contains("uniform"))    return Pattern::Uniform;
        if (pattern.contains("beta"))       return Pattern::Beta;
        if (pattern.contains("seqzones"))   return Pattern::SeqZones;
        if (pattern.contains("zones"))      return Pattern::Zones;
        if (pattern.contains("fiozipf"))    return Pattern::FioZipf;
        if (pattern.contains("zipf"))       return Pattern::Zipf;
        if (pattern.contains("trace"))      return Pattern::Traces;
        if (pattern.contains("zns"))        return Pattern::ZNS;
        if (pattern.contains("lsmnowa"))    return Pattern::LSMNoWA;
        if (pattern.contains("nowa"))       return Pattern::NoWA;
        if (pattern.contains("lsm"))        return Pattern::LSM;
        ensurem(false, "pattern does not exist: " + pattern);
        return Pattern::Undefined;
    }

    void init() {
        if (pattern == Pattern::FioZipf) {
            generateFioZipfTraces(options.skewFactor, options.logicalPages, options.totalWrites);
        } else if (pattern == Pattern::Traces) {
            traceFilePath = getTraceFilePath(options.patternString);
            validateAndLoadTraceFiles(traceFilePath, options.patternString,
                                      options.sectorSize, options.logicalPages,
                                      options.pageSize, inputTraces);
        } else if (pattern == Pattern::Zones) {
            parseAndInitZoneAccessPattern();
        } else if (pattern == Pattern::SeqZones) {
            parseAndInitSeqZoneAccessPattern();
        } else if (pattern == Pattern::ZNS) {
            parseAndInitZNSAccessPattern();
        } else if (pattern == Pattern::NoWA) {
            parseAndInitNoWAAccessPattern();
        } else if (pattern == Pattern::LSM) {
            parseAndInitLSMAccessPattern();
        }else if (pattern == Pattern::LSMNoWA) {
            parseAndInitLSMNoWAAccessPattern();
        }

        if (shuffle) {
            updatePattern.resize(options.logicalPages);
            std::iota(updatePattern.begin(), updatePattern.end(), 0ULL);
            std::random_device rd;
            std::mt19937_64 g(rd());
            std::shuffle(updatePattern.begin(), updatePattern.end(), g);
        }
    }

    int64_t accessPatternGenerator(std::mt19937_64& gen) {
        uint64_t page = 0;

        if (pattern == Pattern::Sequential) {
            page = seq++ % options.logicalPages;
        } else if (pattern == Pattern::Uniform) {
            page = rndPage(gen);
        } else if (pattern == Pattern::SeqZones || pattern == Pattern::Zones) {
            page = accessZonesGenerator(gen);
        } else if (pattern == Pattern::Beta) {
            double beta_val = beta_distribution(gen, options.alpha, options.beta);
            page = (uint64_t)(beta_val * (options.logicalPages - 1));
        } else if (pattern == Pattern::Zipf) {
            page = zipfSampler.sample(gen) - 1;
        } else if (pattern == Pattern::FioZipf) {
            page = getPageFromFIOTrace();
        } else if (pattern == Pattern::Traces) {
            page = getPageFromParsedTrace(inputTraces, traceIndex, chunkSize);
        } else if (pattern == Pattern::ZNS) {
            if (seq < (options.znsPagesPerZone * (znsZones - (znsZones % options.znsActiveZones)))) {
                page = seq++ % options.logicalPages;
            } else {
                page = accessZNS(gen);
            }
        } else if (pattern == Pattern::NoWA) {
            if (seq < (options.znsPagesPerZone * (znsZones - (znsZones % options.znsActiveZones)))) {
                page = seq++ % options.logicalPages;
            } else {
                page = accessNoWA(gen);
            }
        } else if (pattern == Pattern::LSM) {
            page = accessLSM(gen);
        }  else if (pattern == Pattern::LSMNoWA) {
            page = accessLSMNoWA(gen);
        } else {
            throw std::runtime_error("Error: pattern not implemented.");
        }

        if (shuffle) {
            if (!(page < updatePattern.size())) raise(SIGINT);
            page = updatePattern.at(page);
        }

        ensure(page < options.logicalPages);
        return (int64_t)page;
    }

private:
    // ---------------- Zones pattern init ----------------
    double sumFreq = 0.0;
    std::vector<AccessZone> accessZones;

    void parseZoneSizes(const std::string& str) {
        std::stringstream ss(str);
        std::string value;

        double size = -1.0;
        double freq = -1.0;
        Pattern pat = Pattern::Uniform;
        double skew = -1.0;
        bool shuf = false;

        auto addZone = [&]() {
            if (size > 0) {
                accessZones.emplace_back(size, freq, pat, skew, shuf);
                size = -1; freq = -1; pat = Pattern::Uniform; skew = -1; shuf = false;
            }
        };

        while (ss >> value) {
            if (value[0] == 's' && value != "shuffle" && value != "sequential") {
                addZone();
                size = std::stod(value.substr(1));
            } else if (value[0] == 'f') {
                freq = std::stod(value.substr(1));
            } else if (value == "uniform") {
                pat = Pattern::Uniform;
            } else if (value.rfind("zipf", 0) == 0) {
                pat = Pattern::Zipf;
                skew = std::stod(value.substr(4));
            } else if (value == "sequential") {
                pat = Pattern::Sequential;
            } else if (value == "shuffle") {
                shuf = true;
            } else if (value == "noshuffle") {
                shuf = false;
            } else {
                ensurem(false, "Zone keyword not recognized: " + value);
            }
        }
        addZone();
    }

    void initZoneAccessPattern() {
        const long pageCount = (long)options.logicalPages;

        double sumRelSizes = 0;
        for (auto& hz : accessZones) sumRelSizes += hz.rel_size;

        long assigned = 0;
        sumFreq = 0.0;
        for (auto& h : accessZones) {
            h.count = (uint64_t)((double)h.rel_size / sumRelSizes * pageCount);
            assigned += (long)h.count;
            sumFreq += h.freq;
        }

        long remaining = pageCount - assigned;
        ensurem(remaining >= 0, "Remaining pages negative " + std::to_string(remaining));
        int i = 0;
        while (remaining > 0) {
            accessZones[i++ % (int)accessZones.size()].count += 1;
            remaining--;
        }

        long lastEnd = 0;
        long assigned2 = 0;
        for (auto& h : accessZones) {
            h.offset = (uint64_t)lastEnd;
            assigned2 += (long)h.count;
            lastEnd += (long)h.count;
        }
        ensurem(pageCount == assigned2, "Pages not correctly assigned");

        for (auto& h : accessZones) {
            h.subGen = std::make_unique<PatternGen>(h.pattern, h.count, h.skewFactor, h.shuffle);
        }
    }

    void parseAndInitZoneAccessPattern() {
        accessZones.clear();
        sumFreq = 0.0;
        parseZoneSizes(options.zonesString);
        initZoneAccessPattern();
    }

    void parseAndInitSeqZoneAccessPattern() {
        accessZones.clear();
        sumFreq = 0.0;

        const int zones = std::stoi(options.zonesString);
        float f = std::pow(10.0f, 1.0f / (zones - 1));
        for (int i = 0; i < zones; i++) {
            accessZones.emplace_back(1, std::pow(f, (float)i), Pattern::Sequential, -1, false);
        }
        initZoneAccessPattern();
    }

    uint64_t accessZonesGenerator(std::mt19937_64& gen) {
        std::uniform_real_distribution<double> realDist(0, sumFreq);
        double randFreq = realDist(gen);

        int randZoneId = 0;
        double freqCnt = accessZones.at(0).freq;
        while (freqCnt < randFreq) {
            randZoneId++;
            freqCnt += accessZones.at(randZoneId).freq;
        }

        auto& az = accessZones.at(randZoneId);
        return az.offset + (uint64_t)az.subGen->accessPatternGenerator(gen);
    }

    // ---------------- Slot distribution ----------------
    void initActiveSlotDistribution() {
        std::vector<double> weights;
        weights.reserve(accessZones.size());
        for (auto& az : accessZones) weights.push_back(az.freq);
        activeSlotDist = std::discrete_distribution<int>(weights.begin(), weights.end());
        activeSlotDistInit = true;
    }

    // ---------------- Group metadata helpers ----------------
    void ensureGroupArraysSize(uint32_t gid) {
        if (groupMeta.size() <= gid) groupMeta.resize(gid + 1);
        if (groupEligible.size() <= gid) groupEligible.resize(gid + 1, 1);
        if (groupImbalanced.size() <= gid) groupImbalanced.resize(gid + 1, 0);
        if (posInImbalanced.size() <= gid) posInImbalanced.resize(gid + 1, -1);
    }

    void setGroupImbalanced(uint32_t gid, bool isImbalanced) {
        ensureGroupArraysSize(gid);

        uint8_t want = isImbalanced ? 1 : 0;
        if (groupImbalanced[gid] == want) return;

        groupImbalanced[gid] = want;
        int64_t& pos = posInImbalanced[gid];

        if (want) {
            pos = (int64_t)imbalancedGroups.size();
            imbalancedGroups.push_back(gid);
        } else {
            if (pos >= 0) {
                uint32_t back = imbalancedGroups.back();
                imbalancedGroups[(size_t)pos] = back;
                posInImbalanced[back] = pos;
                imbalancedGroups.pop_back();
                pos = -1;
            }
        }
    }

    void initGroupMeta(uint32_t gid, const std::vector<uint64_t>& groupZones) {
        ensureGroupArraysSize(gid);

        GroupMeta m;
        m.n = (uint32_t)groupZones.size();
        m.bucket.reserve(groupZones.size() * 2);

        uint64_t minF = std::numeric_limits<uint64_t>::max();
        uint64_t maxF = 0;

        for (uint64_t zid : groupZones) {
            if (zid < zoneToGroups.size()) zoneToGroups[zid].push_back(gid);

            uint64_t f = znsZoneInvalidationCount[zid];
            m.bucket[f] += 1;
            if (f < minF) minF = f;
            if (f > maxF) maxF = f;
        }

        m.minF = (minF == std::numeric_limits<uint64_t>::max()) ? 0 : minF;
        m.maxF = maxF;
        m.initialized = true;

        groupMeta[gid] = std::move(m);
        setGroupImbalanced(gid, (groupMeta[gid].maxF > groupMeta[gid].minF));
    }

    void onZoneInvalidationInc(uint64_t zid, uint64_t oldF, uint64_t newF) {
        if (zid >= zoneToGroups.size()) return;
        const auto& gs = zoneToGroups[zid];

        for (uint32_t gid : gs) {
            if (gid >= groupMeta.size()) continue;
            auto& m = groupMeta[gid];
            if (!m.initialized) continue;

            auto itOld = m.bucket.find(oldF);
            if (itOld != m.bucket.end()) {
                if (itOld->second == 1) m.bucket.erase(itOld);
                else itOld->second -= 1;
            }
            m.bucket[newF] += 1;

            if (newF > m.maxF) m.maxF = newF;

            if (oldF == m.minF && m.bucket.find(oldF) == m.bucket.end()) {
                while (m.minF < m.maxF && m.bucket.find(m.minF) == m.bucket.end()) m.minF++;
            }
            if (oldF == m.maxF && m.bucket.find(oldF) == m.bucket.end()) {
                while (m.maxF > m.minF && m.bucket.find(m.maxF) == m.bucket.end()) m.maxF--;
            }

            setGroupImbalanced(gid, (m.maxF > m.minF));
        }
    }

    void incrementZoneInvalidation(uint64_t zid) {
        uint64_t oldF = znsZoneInvalidationCount[zid];
        uint64_t newF = oldF + 1;
        znsZoneInvalidationCount[zid] = newF;
        onZoneInvalidationInc(zid, oldF, newF);
    }

    int detectRandomImbalancedGroupFast(std::mt19937_64& gen) {
        if (imbalancedGroups.empty()) return -1;

        for (int tries = 0; tries < 32; ++tries) {
            std::uniform_int_distribution<size_t> dist(0, imbalancedGroups.size() - 1);
            uint32_t gid = imbalancedGroups[dist(gen)];
            if (gid < groupEligible.size() && groupEligible[gid]) return (int)gid;
        }
        for (uint32_t gid : imbalancedGroups) {
            if (gid < groupEligible.size() && groupEligible[gid]) return (int)gid;
        }
        return -1;
    }

    // ---------------- Active-slot accounting ----------------
    void setSlotZone(uint64_t slot, int64_t zoneId, uint64_t offset) {
        int64_t old = znsActiveZoneIds[slot];
        if (old < 0 && zoneId >= 0) activeCount++;
        if (old >= 0 && zoneId < 0) activeCount--;
        znsActiveZoneIds[slot] = zoneId;
        znsActiveZoneOffset[slot] = offset;
    }

    uint64_t countActiveZonesFast() const { return activeCount; }

    // ---------------- ZNS / NoWA init ----------------
    void parseAndInitZNSAccessPattern() {
        ensure(options.logicalPages > options.znsPagesPerZone);
        znsZones = options.logicalPages / options.znsPagesPerZone;
        ensure(options.znsActiveZones < znsZones);

        accessZones.clear();
        sumFreq = 0.0;

        float f = std::pow(10.0f, 1.0f / (float)(options.znsActiveZones - 1));
        for (int i = 0; i < (int)options.znsActiveZones; i++) {
            accessZones.emplace_back(1, std::pow(f, (float)i), Pattern::Undefined, -1, false);
        }
        initZoneAccessPattern();
        initActiveSlotDistribution();

        znsActiveZoneIds.assign(options.znsActiveZones, -1);
        znsActiveZoneOffset.assign(options.znsActiveZones, 0);
        activeCount = 0;

        curmaxopenzonecnt = options.znsActiveZones;

        znsZoneInvalidationCount.assign(znsZones, 0);

        znsZoneGroups.clear();
        groupEligible.clear();

        groupMeta.clear();
        zoneToGroups.assign(znsZones, {});
        groupImbalanced.clear();
        imbalancedGroups.clear();
        posInImbalanced.clear();

        zipfZoneSampler = std::make_unique<RejectionInversionZipfSampler>(znsZones, 0.4);
    }

    void parseAndInitNoWAAccessPattern() {
        ensure(options.logicalPages > options.znsPagesPerZone);
        znsZones = options.logicalPages / options.znsPagesPerZone;
        ensure(options.znsActiveZones < znsZones);

        accessZones.clear();
        sumFreq = 0.0;

        for (int i = 0; i < (int)options.znsActiveZones; i++) {
            accessZones.emplace_back(1, 1.0, Pattern::Undefined, -1, false);
        }
        initZoneAccessPattern();
        initActiveSlotDistribution();

        znsActiveZoneIds.assign(options.znsActiveZones, -1);
        znsActiveZoneOffset.assign(options.znsActiveZones, 0);
        activeCount = 0;

        curmaxopenzonecnt = options.znsActiveZones;

        znsZoneInvalidationCount.assign(znsZones, 0);

        znsZoneGroups.clear();
        groupEligible.clear();

        groupMeta.clear();
        zoneToGroups.assign(znsZones, {});
        groupImbalanced.clear();
        imbalancedGroups.clear();
        posInImbalanced.clear();

        zipfZoneSampler = std::make_unique<RejectionInversionZipfSampler>(znsZones, 0.4);
    }

    // ---------------- Zone selection helpers ----------------
    uint64_t pickRandomZoneUniform(std::mt19937_64& gen) const {
        std::uniform_int_distribution<uint64_t> dist(0, znsZones - 1);
        return dist(gen);
    }

    // ---------------- NoWA group rotation (same policy, faster metadata) ----------------
    uint64_t getNewZoneToOpen(std::mt19937_64& gen) {
        std::vector<uint64_t> curGroup;
        curGroup.reserve(options.znsActiveZones);

        bool curAllZeroBeforeInc = true;
        for (size_t i = 0; i < options.znsActiveZones; ++i) {
            int64_t zid = znsActiveZoneIds[i];
            if (zid >= 0) {
                uint64_t uzid = (uint64_t)zid;
                curGroup.push_back(uzid);

                if (znsZoneInvalidationCount[uzid] != 0) curAllZeroBeforeInc = false;
                incrementZoneInvalidation(uzid);
            }
        }

        znsZoneGroups.push_back(curGroup);
        uint32_t group_index = (uint32_t)(znsZoneGroups.size() - 1);

        if (groupEligible.size() <= group_index) groupEligible.resize(group_index + 1, 1);
        groupEligible[group_index] = 1;
        initGroupMeta(group_index, curGroup);

        std::uniform_int_distribution<int> distOpen(1, (int)options.znsActiveZones);
        curmaxopenzonecnt = (uint64_t)distOpen(gen);

        const int worstgroup = detectRandomImbalancedGroupFast(gen);

        auto applyZones = [&](const std::vector<uint64_t>& chosen) -> uint64_t {
            curmaxopenzonecnt = chosen.size();
            for (size_t i = 0; i < options.znsActiveZones; ++i) {
                if (i < chosen.size()) setSlotZone((uint64_t)i, (int64_t)chosen[i], 0);
                else setSlotZone((uint64_t)i, -1, 0);
            }
            return (uint64_t)znsActiveZoneIds[0];
        };

        if (group_index >= 1 && worstgroup >= 0 && (size_t)worstgroup < znsZoneGroups.size()) {
            const auto& victim = znsZoneGroups[(size_t)worstgroup];
            if (!victim.empty()) {
                uint64_t maxFreq = znsZoneInvalidationCount[victim[0]];
                for (uint64_t zid : victim) {
                    maxFreq = std::max<uint64_t>(maxFreq, znsZoneInvalidationCount[zid]);
                }

                std::vector<uint64_t> candidates;
                candidates.reserve(victim.size());
                for (uint64_t zid : victim) {
                    if (znsZoneInvalidationCount[zid] < maxFreq) candidates.push_back(zid);
                }

                if (!candidates.empty()) {
                    const size_t K = std::min<size_t>(options.znsActiveZones, candidates.size());

                    if (curAllZeroBeforeInc) {
                        std::vector<uint64_t> zeros;
                        zeros.reserve(K);
                        for (uint64_t zid : candidates) {
                            if (znsZoneInvalidationCount[zid] == 0) zeros.push_back(zid);
                            if (zeros.size() == K) break;
                        }
                        if (!zeros.empty()) {
                            std::shuffle(zeros.begin(), zeros.end(), gen);
                            return applyZones(zeros);
                        }
                    }

                    auto cmp = [&](uint64_t a, uint64_t b) {
                        auto fa = znsZoneInvalidationCount[a];
                        auto fb = znsZoneInvalidationCount[b];
                        if (fa != fb) return fa < fb;
                        return a < b;
                    };

                    std::nth_element(candidates.begin(), candidates.begin() + K, candidates.end(), cmp);
                    candidates.resize(K);
                    std::shuffle(candidates.begin(), candidates.end(), gen);
                    return applyZones(candidates);
                }
            }
        }

        for (uint64_t i = 0; i < options.znsActiveZones; ++i) setSlotZone(i, -1, 0);

        if (!zipfZoneSampler) zipfZoneSampler = std::make_unique<RejectionInversionZipfSampler>(znsZones, 0.4);

        uint64_t newZone = 0;

        if (curAllZeroBeforeInc) {
            for (int attempt = 0; attempt < 64; ++attempt) {
                uint64_t cand = zipfZoneSampler->sample(gen) - 1;
                if (cand < znsZones && znsZoneInvalidationCount[cand] == 0) {
                    newZone = cand;
                    setSlotZone(0, (int64_t)newZone, 0);
                    reorganized_gidx = group_index;
                    return newZone;
                }
            }
        }

        do {
            newZone = zipfZoneSampler->sample(gen) - 1;
        } while (newZone >= znsZones);

        setSlotZone(0, (int64_t)newZone, 0);
        reorganized_gidx = group_index;
        return newZone;
    }

    // ---------------- ZNS baseline access ----------------
    uint64_t accessZNS(std::mt19937_64& gen) {
        if (!activeSlotDistInit) initActiveSlotDistribution();
        int slot = activeSlotDist(gen);
        if (slot < 0 || (size_t)slot >= options.znsActiveZones) slot = 0;

        std::lock_guard<std::mutex> guard(znsMutex);

        if (znsActiveZoneIds[slot] < 0) {
            uint64_t randomZone;
            do {
                randomZone = pickRandomZoneUniform(gen);
            } while (std::find(znsActiveZoneIds.begin(), znsActiveZoneIds.end(),
                               (int64_t)randomZone) != znsActiveZoneIds.end());
            setSlotZone((uint64_t)slot, (int64_t)randomZone, 0);
        }

        uint64_t idInZone = znsActiveZoneOffset[slot]++;

        if (idInZone >= options.znsPagesPerZone) {
            uint64_t randomZone;
            do {
                randomZone = pickRandomZoneUniform(gen);
            } while (std::find(znsActiveZoneIds.begin(), znsActiveZoneIds.end(),
                               (int64_t)randomZone) != znsActiveZoneIds.end());

            setSlotZone((uint64_t)slot, (int64_t)randomZone, 0);
            idInZone = znsActiveZoneOffset[slot]++;
        }

        uint64_t pid = (uint64_t)znsActiveZoneIds[slot] * options.znsPagesPerZone + idInZone;
        if (pid >= options.logicalPages) {
            std::cerr << "ZNS invalid pid=" << pid
                      << " slot=" << slot
                      << " zone=" << znsActiveZoneIds[slot]
                      << " off=" << idInZone
                      << " ppz=" << options.znsPagesPerZone
                      << " lp=" << options.logicalPages << "\n";
            raise(SIGINT);
        }
        return pid;
    }

    // ---------------- NoWA access (same pattern; fixes invalid pid + faster selection) ----------------
    uint64_t accessNoWA(std::mt19937_64& gen) {
        if (!activeSlotDistInit) initActiveSlotDistribution();
        int slot = activeSlotDist(gen);
        if (slot < 0 || (size_t)slot >= options.znsActiveZones) slot = 0;

        std::lock_guard<std::mutex> guard(znsMutex);

        auto slotValid = [&](int s) -> bool {
            if (s < 0 || (size_t)s >= options.znsActiveZones) return false;
            return znsActiveZoneIds[s] >= 0 && znsActiveZoneOffset[s] < options.znsPagesPerZone;
        };
        auto findAnyValidSlot = [&]() -> int {
            for (uint64_t i = 0; i < options.znsActiveZones; ++i) {
                if (znsActiveZoneIds[i] >= 0 && znsActiveZoneOffset[i] < options.znsPagesPerZone)
                    return (int)i;
            }
            return -1;
        };

        if (znsActiveZoneIds[slot] < 0) {
            if (countActiveZonesFast() < curmaxopenzonecnt) {
                if (!zipfZoneSampler) zipfZoneSampler = std::make_unique<RejectionInversionZipfSampler>(znsZones, 0.4);
                uint64_t newZone;
                do {
                    newZone = zipfZoneSampler->sample(gen) - 1;
                } while (newZone >= znsZones ||
                         std::find(znsActiveZoneIds.begin(), znsActiveZoneIds.end(),
                                   (int64_t)newZone) != znsActiveZoneIds.end());

                setSlotZone((uint64_t)slot, (int64_t)newZone, 0);
            } else {
                int reuse = findAnyValidSlot();
                if (reuse >= 0) {
                    slot = reuse;
                } else {
                    (void)getNewZoneToOpen(gen);
                    slot = 0;
                }
            }
        }

        if (!slotValid(slot)) {
            int reuse = findAnyValidSlot();
            if (reuse >= 0) slot = reuse;
            else {
                (void)getNewZoneToOpen(gen);
                slot = 0;
            }
        }

        if (znsActiveZoneIds[slot] < 0) {
            if (!zipfZoneSampler) zipfZoneSampler = std::make_unique<RejectionInversionZipfSampler>(znsZones, 0.4);
            uint64_t newZone;
            do { newZone = zipfZoneSampler->sample(gen) - 1; } while (newZone >= znsZones);
            setSlotZone((uint64_t)slot, (int64_t)newZone, 0);
        }

        uint64_t idInZone = znsActiveZoneOffset[slot]++;
        uint64_t zid = (uint64_t)znsActiveZoneIds[slot];
        uint64_t pid = zid * options.znsPagesPerZone + idInZone;

        if (!(zid < znsZones) || !(idInZone < options.znsPagesPerZone) || pid >= options.logicalPages) {
            std::cerr << "NoWA invalid pid=" << pid
                      << " slot=" << slot
                      << " zone=" << znsActiveZoneIds[slot]
                      << " off=" << idInZone
                      << " ppz=" << options.znsPagesPerZone
                      << " lp=" << options.logicalPages << "\n";
            raise(SIGINT);
        }

        return pid;
    }

    // ---------------- LSM init + access ----------------
    void parseAndInitLSMAccessPattern() {
        // LSM engine write pattern:
        // - region 0: ZNS-like data writes with 512KiB zones
        // - region 1: WAL sequential append writes for 1GiB (offset after region 0)
        // - frequency ratio: data:wal = 8:1 (handled in accessLSM)

        const uint64_t zoneBytes = 16ULL * 1024ULL * 1024ULL;              // 512 KiB zones
        const uint64_t walBytes  = 1ULL * 1024ULL * 1024ULL; // 1 MB WAL

        ensure(options.pageSize > 0);

        const uint64_t zonePages = zoneBytes / options.pageSize;
        const uint64_t walPages  = walBytes  / options.pageSize;

        ensure(zonePages > 0);
        ensure(walPages > 0);
        ensure(options.logicalPages > walPages);

        lsmWalPages  = walPages;
        lsmDataPages = options.logicalPages - walPages;
        lsmWalOffset = lsmDataPages;
        lsmWalSeq.store(0);

        // Configure ZNS behavior for the data region only
        options.znsPagesPerZone = zonePages;

        znsZones = lsmDataPages / options.znsPagesPerZone;
        ensure(znsZones > 0);

        // Clamp active zones to what's possible in the data region
        if (options.znsActiveZones >= znsZones) {
            options.znsActiveZones = std::max<uint64_t>(1, znsZones - 1);
        }
        ensure(options.znsActiveZones > 0);

        // Use equal weights across active slots (simple ZNS baseline for data)
        accessZones.clear();
        sumFreq = 0.0;
        for (uint64_t i = 0; i < options.znsActiveZones; ++i) {
            accessZones.emplace_back(1, 1.0, Pattern::Undefined, -1, false);
        }
        initZoneAccessPattern();
        initActiveSlotDistribution();

        znsActiveZoneIds.assign(options.znsActiveZones, -1);
        znsActiveZoneOffset.assign(options.znsActiveZones, 0);
        activeCount = 0;

        curmaxopenzonecnt = options.znsActiveZones;

        znsZoneInvalidationCount.assign(znsZones, 0);

        znsZoneGroups.clear();
        groupEligible.clear();

        groupMeta.clear();
        zoneToGroups.assign(znsZones, {});
        groupImbalanced.clear();
        imbalancedGroups.clear();
        posInImbalanced.clear();

        zipfZoneSampler = std::make_unique<RejectionInversionZipfSampler>(znsZones, 0.4);
    }

    uint64_t accessLSM(std::mt19937_64& gen) {
        // This pattern consists of:
        // - zone-sized (512KiB) ZNS data workload
        // - WAL log append writes (1GiB) at an offset
        // Data:WAL frequency ratio = 8:1

        if (lsmPickWal(gen)) {
            uint64_t walPage = lsmWalSeq++ % lsmWalPages;
            uint64_t pid = lsmWalOffset + walPage;
            ensure(pid < options.logicalPages);
            return pid;
        }

        // Data write: ZNS-style but constrained to data region size
        uint64_t pid = accessZNS(gen);
        ensure(pid < lsmDataPages);
        return pid;
    }

    // ---------------- LSM init + access ----------------
   // ===========================
// 3) Implement parseAndInitLSMNoWAAccessPattern() per your comment
// ===========================

void parseAndInitLSMNoWAAccessPattern() {
    // Comment spec:
    // - superblock size = ZNS_ZONE_SIZE (options.znsPagesPerZone already set from env)
    // - slice (logical - WAL) into superblocks
    // - region0: [0, (sbCnt-2)*sbSize)  (NoWA on 512KiB zones)
    // - region1: [(sbCnt-2)*sbSize, (sbCnt-1)*sbSize)  (one superblock sequential “compaction”)
    // - region2 (WAL): [(sbCnt-1)*sbSize, + WALsize)  (sequential)
    // - Normal: region0 : region2 accessed 8:1
    // - When region0Writes > (sbSize * sbCnt) AND region0Writes % sbSize == 0:
    //     write sequentially to region1 and fill entire 1 superblock.

    const uint64_t walBytes = 16ULL * 1024ULL * 1024ULL * 1024ULL; // 1GiB WAL (as in your earlier LSM comment)
    ensure(options.pageSize > 0);

    // superblockPages from env ZNS_ZONE_SIZE
    ensurem(options.znsPagesPerZone > 0, "LSMNoWA requires ZNS_ZONE_SIZE (superblock size) to be set");
    lsmSuperblockPages = options.znsPagesPerZone;

    // WAL pages
    lsmWalPages = walBytes / options.pageSize;
    ensure(lsmWalPages > 0);
    ensure(options.logicalPages > lsmWalPages);

    // Use remaining space (logical - wal) for superblocks
    const uint64_t nonWalPages = options.logicalPages - lsmWalPages;
    lsmSuperblockCnt = nonWalPages / lsmSuperblockPages;
    ensurem(lsmSuperblockCnt >= 3, "Need at least 3 superblocks for region0/1/2 layout");

    // region layout in pages
    lsmRegion1Offset = (lsmSuperblockCnt - 2) * lsmSuperblockPages;
    lsmRegion1Pages  = lsmSuperblockPages;
    lsmWalOffset2    = (lsmSuperblockCnt - 1) * lsmSuperblockPages; // start of region2
    lsmWalOffset     = lsmWalOffset2;                               // reuse existing member for convenience
    lsmRegion0Pages  = lsmRegion1Offset;

    ensure(lsmRegion0Pages > 0);
    ensure(lsmRegion1Offset + lsmRegion1Pages == lsmWalOffset2);
    ensure(lsmWalOffset2 + lsmWalPages <= options.logicalPages);

    // reset counters
    lsmWalSeq.store(0);
    lsmRegion0Writes.store(0);
    lsmR1Pos.store(0);
    lsmR1Remaining.store(0);

    // ---------------- Region0 uses NoWA on 512KiB ZNS zones ----------------
    const uint64_t zoneBytes = 512ULL * 1024ULL; // fixed “data zones” size
    const uint64_t zonePages = zoneBytes / options.pageSize;
    ensurem(zonePages > 0, "pageSize too large for 512KiB zones");

    // Configure the internal NoWA/ZNS machinery to operate only within region0:
    options.znsPagesPerZone = zonePages;              // zone size for region0 NoWA
    znsZones = lsmRegion0Pages / options.znsPagesPerZone;
    ensurem(znsZones > 0, "region0 too small for 512KiB zones");

    if (options.znsActiveZones >= znsZones) {
        options.znsActiveZones = std::max<uint64_t>(1, znsZones - 1);
    }
    ensure(options.znsActiveZones > 0);

    accessZones.clear();
    sumFreq = 0.0;
    // Keep NoWA-style equal weights
    for (uint64_t i = 0; i < options.znsActiveZones; ++i) {
        accessZones.emplace_back(1, 1.0, Pattern::Undefined, -1, false);
    }
    initZoneAccessPattern();
    initActiveSlotDistribution();

    znsActiveZoneIds.assign(options.znsActiveZones, -1);
    znsActiveZoneOffset.assign(options.znsActiveZones, 0);
    activeCount = 0;

    curmaxopenzonecnt = options.znsActiveZones;

    znsZoneInvalidationCount.assign(znsZones, 0);
    znsZoneGroups.clear();
    groupEligible.clear();

    groupMeta.clear();
    zoneToGroups.assign(znsZones, {});
    groupImbalanced.clear();
    imbalancedGroups.clear();
    posInImbalanced.clear();

    zipfZoneSampler = std::make_unique<RejectionInversionZipfSampler>(znsZones, 0.4);
}

// ===========================
// 4) Implement accessLSMNoWA()
// ===========================

uint64_t accessLSMNoWA(std::mt19937_64& gen) {
    // Priority 1: if we are in the middle of filling region1, keep writing it sequentially
    uint64_t rem = lsmR1Remaining.load(std::memory_order_relaxed);
    if (rem > 0) {
        uint64_t pos = lsmR1Pos.fetch_add(1, std::memory_order_relaxed);
        if (pos >= lsmRegion1Pages) {
            // safety: clamp/reset if something went off
            lsmR1Pos.store(0, std::memory_order_relaxed);
            lsmR1Remaining.store(0, std::memory_order_relaxed);
            // fall through to normal behavior
        } else {
            // consume one “compaction” page
            lsmR1Remaining.fetch_sub(1, std::memory_order_relaxed);
            return lsmRegion1Offset + pos;
        }
    }

    // Normal behavior: region0 (data, NoWA) vs region2 (WAL) with ratio 8:1
    // lsmPickWal is 1/9 true -> WAL
    if (lsmPickWal(gen)) {
        uint64_t walPage = lsmWalSeq++ % lsmWalPages;
        uint64_t pid = lsmWalOffset2 + walPage;
        ensure(pid < options.logicalPages);
        return pid;
    }

    // region0 data write using NoWA mechanics (guaranteed < lsmRegion0Pages)
    uint64_t pid = accessNoWA(gen);
    ensure(pid < lsmRegion0Pages);

    // update region0 cumulative writes
    uint64_t w = lsmRegion0Writes.fetch_add(1, std::memory_order_relaxed) + 1;

    // Trigger compaction burst into region1:
    // "whenever cumulative writes to region0 exceeds superblock size * superblock cnt,
    //  and when cumulative writes % superblock size == 0, then fill region1 (one superblock)"
    const uint64_t threshold = lsmSuperblockPages * lsmSuperblockCnt;
    if (w > threshold && (w % lsmSuperblockPages) == 0) {
        lsmR1Pos.store(0, std::memory_order_relaxed);
        lsmR1Remaining.store(lsmRegion1Pages, std::memory_order_relaxed);
    }

    return pid;
}


    // ---------------- Misc ----------------
    static double beta_distribution(std::mt19937_64& gen, double alpha, double beta) {
        std::gamma_distribution<> X(alpha, 1.0);
        std::gamma_distribution<> Y(beta, 1.0);
        double x = X(gen);
        double y = Y(gen);
        return x / (x + y);
    }

    void generateFioZipfTraces(double alpha, uint64_t n, uint64_t accesses) {
        const std::string filename = "fiotrace";
        if (fs::exists(filename)) fs::remove(filename);

        std::ofstream outfile(filename, std::ios::app);
        std::string command =
            "../shared/genzipf " + std::to_string(alpha) + " " +
            std::to_string(n) + " " + std::to_string(accesses) +
            " >> " + filename;

        int rc = std::system(command.c_str());
        if (rc == -1) {
            std::cerr << "system() failed: " << std::strerror(errno) << "\n";
            return;
        }
        if (rc != 0) {
            std::cerr << "genzipf returned non-zero status: " << rc << "\n";
        }
        outfile.close();
    }

    const std::string fiotTracefile = "fiotrace";
    uint64_t lastPos = 0;

    bool fetchPagesFromFIOTrace() {
        std::ifstream inFile{fiotTracefile};
        inFile.seekg((std::streampos)lastPos);
        if (!inFile.is_open()) {
            std::cerr << "Error: Unable to open file " << fiotTracefile << " for reading input traces.\n";
            return false;
        }

        inputTraces.clear();
        inputTraces.reserve(chunkSize);

        uint64_t trace;
        size_t count = 0;
        while (count < chunkSize && inFile >> trace) {
            inputTraces.push_back(trace);
            count++;
        }
        lastPos = (uint64_t)inFile.tellg();
        inFile.close();
        return !inputTraces.empty();
    }

    uint64_t getPageFromFIOTrace() {
        if (traceIndex % chunkSize == 0) {
            if (!fetchPagesFromFIOTrace()) {
                throw std::runtime_error("Error: Unable to fetch more pages from the parsed trace file.");
            }
        }
        if (traceIndex >= options.totalWrites) return 0;
        return inputTraces[traceIndex++ % chunkSize];
    }
};

} // namespace iob
