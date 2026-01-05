//
// Created by Gabriel Haas on 02.07.24.
//
#pragma once

#include "SSD.hpp"
#include "TwoAFormula.hpp"

#include <cstdint>
#include <fstream>
#include <csignal>
#include <iostream>
#include <list>
#include <numeric>
#include <random>
#include <algorithm>
#include <vector>

class TwoAGC {
   SSD& ssd;
   const int maxWriteHeads;
   std::vector<uint64_t> writeHeads;
   std::vector<uint64_t> gcWritesHeads;
   std::vector<uint64_t> writeHeadWriteCounter;

   std::vector<uint64_t> statsWriteHeadWrites;
   std::vector<uint64_t> statsWriteHeadWritesTotal;
   std::vector<uint64_t> statsWriteHeadGcCounter;
   std::vector<uint64_t> statsWriteHeadGcCompactionCounter;
   long statsSmartGC = 0;
   long statsGreedyGC = 0;
   std::random_device rd;
   std::mt19937_64 gen{rd()};
   std::list<uint64_t> freeBlocks;
   struct TTElement {
      static constexpr long maxTimestamps = 4;
      std::list<long> timestamps;
      float averageTimestampInterval(long now) const {
         long prev = now;
         long sum = 0;
         assert(timestamps.size() > 0);
         for (auto& t: timestamps) {
               sum += prev - t;
               prev = t;
         }
         auto temp = (float)sum / timestamps.size(); 
         assert(temp >= 0);
         return temp;
      }
      void addTimestamp(long now) {
         timestamps.push_front(now);
         if (timestamps.size() == maxTimestamps) {
            timestamps.pop_back();
         }
      }
   };
   std::vector<TTElement> tt;
   long currentTime = 0;
   bool justTTno2a = false;
public:
   TwoAGC(SSD& ssd, int maxWriteHeads, bool justTTno2a) : ssd(ssd), maxWriteHeads(maxWriteHeads), justTTno2a(justTTno2a) {
      assert((ssd.physicalPages - ssd.logicalPages)/ssd.pagesPerZone > maxWriteHeads);
      for (uint64_t z=0; z < ssd.zones; z++) {
         freeBlocks.push_back(z);
      }
      for (int i=0; i < maxWriteHeads; i++) {
         writeHeads.push_back(freeBlocks.front());
         freeBlocks.pop_front();
      }
      for (int i=0; i < maxWriteHeads; i++) {
         gcWritesHeads.push_back(freeBlocks.front());
         freeBlocks.pop_front();
      }
      writeHeadWriteCounter.resize(maxWriteHeads, 0);
      statsWriteHeadWrites.resize(maxWriteHeads, 0);
      statsWriteHeadWritesTotal.resize(maxWriteHeads, 0);
      statsWriteHeadGcCounter.resize(maxWriteHeads, 0);
      statsWriteHeadGcCompactionCounter.resize(maxWriteHeads, 0);
      tt.resize(ssd.logicalPages);
   }
   string name() {
      if (justTTno2a) {
         return "tt-" + std::to_string(maxWriteHeads);
      } else {
         return "2a-" + std::to_string(maxWriteHeads);
      }
   }
   long lastUpdatePercentilesTS = 0;
   std::vector<float> percentiles;
   int64_t chooseWriteHead(uint64_t pageId) {
      int selectedWriteHeadId = maxWriteHeads / 2;
      TTElement& ttElement = tt.at(pageId);
      if (ttElement.timestamps.size() > 0) {
         float currentInterval = tt.at(pageId).averageTimestampInterval(currentTime);
         // Update write group decision informationu  
         if (lastUpdatePercentilesTS + 10*ssd.pagesPerZone < currentTime) {
            lastUpdatePercentilesTS = currentTime;
            percentiles.clear();
            const long sampleSize = 10000;
            std::uniform_int_distribution<size_t> dist(0, tt.size() - 1);
            std::vector<uint64_t> allTTs;
            allTTs.reserve(sampleSize);
            for (uint64_t i = 0; i < sampleSize; i++) {
               long idx = dist(gen);
               if (tt[idx].timestamps.size() > 0) {
                  allTTs.emplace_back(tt[idx].averageTimestampInterval(currentTime));
               }
            }
            //std::sort(allTTs.begin(), allTTs.end());
            uint64_t prevPos = 0;
            for (int i = 0; i < maxWriteHeads - 1; i++) {
               float pos = ((float)(i+1) / maxWriteHeads) * allTTs.size();
               std::nth_element(allTTs.begin() + prevPos, allTTs.begin() + pos, allTTs.end());
               prevPos = pos;
               percentiles.push_back(allTTs.at(pos));
            }
        }
         selectedWriteHeadId = 0;
         // find writeHead according to current interval and percentiles
         while (selectedWriteHeadId < percentiles.size() && currentInterval > percentiles.at(selectedWriteHeadId)) {
            selectedWriteHeadId++;
         }
         //std::cout << "currentInterval: " << currentInterval <<  " selectedWriteHeadId: " << selectedWriteHeadId << std::endl;
      }
      ensure(selectedWriteHeadId >= 0 && selectedWriteHeadId < maxWriteHeads);
      return selectedWriteHeadId;
   }
   void writePage(uint64_t pageId) {
      // select write block based on TT
      int selectedWriteHeadId = chooseWriteHead(pageId);
      uint64_t selectedBlock = writeHeads[selectedWriteHeadId];
      tt.at(pageId).addTimestamp(currentTime++);
      if (!ssd.blocks()[selectedBlock].canWrite()) {
         ensure(ssd.blocks()[selectedBlock].writePos() == ssd.pagesPerZone); 
         if (freeBlocks.empty()) {
            performGC();
         }
         selectedBlock = freeBlocks.front();
         writeHeads[selectedWriteHeadId] = selectedBlock;
         freeBlocks.pop_front();
         ensure(ssd.blocks()[selectedBlock].canWrite());
      }
      ssd.writePage(pageId, selectedBlock, selectedWriteHeadId);
      statsWriteHeadWrites[selectedWriteHeadId]++;
      statsWriteHeadWritesTotal[selectedWriteHeadId]++;
      writeHeadWriteCounter[selectedWriteHeadId]++;
      //std::cout << "write: " << pageId << " to " << selectedBlock << " selectedWriteHeadId: " << selectedWriteHeadId << std::endl;
      /*
      for (int i = 0; i < maxWriteHeads; i++) {
         std::cout << writeHeadWrites[i] << " ";
      }
      std::cout << std::endl;
      */
   }
   uint64_t singleGreedy() {
      uint64_t minIdx;
      uint64_t minCnt = std::numeric_limits<uint64_t>::max();
      for (uint64_t i = 0; i < ssd.zones; i++) {
         const SSD::Block& block = ssd.blocks()[i];
         if (block.validCnt() < minCnt && block.fullyWritten()) { // only use full blocks for gc
            minIdx = i;
            minCnt = block.validCnt();
         }
      }
      if (ssd.blocks()[minIdx].allValid()) {
         std::cout << "minIdx: " << minIdx << " minCnt: " << minCnt << std::endl;
         ssd.printBlocksStats();
         raise(SIGINT);
      }
      return minIdx;
   }
   uint64_t singleGreedyGroup(int group) {
      uint64_t minIdx;
      uint64_t minCnt = std::numeric_limits<uint64_t>::max();
      for (uint64_t i = 0; i < ssd.zones; i++) {
         const SSD::Block& block = ssd.blocks()[i];
         if ((block.group == group || block.group == -1)
            //&& block.writtenByGc == writtenByGC
            && block.validCnt() < minCnt && block.fullyWritten()) { // only use full blocks for gc
            minIdx = i;
            minCnt = block.validCnt();
         }
      }
      if (ssd.blocks()[minIdx].allValid()) {
         std::cout << "ERROR in greedy! group: " << group << " minIdx: " << minIdx << " minCnt: " << minCnt << std::endl;
         ssd.printBlocksStats();
         raise(SIGINT);
      }
      statsWriteHeadGcCompactionCounter[group]++;
      return minIdx;
   }
   int lastUpdateGC = 0;
   std::map<long, double> groupFillLevels;
   std::vector<double> groupFillsShouldBe;
   std::vector<double> opShare;
   bool justDoGreedy = true;
   void performGC() {
      if (lastUpdateGC + 10*ssd.pagesPerZone < currentTime) {
         lastUpdateGC = currentTime;
         // choose group to GC based on two a formula
         // pick the group that is farthest away from its optimal fill level, i.e. has lower fill level than it should have
         /*
         std::cout << "writeHeadWriteCounter: ";
         for (auto& c: writeHeadWriteCounter) {
            std::cout << c << " ";
         }
         std::cout << std::endl;
         */
         std::vector<double> writeFrequenciesDouble(writeHeadWriteCounter.begin(), writeHeadWriteCounter.end());
         std::transform(writeFrequenciesDouble.begin(), writeFrequenciesDouble.end(), writeFrequenciesDouble.begin(), 
            [](double c) { return c > 0 ? c : 0.01; });
         auto [opShare, expectedWA] = newOptWA(ssd.ssdFill, writeFrequenciesDouble);
         std::fill(writeHeadWriteCounter.begin(), writeHeadWriteCounter.end(), 0);
         groupFillsShouldBe.clear();// = std::vector<double>(opShare.begin(), opShare.end());
         groupFillsShouldBe.resize(maxWriteHeads);// = std::vector<double>(opShare.begin(), opShare.end());
         std::transform(opShare.begin(), opShare.end(), groupFillsShouldBe.begin(),
            [this](double opShareValue) { 
               // ssd.ssdFill / (opShare*alpha)
               // one zone gets 0.2 of full alpha, alpha = 0.1, 40 wh
               // => data = (1/40 * fillLevel)
               //   total = (1/40 * fillLevel) + (0.2*0.1)
               // zoneFill = data / total
               const double data  = ssd.ssdFill / maxWriteHeads;
               const double total = data + opShareValue*(1-ssd.ssdFill);
               const double zoneFill = data / total;
               return zoneFill;
            });
         //std::transform(opShare.begin(), opShare.end(), groupFillsShouldBe.begin(), 
         //   [this](double opShareValue) { return 1 - (ssd.ssdFill * opShareValue); });
         /*uuuuu
         std::cout << "opShare: ";
         for (auto& s: opShare) {
            std::cout << s << " ";
         }
         std::cout << std::endl;
         ///*
         std::cout << "gropuFillsShouldBe updated: ";
         for (auto& f: groupFillsShouldBe) {
            std::cout << f << " ";
         }
         std::cout << std::endl;
         //*/
         justDoGreedy = expectedWA*1.01 >= greedyApproxWA(ssd.ssdFill);
         //std::cout << "expectedWA: " << expectedWA << " greedyApproxWA: " << greedyApproxWA(ssd.ssdFill) << " justDoGreedy: " << justDoGreedy << std::endl;
      }
      int gcGroup = -1;
      // apply 2a
      if (!justTTno2a && !justDoGreedy && groupFillsShouldBe.size() >= maxWriteHeads) {
         std::unordered_map<long, long> blocksPerGroup;
         std::unordered_map<long, long> validCountPerGroup;
         for (uint64_t i = 0; i < ssd.zones; i++) {
            blocksPerGroup[ssd.blocks()[i].group]++;
            validCountPerGroup[ssd.blocks()[i].group] += ssd.blocks()[i].validCnt();
         }
         std::unordered_map<long, double> groupRelativeSize;
         long sumBlocks = std::accumulate(blocksPerGroup.begin(), blocksPerGroup.end(), 0, [](long sum, auto& p) { return sum + p.second; }); // TODO: should prorably be ssd.zones
         groupFillLevels.clear();
         for (auto& [group, blocks] : blocksPerGroup) {
            groupFillLevels[group] = (float)validCountPerGroup[group] / (blocks * ssd.pagesPerZone);
            groupRelativeSize[group] = (float)blocks / sumBlocks;
         }
         /*
         std::cout << "blocksPerGroup: ";
         for (auto& [group, blocks] : blocksPerGroup) {
            std::cout << group << " : " << blocks << " ";
         }
         std::cout << std::endl;
         std::cout << "groupRelativeSize: ";
         for (auto& [group, size] : groupRelativeSize) {
            std::cout << group << " : " << size << " ";
         }
         std::cout << std::endl;
         std::cout << "groupFillLevel: ";
         for (auto& [group, fill] : groupFillLevels) {
            std::cout << group << " : " << fill << " ";
         }
         std::cout << std::endl;
         */
         // find the one that is most higher than groupFillShouldBe
         long groupMaxDiff = -1;
         double maxDiff = 0;
         //std::cout << "diffs: ";
         for (auto& [group, fill] : groupFillLevels) {
            // ignore -1 group
            if (group >= 0) {
               double diff = groupFillsShouldBe[group] - fill; // e.g.: should: 0.9, fill: 0.8 diff: 0.1 -> gc it
               //std::cout << "" << group << " : " << diff << " ";
               // TODO rethink this
               if (diff > maxDiff && groupRelativeSize[group] > (1.0/maxWriteHeads)/2) { // only gc if the group is large enough (half of expected size)
                  maxDiff = diff;
                  groupMaxDiff = group;
               }
            }
         }
         //std::cout << std::endl;
         gcGroup = groupMaxDiff; // ssd.blocks()[singleGreedy()].group;
         //std::cout << "performGC: group: " << gcGroup << " fill: " << groupFillLevels[gcGroup] << " should: " << (gcGroup != -1 ? groupFillsShouldBe[gcGroup] : 0) << std::endl;
      }
      if (gcGroup == -1) {
         statsGreedyGC++;
         gcGroup = ssd.blocks()[singleGreedy()].group;
      } else {
         statsSmartGC++;
      }
      // 
      auto nextBlockFun = [&](int64_t groupId) { return singleGreedyGroup(groupId); };
      auto gcDestinationFun = [&](int64_t pageId) -> std::tuple<int64_t, int64_t> { 
         int group = chooseWriteHead(pageId);
         int64_t wh = gcWritesHeads.at(group);
         ensure(ssd.blocks().at(wh).writePos() == 0 || ssd.blocks().at(wh).group == group);
         //std::cout << "gcDestFun: " << pageId << " whGroup: " << group << " wh.block: " << wh << std::endl;
         return {wh, group}; };
      auto updateGroupFun = [&](int64_t group, int64_t newBlockId) { 
         ensure(group != -1);
         ensure(gcWritesHeads.at(group) == -1 || !ssd.blocks().at(gcWritesHeads.at(group)).canWrite());
         gcWritesHeads[group] = newBlockId;
         ensure(gcWritesHeads.at(group) != -1 && ssd.blocks().at(gcWritesHeads.at(group)).canWrite());
      };
      //std::cout << "performGC: group: " << group << std::endl;
      if (gcGroup == -1) {
         ssd.printBlocksStats();
         raise(SIGINT);
      }
      auto [freeBlock, gcBlock] = ssd.compactUntilFreeBlock(gcGroup, nextBlockFun, gcDestinationFun, updateGroupFun);
      assert(ssd.blocks()[freeBlock].isErased());
      statsWriteHeadGcCounter[gcGroup]++;
      freeBlocks.push_back(freeBlock);
   }
   void stats() {
      std::cout << "TwoA stats: (per group)" << std::endl;
      std::cout << "percentiles: ";
      for (auto& p: percentiles) {
         std::cout << p << " ";
      }
      std::cout << std::endl;
      // block stats
      std::map<long, long> blocksPerGroup;
      std::map<long, long> validCountPerGroup;
      long totalValid = 0;
      for (uint64_t i = 0; i < ssd.zones; i++) {
         const SSD::Block& b = ssd.blocks()[i];
         blocksPerGroup[b.group]++;
         validCountPerGroup[b.group] += b.validCnt();
         totalValid += b.validCnt();
      }
      double optimalWAFull = newOptWA(ssd.ssdFill, statsWriteHeadWrites).second;
      double currentFill = (double)totalValid / ssd.physicalPages;
      double optimalWACurrent = newOptWA(currentFill, statsWriteHeadWrites).second;
      // groups
      long writeSum = std::accumulate(statsWriteHeadWrites.begin(), statsWriteHeadWrites.end(), 0);
      std::cout << "writes: ";
      for (int i = 0; i < maxWriteHeads; i++) {
         std::cout << statsWriteHeadWrites[i] << " ";
      }
      std::cout << std::endl;
      std::cout << "relativeWrites%: ";
      for (int i = 0; i < maxWriteHeads; i++) {
         std::cout << std::round((float)statsWriteHeadWrites[i] / writeSum*1000)/10 << " ";
         statsWriteHeadWrites[i] = 0;
      }
      std::cout << std::endl;
      std::cout << "GCCounter: ";
      for (int i = 0; i < maxWriteHeads; i++) {
         std::cout << statsWriteHeadGcCounter[i] << " ";
      }
      std::cout << std::endl;
      std::cout << "GcCompactionCounter: ";
      for (int i = 0; i < maxWriteHeads; i++) {
         std::cout << statsWriteHeadGcCompactionCounter[i] << " ";
      }
      std::cout << std::endl;
      std::cout << "WA: ";
      long totalCompactions = 0;
      long totalGCs = 0;
      for (int i = 0; i < maxWriteHeads; i++) {
         std::cout << round(statsWriteHeadGcCompactionCounter[i] / (float)statsWriteHeadGcCounter[i]*10)/10  << " ";
         totalCompactions += statsWriteHeadGcCompactionCounter[i];
         totalGCs += statsWriteHeadGcCounter[i];
      }
      std::cout << " total WA: " << totalCompactions / (float)totalGCs;
      std::cout << std::endl;
      std::cout << "WAanteilig%: ";
      for (int i = 0; i < maxWriteHeads; i++) {
         std::cout << std::round(statsWriteHeadGcCompactionCounter[i] / (float)totalCompactions *1000)/10  << " ";
      }
      std::cout << std::endl;
      for (int i = 0; i < maxWriteHeads; i++) {
         statsWriteHeadGcCounter[i] = 0;
         statsWriteHeadGcCompactionCounter[i] = 0;
      }
      std::cout << "Blocks: ";
      for (auto& [group, blocks] : blocksPerGroup) {
         if (group >= 0) {
            std::cout << blocks << " ";
         }
      }
      int gr = -1;
      if (blocksPerGroup.contains(gr)) {
         std::cout << "(" << gr << ": " << blocksPerGroup[gr] << ")";
      }
      std::cout << std::endl;
      std::cout << "ValidPercent%: ";
      for (auto& [group, validCnt] : validCountPerGroup) {
         if (group >= 0) {
            std::cout << std::round((float)validCnt/(blocksPerGroup[group]*ssd.pagesPerZone)*1000)/10 << " ";
         }
      }
      if (validCountPerGroup.contains(gr)) {
         std::cout << "(" << gr << ": " << std::round((float)validCountPerGroup[gr]/(blocksPerGroup[gr]*ssd.pagesPerZone)*1000)/10 << ")";
      }
      std::cout << std::endl;
      std::cout << "fillsShouldBe%: ";
      for (auto& f: groupFillsShouldBe) {
         std::cout << std::round(f*1000)/10 << " ";
      }
      std::cout << std::endl;
      std::cout << "WAshould: ";
      for (auto& f: groupFillsShouldBe) {
         std::cout << round(greedyApproxWA(f)*10)/10 << " ";
      }
      std::cout << std::endl;
      std::cout << "smartGC: " << statsSmartGC << " greedyGC: " << statsGreedyGC << std::endl;
      double optimalWAtotal = newOptWA(ssd.ssdFill, statsWriteHeadWritesTotal).second;
      long sumWritesTotal = std::accumulate(statsWriteHeadWritesTotal.begin(), statsWriteHeadWritesTotal.end(), 0);
      std::cout << "optimalAtotal: " << optimalWAtotal << " (samples: " << sumWritesTotal << "  == " << round((float)sumWritesTotal/ssd.logicalPages*10)/10 << "x SSD) ";
      std::cout << " optimalWACurrent: " << optimalWACurrent << " optimalWAFull: " << optimalWAFull;
      std::cout << " greedyWACurrent: " << greedyApproxWA(currentFill) << " greedyWAFull: " << greedyApproxWA(ssd.ssdFill) <<std::endl;
      statsSmartGC = 0;
      statsGreedyGC = 0;
      std::cout << std::endl;
   }
   void resetStats() {
      std::fill(statsWriteHeadWrites.begin(), statsWriteHeadWrites.end(), 0);
      std::fill(statsWriteHeadWritesTotal.begin(), statsWriteHeadWritesTotal.end(), 0);
      std::fill(statsWriteHeadGcCounter.begin(), statsWriteHeadGcCounter.end(), 0);
      std::fill(statsWriteHeadGcCompactionCounter.begin(), statsWriteHeadGcCompactionCounter.end(), 0);
   }
};
