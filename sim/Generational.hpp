//
// Created by Gabriel Haas on 02.07.24.
//
#pragma once

#include "SSD.hpp"

#include <cstdint>
#include <fstream>
#include <csignal>
#include <list>
#include <random>
#include <algorithm>
#include <vector>

class GenerationalGC {
   SSD& ssd;
   int64_t currentWriteHead = -1;
   std::random_device rd;
   std::mt19937_64 gen{rd()};
   std::list<uint64_t> freeBlocks;
   std::map<int64_t, int64_t> gcWriteHeads;
public:
   GenerationalGC(SSD& ssd) : ssd(ssd) {
      for (uint64_t z=0; z < ssd.zones; z++) {
         freeBlocks.push_back(z);
      }
      currentWriteHead = freeBlocks.front();
      freeBlocks.pop_front();
   }
   string name() {
      return "gen";
   }
   void writePage(uint64_t pageId) {
      // select write block based on TT
      if (!ssd.blocks()[currentWriteHead].canWrite()) {
         ensure(ssd.blocks()[currentWriteHead].writePos() == ssd.pagesPerZone); 
         if (freeBlocks.empty()) {
            performGC();
         }
         currentWriteHead = freeBlocks.front();
         freeBlocks.pop_front();
         //gtstd::cout << "freeBlock gen:" <<  ssd.blocks()[currentWriteHead].gcGeneration << " wp: " << ssd.blocks()[currentWriteHead].writePos() << std::endl;
         ensure(ssd.blocks()[currentWriteHead].canWrite());
      }
      ssd.writePage(pageId, currentWriteHead);
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
   int64_t singleGreedyGeneration(int generation) {
      int64_t minIdx = -1;
      uint64_t minCnt = std::numeric_limits<uint64_t>::max();
      for (uint64_t i = 0; i < ssd.zones; i++) {
         const SSD::Block& block = ssd.blocks()[i];
         if (block.gcGeneration == generation
            //&& block.writtenByGc == writtenByGC
            && block.validCnt() < minCnt 
            && block.validCnt() < block.pagesPerZone 
            && block.fullyWritten()) { // only use full blocks for gc
            minIdx = i;
            minCnt = block.validCnt();
         }
      }
      return minIdx;
   }
   int64_t recursiveGreedyGeneration(const int startGeneration) {
      int generation = startGeneration;
      int64_t minIdx = -1;
      bool descending = true;
      int limit = 1000;
      while (minIdx == -1 && limit-- > 0) {
         minIdx = singleGreedyGeneration(generation);
         if (descending) {
            generation--;
            if (generation < 0) {
               generation = 0;
               descending = false;
            }
         } else {
            generation++;
         }
      }
      if (limit <= 0) {
         ssd.printBlocksStats();
         raise(SIGINT);
      }
      //std::cout << " found gen: " << ssd.blocks()[minIdx].gcGeneration << " ";
      return minIdx;
   }
   void performGC() {
      int64_t greedyBlock = singleGreedy();
      int64_t gcBlockGeneration = ssd.blocks()[greedyBlock].gcGeneration; 
      if (gcBlockGeneration != -1 && gcWriteHeads[gcBlockGeneration] != -1) {
         greedyBlock = gcWriteHeads[gcBlockGeneration];
      }
      //std::cout << "Perform GC: gcGen: " << gcBlockGeneration << std::endl;
      auto [freeBlock, gcBlock] = ssd.compactUntilFreeBlock(greedyBlock, [&]() { return recursiveGreedyGeneration(gcBlockGeneration); });
      assert(ssd.blocks()[freeBlock].isErased());
      gcWriteHeads[gcBlockGeneration] = gcBlock;
      freeBlocks.push_back(freeBlock);
   }
   void stats() {
      std::cout << "GenerationalGC stats: " << std::endl;
   }
   void resetStats() {}
};
