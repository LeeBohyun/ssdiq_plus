//
// Created by Gabriel Haas on 02.07.24.
//
#pragma once

#include "SSD.hpp"
#include "TwoAFormula.hpp"

#include <vector>

class OptimalGC {
   SSD& ssd;
   const long histSize;
   std::vector<uint64_t> writesPerPage;
   long logicalWrites = 0;
   std::string kind;
public:
   OptimalGC(SSD& ssd, std::string kind, int histSize) : ssd(ssd), kind(kind), histSize(histSize) {
      writesPerPage.resize(ssd.logicalPages, 0);
   }
   string name() {
      return kind;
   }
   void writePage(uint64_t pageId) {
      logicalWrites++;
      writesPerPage[pageId]++;
   }
   void stats() {
      std::vector<uint64_t> pages(writesPerPage);
      std::sort(pages.begin(), pages.end(), std::greater<>());
      std::vector<uint64_t> hist(histSize, 0);
      for (long i = 0; i < pages.size(); i++) {
         hist[(float)i/ssd.logicalPages*histSize] += pages[i];
      }
      for (long i = 0; i < hist.size(); i++) {
         if (hist[i] == 0) {
            hist[i] = 1;
         }
      }

      float wa = newOptWA(ssd.ssdFill, hist).second;
      ssd.hackForOptimalWASetPhysWrites(logicalWrites*wa);
      std::cout << "optWA: " << wa << std::endl;
      logicalWrites = 0;
   }
   void resetStats() {
      logicalWrites = 0;
   }
};
