
//
// Created by Gabriel Haas on 18.12.24.
//

#include "TwoAFormula.hpp"
#include <iostream>
#include <cstdint>
#include <vector>

int main() {
    double fillLevel = 0.9;
    //std::vector<double> wf_rel = {0.4, 0.4, 0.15, 0.05};
    //std::vector<double> wf_rel = {0.8, 0.09, 0.09, 0.02};
    //std::vector<double> wf_rel = {8, 9, 9, 2};
    //std::vector<double> wf_rel = {19013, 1338, 635, 386, 515, 232, 472, 339, 224, 438};
    // 800 50 12 25 13 7 8 12 18 20 5 8 3 2 0 1 9 4 16 2
    // std::vector<double> wf_rel = {800, 50, 12, 25, 13, 7, 8, 12, 18, 20, 5, 8, 3, 2, 0, 1, 9, 4, 16, 25};
    std::vector<double> wf_rel = { 12013567, 792602, 468254, 336567, 264217, 217343, 187127, 161497, 142560, 134313, 114690, 114690, 98924, 91752, 91748, 91752, 77341, 68811, 68814, 68811, 68814, 68814, 68811, 65181, 45874, 45876,  45876, 45874, 45876, 45874, 45876, 45876, 45874, 45876, 45874, 45876, 45876, 45874, 43077, 22937, 22938, 22938, 22937,  22938, 22937, 22938, 22938, 22937, 22938, 22937, 22938, 22938, 22937, 22938, 22937, 22938, 22938, 22937, 22938, 22937,  22938, 22938, 22937, 22938, 22937, 22938, 21556, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    auto [result, wa] = newOptWA(fillLevel, wf_rel);

    //std::vector<double> s(wf.size(), 1.0 / wf.size());
    //double wa = intervalWA(fillLevel, s, wf, firsta);
    // Append wa at the end of the returned vector
    //firsta.push_back(wa);

    std::cout << "firsta and WA:\n";
    for (auto val : result) std::cout << val << " ";
    std::cout << std::endl;
    return 0;
}
