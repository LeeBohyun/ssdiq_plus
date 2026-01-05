//
// Created by Gabriel Haas on 18.12.24.
//
#pragma once

#include <vector>
#include <cmath>
#include <cassert>
#include <numeric>
#include <cstdint>

// Approximate functions
inline double greedyApproxFree(double fillLevel) {
    return 2.0 * (1.0 - fillLevel);
}

inline double greedyApproxWA(double fillLevel) {
    return 1.0 / greedyApproxFree(fillLevel);
}

// When w = 0.5, the original formula is 0/0. Using L'Hopital's rule, limit = 0.5.
inline double calc2aSolution(double w) {
    if (std::fabs(2.0 * w - 1.0) < 1e-15) {
        return 0.5;
    }

    double val = w * (1.0 - w);
    if (val < 0.0) return 0.0;

    double numerator = w - std::sqrt(val);
    double denominator = (-1.0 + 2.0 * w);
    if (std::fabs(denominator) < 1e-15) return 0.0;

    return numerator / denominator;
}

// Compute the "a" value from a vector
inline double compute_a(const std::vector<double>& x_vec) {
    double num = 1.0;
    for (auto v : x_vec) num *= v;

    double denom = 1.0;
    for (size_t i = 0; i < x_vec.size(); ++i) {
        double p = 1.0;
        for (size_t j = i; j < x_vec.size(); ++j) p *= x_vec[j];
        denom += p;
    }
    return num / denom;
}

// Compute weighted average for the given intervals
inline double intervalWA(double fillLevel, const std::vector<double>& s, 
                         const std::vector<double>& wf, const std::vector<double>& opInterval) {
    double sumWA = 0.0;
    for (size_t i = 0; i < s.size(); ++i) {
        double sf = s[i] * fillLevel;
        double denom = sf + (1 - fillLevel) * opInterval[i];
        if (std::fabs(denom) < 1e-15) {
            continue;
        }
        double sFillLevel = sf / denom;
        sumWA += wf[i] * greedyApproxWA(sFillLevel);
    }
    return sumWA;
}

// Library function: newOptWA
// Returns a vector containing 'firsta' values followed by the final 'wa' at the end.
inline std::pair<std::vector<double>, double> newOptWA(double fillLevel, const std::vector<double>& wf_rel) {
    // Normalize wf
    double sum_wf_rel = std::accumulate(wf_rel.begin(), wf_rel.end(), 0.0);
    std::vector<double> wf;
    wf.reserve(wf_rel.size());
    for (auto v : wf_rel) {
        assert(v > 0.0);
        wf.push_back(v / sum_wf_rel);
    }

    // Compute rel = wf[i]/(wf[i] + wf[i+1])
    std::vector<double> rel;
    rel.reserve(wf.size() > 1 ? wf.size() - 1 : 0);
    for (size_t i = 0; i + 1 < wf.size(); ++i) {
        double val = wf[i] / (wf[i] + wf[i+1]);
        rel.push_back(val);
    }

    // Compute splits
    std::vector<double> splits;
    splits.reserve(rel.size());
    for (auto r : rel) {
        splits.push_back(calc2aSolution(r));
    }

    // factors = splits/(1 - splits), skip invalid ones
    std::vector<double> factors;
    for (auto sp : splits) {
        double denom = 1.0 - sp;
        if (std::fabs(denom) > 1e-15) {
            factors.push_back(sp / denom);
        }
    }

    // firsta
    std::vector<double> firsta;
    if (!factors.empty()) {
        double init = compute_a(factors);
        firsta.push_back(init);
        for (size_t i = 0; i < factors.size(); ++i) {
            if (std::fabs(factors[i]) < 1e-15) {
                firsta.push_back(0.0);
            } else {
                firsta.push_back(firsta[i] / factors[i]);
            }
        }
    } else {
        firsta.push_back(1.0);
    }

    std::vector<double> s(wf.size(), 1.0 / wf.size());
    double wa = intervalWA(fillLevel, s, wf, firsta);
    return {firsta, wa};
}
inline std::pair<std::vector<double>, double> newOptWA(double fillLevel, const std::vector<uint64_t>& wf_rel) {
    std::vector<double> wf_rel_double(wf_rel.begin(), wf_rel.end());
    return newOptWA(fillLevel, wf_rel_double);
}
