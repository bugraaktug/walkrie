#pragma once
#include <cstdint>
#include <vector>
#include <algorithm>
#include <numeric>

namespace pgcdc
{

namespace bench 
{

class LagStats
{
public:
    void record(int64_t lag_us) 
    {
        samples_.push_back(lag_us);
    }

    size_t count() const { return samples_.size(); }

    struct Summary 
    {
        int64_t min_us = 0;
        int64_t max_us = 0;
        double  avg_us = 0;
        int64_t p50_us = 0;
        int64_t p95_us = 0;
        int64_t p99_us = 0;
    };

    Summary summarize() const 
    {
        Summary s;
        if (samples_.empty()) return s;

        std::vector<int64_t> sorted = samples_;
        std::sort(sorted.begin(), sorted.end());

        s.min_us = sorted.front();
        s.max_us = sorted.back();
        s.avg_us = static_cast<double>(
            std::accumulate(sorted.begin(), sorted.end(), int64_t{0})) / sorted.size();

        auto pct = [&](double p) -> int64_t {
            size_t idx = static_cast<size_t>(p * (sorted.size() - 1));
            return sorted[idx];
        };
        s.p50_us = pct(0.50);
        s.p95_us = pct(0.95);
        s.p99_us = pct(0.99);
        return s;
    }

private:
    std::vector<int64_t> samples_;
};

inline bool lag_summary_count_warning(const LagStats& stats) 
{
    return stats.count() == 0;
}

}
} // namespace pgcdc::bench
