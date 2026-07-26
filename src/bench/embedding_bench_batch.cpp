#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>

#include "config.hpp"
#include "embedding_provider.hpp"

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--rounds N] [--batch-size N]\n";
        return 1;
    }

    int rounds = 20;
    int batch_size = 10;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--rounds" && i + 1 < argc) rounds = std::stoi(argv[++i]);
        if (arg == "--batch-size" && i + 1 < argc) batch_size = std::stoi(argv[++i]);
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }

    auto provider = pgcdc::make_embedding_provider(cfg.embedding);
    provider->init();

    std::string sample_text = "This is a representative sample sentence used to benchmark embedding latency.";
    std::vector<std::string> batch_texts(static_cast<size_t>(batch_size), sample_text);

    // --- Baseline: N sequential embed() calls, N round-trips ---
    std::vector<double> sequential_totals_ms;
    for (int r = 0; r < rounds; ++r) {
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < batch_size; ++i) {
            auto vec = provider->embed(sample_text);
            if (vec.empty()) std::cerr << "sequential call failed in round " << r << "\n";
        }
        auto end = std::chrono::steady_clock::now();
        sequential_totals_ms.push_back(std::chrono::duration<double, std::milli>(end - start).count());
    }

    // --- Batched: one embed_batch() call, 1 round-trip for N texts ---
    std::vector<double> batched_totals_ms;
    for (int r = 0; r < rounds; ++r) {
        auto start = std::chrono::steady_clock::now();
        auto vecs = provider->embed_batch(batch_texts);
        auto end = std::chrono::steady_clock::now();
        bool ok = (vecs.size() == batch_texts.size()) &&
                  std::all_of(vecs.begin(), vecs.end(), [](auto& v) { return !v.empty(); });
        if (!ok) std::cerr << "batched call failed or incomplete in round " << r << "\n";
        batched_totals_ms.push_back(std::chrono::duration<double, std::milli>(end - start).count());
    }

    auto summarize = [](std::vector<double> v, const char* label, int n) {
        std::sort(v.begin(), v.end());
        double sum = std::accumulate(v.begin(), v.end(), 0.0);
        double avg = sum / v.size();
        std::cout << "\n" << label << " (" << n << " rows/round, " << v.size() << " rounds)\n";
        std::cout << "  min total/round:  " << v.front() << " ms\n";
        std::cout << "  avg total/round:  " << avg << " ms\n";
        std::cout << "  max total/round:  " << v.back() << " ms\n";
        std::cout << "  avg per-row:      " << (avg / n) << " ms\n";
    };

    summarize(sequential_totals_ms, "Sequential (N x embed())", batch_size);
    summarize(batched_totals_ms,    "Batched (1 x embed_batch())", batch_size);

    return 0;
}
