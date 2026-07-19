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
        std::cerr << "usage: " << argv[0] << " <config.toml> [--calls N]\n";
        return 1;
    }

    int num_calls = 100;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--calls" && i + 1 < argc) {
            num_calls = std::stoi(argv[++i]);
        }
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }

    auto provider = pgcdc::make_embedding_provider(cfg.embedding);

    auto init_start = std::chrono::steady_clock::now();
    provider->init();
    auto init_end = std::chrono::steady_clock::now();
    double init_secs = std::chrono::duration<double>(init_end - init_start).count();

    std::cout << "provider:      " << provider->name() << "\n";
    std::cout << "dimensions:    " << provider->dimensions() << "\n";
    std::cout << "init time:     " << std::fixed << std::setprecision(2) << init_secs << " s "
              << "(model load — one-time cost, not per-row)\n\n";

    // Representative sample text — adjust length to match your real embed
    // column's typical size. Latency scales with input token count, so a
    // 10-word test string will understate real-world per-row cost if your
    // actual data is closer to a paragraph.
    std::string sample_text = "This is a representative sample sentence used to benchmark embedding latency for the walkrie CDC pipeline.";

    std::vector<double> latencies_ms;
    latencies_ms.reserve(static_cast<size_t>(num_calls));

    std::cout << "running " << num_calls << " embed() calls...\n";
    for (int i = 0; i < num_calls; ++i) {
        auto start = std::chrono::steady_clock::now();
        auto vec = provider->embed(sample_text);
        auto end = std::chrono::steady_clock::now();

        if (vec.empty()) {
            std::cerr << "call " << i << " returned an empty vector — check logs\n";
            continue;
        }
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        latencies_ms.push_back(ms);
    }

    if (latencies_ms.empty()) {
        std::cerr << "no successful embed() calls — nothing to report\n";
        return 1;
    }

    std::sort(latencies_ms.begin(), latencies_ms.end());
    double sum = std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0);
    double avg = sum / latencies_ms.size();
    double p50 = latencies_ms[latencies_ms.size() / 2];
    double p95 = latencies_ms[static_cast<size_t>(latencies_ms.size() * 0.95)];
    double min = latencies_ms.front();
    double max = latencies_ms.back();

    std::cout << "\n=== embed() latency (single-threaded, serial calls) ===\n";
    std::cout << "successful calls: " << latencies_ms.size() << " / " << num_calls << "\n";
    std::cout << "min:  " << min << " ms\n";
    std::cout << "avg:  " << avg << " ms\n";
    std::cout << "p50:  " << p50 << " ms\n";
    std::cout << "p95:  " << p95 << " ms\n";
    std::cout << "max:  " << max << " ms\n";
    std::cout << "\nprojected throughput at this latency: "
               << std::fixed << std::setprecision(1) << (1000.0 / avg) << " rows/sec "
               << "(single dispatcher thread, no batching)\n";

    return 0;
}
