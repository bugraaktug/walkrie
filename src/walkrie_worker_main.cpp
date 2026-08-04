#include <cstdlib>
#include <getopt.h>
#include <iostream>
#include <string>

#include <spdlog/spdlog.h>

#include "backfill_worker.hpp"

namespace
{

struct Options
{
    std::string config_path;
    std::string store_path;
    std::string slot_name; // <<< logging only — the store path is what actually scopes the source
    size_t batch_size = 200;
    bool show_help = false;
};

void print_usage(const char* argv0)
{
    std::cerr
        << "usage: " << argv0 << " -c <config.toml> --store <backfill.sqlite3> [--slot <name>] [--batch-size <n>]\n"
        << "\n"
        << "  -c, --config <path>     path to config.toml (required)\n"
        << "      --store <path>      backfill SQLite store file for this source (required)\n"
        << "      --slot <name>       source slot name, for log lines only\n"
        << "      --batch-size <n>    rows claimed per drain iteration (default 200)\n"
        << "  -h, --help               show this message\n";
}

Options parse_args(int argc, char** argv)
{
    Options opts;
    static struct option long_opts[] = {
        {"config",     required_argument, nullptr, 'c'},
        {"store",      required_argument, nullptr, 's'},
        {"slot",       required_argument, nullptr, 'n'},
        {"batch-size", required_argument, nullptr, 'b'},
        {"help",       no_argument,       nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "c:h", long_opts, nullptr)) != -1) {
        switch (c) {
            case 'c': opts.config_path = optarg; break;
            case 's': opts.store_path = optarg; break;
            case 'n': opts.slot_name = optarg; break;
            case 'b': opts.batch_size = static_cast<size_t>(std::stoul(optarg)); break;
            case 'h': opts.show_help = true; break;
            default:
                print_usage(argv[0]);
                std::exit(1);
        }
    }
    return opts;
}

} // namespace

int main(int argc, char** argv)
{
    Options opts = parse_args(argc, argv);
    if (opts.show_help || opts.config_path.empty() || opts.store_path.empty()) {
        print_usage(argv[0]);
        return opts.show_help ? 0 : 1;
    }

    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%t] [%l] %v");
    spdlog::info("[BackfillWorker] starting — slot={} store={}", opts.slot_name, opts.store_path);

    pgcdc::BackfillWorker worker(opts.config_path, opts.store_path, opts.batch_size);
    if (!worker.run()) {
        spdlog::error("[BackfillWorker] failed — {}", worker.last_error());
        std::cerr << "backfill worker failed: " << worker.last_error() << "\n";
        return 1;
    }

    spdlog::info("[BackfillWorker] drain complete — slot={}", opts.slot_name);
    return 0;
}
