#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../span.hh"

#include "../bmux/blockmux.hh"
#include "tests/utils.hh"

using bmux::BlockingMux;
using bmux::BlockLocation;
using bmux::FileSystemTier;
using bmux::IoBuffer;
using bmux::MetadataStore;
using bmux::MutablePlacementPolicy;
using bmux::TierId;
using bmux::TierRegistry;
using bmux::kBlockSize;

namespace {

static_assert(
#ifdef __linux__
    true,
#else
    false,
#endif
    "perf_blocking.cc requires Linux");

std::string normalize_test_path(const std::string& path) {
    fs::path p(path);
    p = p.lexically_normal();
    if (p.is_absolute()) {
        p = p.lexically_relative("/");
    }
    return p.string();
}

std::vector<BlockLocation> sorted_locs_blocking(MetadataStore& metadata,
                                                const std::string& path,
                                                std::uint64_t offset,
                                                std::uint64_t size) {
    auto locs = metadata.lookup(normalize_test_path(path), offset, size);
    std::sort(locs.begin(), locs.end(),
              [](const BlockLocation& a, const BlockLocation& b) {
                  return a.file_offset < b.file_offset;
              });
    return locs;
}

void ensure_tier_ready(const fs::path& root, const std::string& name) {
    ensure_dir_exists(root);

    if (!is_mountpoint(root)) {
        throw std::runtime_error(name + " tier is not mounted: " + root.string());
    }

    auto throw_not_writable = [&](int err, const std::string& context) {
        struct stat st {};
        std::string perms = "unknown";
        std::string owner = "unknown";
        std::string group = "unknown";

        if (::stat(root.c_str(), &st) == 0) {
            char mode_buf[16] = {};
            std::snprintf(mode_buf, sizeof(mode_buf), "%04o", st.st_mode & 07777);
            perms = mode_buf;
            owner = std::to_string(static_cast<long long>(st.st_uid));
            group = std::to_string(static_cast<long long>(st.st_gid));
        }

        throw std::runtime_error(
            context + ": " + root.string() +
            " errno=" + std::to_string(err) +
            " (" + std::string(std::strerror(err)) + ")" +
            " mode=" + perms +
            " uid=" + owner +
            " gid=" + group);
    };

    if (::access(root.c_str(), W_OK) != 0) {
        const int initial_err = errno;

        if (::chmod(root.c_str(), 0777) != 0) {
            throw_not_writable(initial_err, "tier root is not writable and chmod repair failed");
        }

        if (::access(root.c_str(), W_OK) != 0) {
            throw_not_writable(errno, "tier root is not writable after chmod repair");
        }
    }
}

void cleanup_prefix(const std::string& prefix) {
    std::error_code ec;
    fs::remove_all(kHotRoot / prefix, ec);
    fs::remove_all(kWarmRoot / prefix, ec);
    fs::remove_all(kColdRoot / prefix, ec);
}

struct BenchmarkConfig {
    std::size_t file_size_bytes = 64 * 1024 * 1024;
    std::size_t io_size_bytes = 4096;
    std::size_t ops = 50000;
    std::size_t concurrency = 16;
    std::size_t thread_pool_threads = 8;
    std::uint64_t seed = 1;
    bool csv = false;
    bool verify = false;
};

struct BenchmarkResult {
    std::string backend;
    std::string workload;
    std::size_t ops = 0;
    std::size_t total_bytes = 0;
    std::size_t concurrency = 0;
    double seconds = 0.0;
    double ops_per_sec = 0.0;
    double mib_per_sec = 0.0;
    double avg_us = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
    double max_us = 0.0;
};

double percentile_us(std::vector<std::uint64_t> ns, double p) {
    if (ns.empty()) {
        return 0.0;
    }
    std::sort(ns.begin(), ns.end());
    const double idx = p * static_cast<double>(ns.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(std::floor(idx));
    const std::size_t hi = static_cast<std::size_t>(std::ceil(idx));
    const double frac = idx - static_cast<double>(lo);
    const double value =
        (1.0 - frac) * static_cast<double>(ns[lo]) +
        frac * static_cast<double>(ns[hi]);
    return value / 1000.0;
}

BenchmarkResult summarize(const std::string& backend,
                          const std::string& workload,
                          std::size_t concurrency,
                          std::size_t total_bytes,
                          double seconds,
                          const std::vector<std::uint64_t>& latencies_ns) {
    BenchmarkResult r;
    r.backend = backend;
    r.workload = workload;
    r.ops = latencies_ns.size();
    r.total_bytes = total_bytes;
    r.concurrency = concurrency;
    r.seconds = seconds;
    r.ops_per_sec = seconds > 0.0 ? static_cast<double>(r.ops) / seconds : 0.0;
    r.mib_per_sec = seconds > 0.0
        ? (static_cast<double>(total_bytes) / (1024.0 * 1024.0)) / seconds
        : 0.0;

    if (!latencies_ns.empty()) {
        const auto sum_ns = std::accumulate(latencies_ns.begin(),
                                            latencies_ns.end(),
                                            std::uint64_t{0});
        r.avg_us = (static_cast<double>(sum_ns) / static_cast<double>(latencies_ns.size())) / 1000.0;
        r.p50_us = percentile_us(latencies_ns, 0.50);
        r.p95_us = percentile_us(latencies_ns, 0.95);
        r.p99_us = percentile_us(latencies_ns, 0.99);
        r.max_us = static_cast<double>(*std::max_element(latencies_ns.begin(), latencies_ns.end())) / 1000.0;
    }

    return r;
}

void print_result(const BenchmarkResult& r, bool csv) {
    if (csv) {
        std::cout
            << r.backend << ","
            << r.workload << ","
            << r.ops << ","
            << r.total_bytes << ","
            << r.concurrency << ","
            << std::fixed << std::setprecision(6) << r.seconds << ","
            << r.ops_per_sec << ","
            << r.mib_per_sec << ","
            << r.avg_us << ","
            << r.p50_us << ","
            << r.p95_us << ","
            << r.p99_us << ","
            << r.max_us << "\n";
        return;
    }

    std::cout
        << std::left << std::setw(12) << r.backend
        << std::setw(24) << r.workload
        << " ops=" << std::setw(8) << r.ops
        << " conc=" << std::setw(4) << r.concurrency
        << " sec=" << std::setw(9) << std::fixed << std::setprecision(3) << r.seconds
        << " ops/s=" << std::setw(12) << std::fixed << std::setprecision(1) << r.ops_per_sec
        << " MiB/s=" << std::setw(10) << std::fixed << std::setprecision(1) << r.mib_per_sec
        << " avg_us=" << std::setw(10) << std::fixed << std::setprecision(1) << r.avg_us
        << " p50=" << std::setw(9) << r.p50_us
        << " p95=" << std::setw(9) << r.p95_us
        << " p99=" << std::setw(9) << r.p99_us
        << " max=" << std::setw(9) << r.max_us
        << "\n";
    std::cout.unsetf(std::ios::floatfield);
}

struct Fixture {
    TierRegistry tiers;
    MetadataStore metadata;
    MutablePlacementPolicy placement{1};
    BlockingMux mux;
    std::string prefix;

    explicit Fixture(std::size_t pool_threads, std::string run_prefix)
        : mux(tiers, metadata, placement, pool_threads, true, 1)
        , prefix(std::move(run_prefix)) {
        wait_for_mount(kHotRoot, "hot");
        wait_for_mount(kWarmRoot, "warm");
        wait_for_mount(kColdRoot, "cold");

        ensure_tier_ready(kHotRoot, "hot");
        ensure_tier_ready(kWarmRoot, "warm");
        ensure_tier_ready(kColdRoot, "cold");

        const long hot_magic = fs_magic_for(kHotRoot);
        const long warm_magic = fs_magic_for(kWarmRoot);
        const long cold_magic = fs_magic_for(kColdRoot);

        if (hot_magic != kTmpfsMagic) {
            throw std::runtime_error("hot tier must be tmpfs");
        }
        if (!(warm_magic == kExtMagic || warm_magic == kXfsMagic)) {
            throw std::runtime_error("warm tier must be ext4 or xfs");
        }
        if (!(cold_magic == kExtMagic || cold_magic == kXfsMagic)) {
            throw std::runtime_error("cold tier must be ext4 or xfs");
        }

        cleanup_prefix(prefix);

        tiers.add(std::make_unique<FileSystemTier>(1, "hot", kHotRoot));
        tiers.add(std::make_unique<FileSystemTier>(2, "warm", kWarmRoot));
        tiers.add(std::make_unique<FileSystemTier>(3, "cold", kColdRoot));
    }

    ~Fixture() {
        cleanup_prefix(prefix);
    }

    std::string full_path(std::string_view suffix) const {
        return "/" + prefix + "/" + std::string(suffix);
    }
};

struct BlockingMuxAdapter {
    Fixture& fx;

    explicit BlockingMuxAdapter(Fixture& f) : fx(f) {}

    std::string name() const { return "blockingmux"; }

    void set_default_tier(TierId tier) { fx.placement.set(tier); }

    void write(const std::string& path,
               std::uint64_t offset,
               span<const std::byte> bytes) {
        fx.mux.write(path, offset, bytes);
    }

    IoBuffer read(const std::string& path,
                  std::uint64_t offset,
                  std::uint64_t size) {
        return fx.mux.read(path, offset, size);
    }

    void migrate(std::uint64_t block_id, TierId src, TierId dst) {
        fx.mux.migrate(block_id, src, dst);
    }

    void promote(std::uint64_t block_id, TierId dst) {
        fx.mux.promote(block_id, dst);
    }

    std::vector<BlockLocation> lookup(const std::string& path,
                                      std::uint64_t offset,
                                      std::uint64_t size) {
        return sorted_locs_blocking(fx.metadata, path, offset, size);
    }
};

std::vector<std::byte> deterministic_bytes(std::size_t n, std::uint64_t seed) {
    std::vector<std::byte> out(n);
    std::mt19937_64 rng(seed);
    for (std::size_t i = 0; i < n; ++i) {
        out[i] = static_cast<std::byte>(rng() & 0xFF);
    }
    return out;
}

std::uint64_t random_aligned_offset(std::mt19937_64& rng,
                                    std::size_t file_size,
                                    std::size_t io_size) {
    if (file_size <= io_size) {
        return 0;
    }
    const std::size_t max_slot = (file_size - io_size) / io_size;
    const std::size_t slot = static_cast<std::size_t>(rng() % (max_slot + 1));
    return static_cast<std::uint64_t>(slot * io_size);
}

void prepopulate_file(BlockingMuxAdapter& adapter,
                      const std::string& path,
                      std::size_t file_size,
                      std::uint64_t seed,
                      TierId tier = 1) {
    adapter.set_default_tier(tier);
    auto bytes = deterministic_bytes(file_size, seed);
    adapter.write(path, 0, span<const std::byte>(bytes.data(), bytes.size()));
}

template <typename Fn>
BenchmarkResult run_concurrent_workload(
    const std::string& backend_name,
    const std::string& workload_name,
    std::size_t concurrency,
    std::size_t ops,
    std::size_t bytes_per_op_estimate,
    Fn make_one_op) {

    std::atomic<std::size_t> next_index{0};
    std::vector<std::uint64_t> latencies_ns(ops, 0);

    std::mutex ex_mu;
    std::exception_ptr ex;

    auto worker = [&](std::size_t worker_id) {
        try {
            while (true) {
                const std::size_t i = next_index.fetch_add(1, std::memory_order_relaxed);
                if (i >= ops) {
                    break;
                }

                const auto t0 = std::chrono::steady_clock::now();
                make_one_op(worker_id, i);
                const auto t1 = std::chrono::steady_clock::now();

                latencies_ns[i] = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            }
        } catch (...) {
            std::lock_guard<std::mutex> guard(ex_mu);
            if (!ex) {
                ex = std::current_exception();
            }
        }
    };

    const auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    workers.reserve(concurrency);
    for (std::size_t i = 0; i < concurrency; ++i) {
        workers.emplace_back(worker, i);
    }

    for (auto& w : workers) {
        if (w.joinable()) {
            w.join();
        }
    }

    if (ex) {
        std::rethrow_exception(ex);
    }

    const auto end = std::chrono::steady_clock::now();
    const double seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();

    return summarize(backend_name,
                     workload_name,
                     concurrency,
                     ops * bytes_per_op_estimate,
                     seconds,
                     latencies_ns);
}

BenchmarkResult benchmark_sequential_write(BlockingMuxAdapter& adapter,
                                           Fixture& fx,
                                           const BenchmarkConfig& cfg) {
    const std::string path = fx.full_path("seq_write.dat");
    const auto payload = deterministic_bytes(cfg.io_size_bytes, cfg.seed + 100);

    adapter.set_default_tier(1);

    return run_concurrent_workload(
        adapter.name(),
        "sequential_write",
        cfg.concurrency,
        cfg.ops,
        cfg.io_size_bytes,
        [&](std::size_t, std::size_t i) {
            const std::uint64_t off =
                static_cast<std::uint64_t>((i * cfg.io_size_bytes) % cfg.file_size_bytes);
            adapter.write(path,
                          off,
                          span<const std::byte>(payload.data(), payload.size()));
        });
}

BenchmarkResult benchmark_sequential_read(BlockingMuxAdapter& adapter,
                                          Fixture& fx,
                                          const BenchmarkConfig& cfg) {
    const std::string path = fx.full_path("seq_read.dat");
    prepopulate_file(adapter, path, cfg.file_size_bytes, cfg.seed + 200, 1);

    return run_concurrent_workload(
        adapter.name(),
        "sequential_read",
        cfg.concurrency,
        cfg.ops,
        cfg.io_size_bytes,
        [&](std::size_t, std::size_t i) {
            const std::uint64_t off =
                static_cast<std::uint64_t>((i * cfg.io_size_bytes) % cfg.file_size_bytes);
            auto out = adapter.read(path, off, cfg.io_size_bytes);
            if (cfg.verify && out.size() != cfg.io_size_bytes) {
                throw std::runtime_error("short read in sequential_read");
            }
        });
}

BenchmarkResult benchmark_random_read(BlockingMuxAdapter& adapter,
                                      Fixture& fx,
                                      const BenchmarkConfig& cfg) {
    const std::string path = fx.full_path("rand_read.dat");
    prepopulate_file(adapter, path, cfg.file_size_bytes, cfg.seed + 300, 1);

    return run_concurrent_workload(
        adapter.name(),
        "random_read",
        cfg.concurrency,
        cfg.ops,
        cfg.io_size_bytes,
        [&](std::size_t worker_id, std::size_t i) {
            std::mt19937_64 rng(cfg.seed + 1000 + worker_id * 1315423911ULL + i);
            const std::uint64_t off =
                random_aligned_offset(rng, cfg.file_size_bytes, cfg.io_size_bytes);
            auto out = adapter.read(path, off, cfg.io_size_bytes);
            if (cfg.verify && out.size() != cfg.io_size_bytes) {
                throw std::runtime_error("short read in random_read");
            }
        });
}

BenchmarkResult benchmark_random_write(BlockingMuxAdapter& adapter,
                                       Fixture& fx,
                                       const BenchmarkConfig& cfg) {
    const std::string path = fx.full_path("rand_write.dat");
    prepopulate_file(adapter, path, cfg.file_size_bytes, cfg.seed + 400, 1);
    const auto payload = deterministic_bytes(cfg.io_size_bytes, cfg.seed + 401);

    return run_concurrent_workload(
        adapter.name(),
        "random_write",
        cfg.concurrency,
        cfg.ops,
        cfg.io_size_bytes,
        [&](std::size_t worker_id, std::size_t i) {
            std::mt19937_64 rng(cfg.seed + 2000 + worker_id * 11400714819323198485ULL + i);
            const std::uint64_t off =
                random_aligned_offset(rng, cfg.file_size_bytes, cfg.io_size_bytes);

            const TierId tier = (i % 3 == 0) ? 1 : ((i % 3 == 1) ? 2 : 3);
            adapter.set_default_tier(tier);

            adapter.write(path,
                          off,
                          span<const std::byte>(payload.data(), payload.size()));
        });
}

BenchmarkResult benchmark_mixed_rw(BlockingMuxAdapter& adapter,
                                   Fixture& fx,
                                   const BenchmarkConfig& cfg) {
    const std::string path = fx.full_path("mixed_rw.dat");
    prepopulate_file(adapter, path, cfg.file_size_bytes, cfg.seed + 500, 1);
    const auto payload = deterministic_bytes(cfg.io_size_bytes, cfg.seed + 501);

    return run_concurrent_workload(
        adapter.name(),
        "mixed_80r_20w",
        cfg.concurrency,
        cfg.ops,
        cfg.io_size_bytes,
        [&](std::size_t worker_id, std::size_t i) {
            std::mt19937_64 rng(cfg.seed + 3000 + worker_id * 6364136223846793005ULL + i);
            const std::uint64_t off =
                random_aligned_offset(rng, cfg.file_size_bytes, cfg.io_size_bytes);

            if ((i % 5) == 0) {
                adapter.set_default_tier((i % 2) ? 2 : 1);
                adapter.write(path,
                              off,
                              span<const std::byte>(payload.data(), payload.size()));
            } else {
                auto out = adapter.read(path, off, cfg.io_size_bytes);
                if (cfg.verify && out.size() != cfg.io_size_bytes) {
                    throw std::runtime_error("short read in mixed workload");
                }
            }
        });
}

BenchmarkResult benchmark_fanout_read(BlockingMuxAdapter& adapter,
                                      Fixture& fx,
                                      const BenchmarkConfig& cfg) {
    const std::string path = fx.full_path("fanout_read.dat");
    const std::size_t size = std::max<std::size_t>(cfg.file_size_bytes, 8 * kBlockSize);
    prepopulate_file(adapter, path, size, cfg.seed + 600, 1);

    auto locs = adapter.lookup(path, 0, size);
    if (locs.size() >= 3) {
        for (std::size_t i = 0; i < locs.size(); ++i) {
            const TierId src = locs[i].tier_id;
            const TierId dst = (i % 3 == 0) ? 1 : ((i % 3 == 1) ? 2 : 3);
            if (src != dst) {
                adapter.migrate(locs[i].block_id, src, dst);
            }
        }
    }

    const std::size_t read_size = std::min<std::size_t>(cfg.io_size_bytes, 16 * 1024);

    return run_concurrent_workload(
        adapter.name(),
        "fanout_read_multitier",
        cfg.concurrency,
        cfg.ops,
        read_size,
        [&](std::size_t worker_id, std::size_t i) {
            std::mt19937_64 rng(cfg.seed + 4000 + worker_id * 911382323ULL + i);
            const std::uint64_t off =
                random_aligned_offset(rng, size, read_size);
            auto out = adapter.read(path, off, read_size);
            if (cfg.verify && out.size() != read_size) {
                throw std::runtime_error("short read in fanout_read_multitier");
            }
        });
}

BenchmarkResult benchmark_background_migration(BlockingMuxAdapter& adapter,
                                               Fixture& fx,
                                               const BenchmarkConfig& cfg) {
    const std::string path = fx.full_path("migration_bg.dat");
    const std::size_t size = std::max<std::size_t>(cfg.file_size_bytes, 16 * kBlockSize);
    prepopulate_file(adapter, path, size, cfg.seed + 700, 1);

    auto locs = adapter.lookup(path, 0, size);
    if (locs.empty()) {
        throw std::runtime_error("background migration benchmark requires populated metadata");
    }

    for (std::size_t i = 0; i < locs.size(); ++i) {
        const TierId dst = (i % 2 == 0) ? 2 : 3;
        adapter.migrate(locs[i].block_id, 1, dst);
    }

    return run_concurrent_workload(
        adapter.name(),
        "foreground_rw_with_bg_promotion",
        cfg.concurrency,
        cfg.ops,
        cfg.io_size_bytes,
        [&](std::size_t worker_id, std::size_t i) {
            std::mt19937_64 rng(cfg.seed + 5000 + worker_id * 1442695040888963407ULL + i);
            const std::uint64_t off =
                random_aligned_offset(rng, size, cfg.io_size_bytes);

            if ((i % 10) < 8) {
                auto out = adapter.read(path, off, cfg.io_size_bytes);
                if (cfg.verify && out.size() != cfg.io_size_bytes) {
                    throw std::runtime_error("short read in bg promotion workload");
                }
            } else {
                auto payload = deterministic_bytes(cfg.io_size_bytes, cfg.seed + 9000 + i);
                adapter.set_default_tier((i % 2) ? 1 : 2);
                adapter.write(path,
                              off,
                              span<const std::byte>(payload.data(), payload.size()));
            }
        });
}

void run_suite(BlockingMuxAdapter& adapter, Fixture& fx, const BenchmarkConfig& cfg) {
    if (cfg.csv) {
        std::cout
            << "backend,workload,ops,total_bytes,concurrency,seconds,ops_per_sec,"
            << "mib_per_sec,avg_us,p50_us,p95_us,p99_us,max_us\n";
    }

    print_result(benchmark_sequential_write(adapter, fx, cfg), cfg.csv);
    print_result(benchmark_sequential_read(adapter, fx, cfg), cfg.csv);
    print_result(benchmark_random_read(adapter, fx, cfg), cfg.csv);
    print_result(benchmark_random_write(adapter, fx, cfg), cfg.csv);
    print_result(benchmark_mixed_rw(adapter, fx, cfg), cfg.csv);
    print_result(benchmark_fanout_read(adapter, fx, cfg), cfg.csv);
    print_result(benchmark_background_migration(adapter, fx, cfg), cfg.csv);
}

BenchmarkConfig parse_args(int argc, char** argv) {
    BenchmarkConfig cfg;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];

        auto need_value = [&](const char* name) -> const char* {
            if (i + 1 >= argc) {
                throw std::runtime_error(std::string("missing value for ") + name);
            }
            return argv[++i];
        };

        if (arg == "--file-size-mib") {
            cfg.file_size_bytes = static_cast<std::size_t>(std::stoull(need_value("--file-size-mib"))) * 1024ULL * 1024ULL;
        } else if (arg == "--io-size") {
            cfg.io_size_bytes = static_cast<std::size_t>(std::stoull(need_value("--io-size")));
        } else if (arg == "--ops") {
            cfg.ops = static_cast<std::size_t>(std::stoull(need_value("--ops")));
        } else if (arg == "--concurrency") {
            cfg.concurrency = static_cast<std::size_t>(std::stoull(need_value("--concurrency")));
        } else if (arg == "--threads") {
            cfg.thread_pool_threads = static_cast<std::size_t>(std::stoull(need_value("--threads")));
        } else if (arg == "--seed") {
            cfg.seed = static_cast<std::uint64_t>(std::stoull(need_value("--seed")));
        } else if (arg == "--csv") {
            cfg.csv = true;
        } else if (arg == "--verify") {
            cfg.verify = true;
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }

    if (cfg.io_size_bytes == 0) {
        throw std::runtime_error("--io-size must be > 0");
    }
    if (cfg.file_size_bytes < cfg.io_size_bytes) {
        throw std::runtime_error("file size must be >= io size");
    }
    if (cfg.ops == 0 || cfg.concurrency == 0 || cfg.thread_pool_threads == 0) {
        throw std::runtime_error("ops, concurrency, and threads must be > 0");
    }

    return cfg;
}

} // namespace

int main(int argc, char** argv) {
    try {
        const auto cfg = parse_args(argc, argv);

        const std::string run_prefix =
            "perf_blocking_" + std::to_string(::getpid()) + "_" +
            std::to_string(static_cast<unsigned long long>(cfg.seed));

        Fixture fx(cfg.thread_pool_threads, run_prefix);

        BlockingMuxAdapter adapter{fx};
        run_suite(adapter, fx, cfg);

        return 0;
    } catch (const std::exception& ex) {
        log_uncaught_exception(ex, __FILE__, __LINE__, __func__);
        return 1;
    } catch (...) {
        log_uncaught_nonstd(__FILE__, __LINE__, __func__);
        return 1;
    }
}