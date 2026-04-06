#ifndef ASYNCMUX_FILESYSTEM_TIER_HH
#define ASYNCMUX_FILESYSTEM_TIER_HH

#include "../tier.hh"

#include <cppcoro/static_thread_pool.hpp>

#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace asyncmux {

class FileSystemTier final : public Tier {
public:
    FileSystemTier(TierId id,
                   std::string name,
                   std::filesystem::path root_dir,
                   cppcoro::static_thread_pool& pool);

    TierId id() const override;
    std::string name() const override;

    cppcoro::task<IoBuffer> read_at(const std::string& relative_path,
                                    uint64_t offset,
                                    uint64_t size) override;

    cppcoro::task<void> write_at(const std::string& relative_path,
                                 uint64_t offset,
                                 std::span<const Byte> data) override;

    cppcoro::task<void> remove_file(const std::string& relative_path) override;

private:
    std::filesystem::path full_path(const std::string& relative_path) const;
    void ensure_parent_dirs(const std::filesystem::path& p) const;
    std::shared_ptr<std::shared_mutex> lock_for_path(const std::filesystem::path& p) const;

    TierId id_;
    std::string name_;
    std::filesystem::path root_dir_;
    cppcoro::static_thread_pool& pool_;
    mutable std::mutex lock_table_mu_;
    mutable std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>> file_locks_;
};

} // namespace asyncmux

#endif