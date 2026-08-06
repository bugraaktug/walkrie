#pragma once

#include <atomic>
#include <filesystem>
#include <vector>
#include <string>
#include <memory>

#include <event2/event.h>
#include <sys/wait.h>
#include <unistd.h>

#include "pgreplication_source.hpp"

namespace pgcdc 
{

struct BackfillWorkerHandle 
{
    pid_t pid;
    std::string slot_name;
};

enum class BackfillSpawnStatus 
{
    NoPendingWork,       // nothing to drain — not an error, just nothing to do
    WorkerBinaryMissing,
    ForkFailed,
    Spawned,
};

class BackfillManager 
{
public:
    explicit BackfillManager(event_base* base, const std::string& config_path);
    ~BackfillManager();

    BackfillManager(const BackfillManager&) = delete;
    BackfillManager& operator=(const BackfillManager&) = delete;

    void set_worker_path(const std::filesystem::path& worker_path);
    
    BackfillSpawnStatus spawn_if_required(PgReplicationSource& source);
    
    event* get_reap_timer() const { return reap_timer_; }
    bool has_running_workers() const { return !workers_.empty(); }
    void reap_finished_workers();
    void wait_for_all_workers();
    
    // Log spawn status
    static void log_spawn_status(BackfillSpawnStatus status, 
                                 const PgReplicationSource& source,
                                 const std::filesystem::path& worker_path);

private:
    static void on_reap_timer(evutil_socket_t, short, void* arg);
    
    void reap_worker(BackfillWorkerHandle& worker);
    void remove_finished_workers();

    event_base* base_;
    std::string config_path_;
    std::filesystem::path worker_path_;
    std::vector<BackfillWorkerHandle> workers_;
    event* reap_timer_;
    bool initialized_;
};

} // namespace pgcdc
