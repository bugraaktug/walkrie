#include "backfill_manager.hpp"
#include <spdlog/spdlog.h>
#include <cerrno>
#include <cstring>

namespace pgcdc 
{

BackfillManager::BackfillManager(event_base* base, const std::string& config_path)
    : base_(base)
    , config_path_(config_path)
    , reap_timer_(nullptr)
    , initialized_(false)
{
    // Set up periodic reaping timer
    reap_timer_ = event_new(base_, -1, EV_PERSIST, on_reap_timer, this);
    timeval interval{2, 0};
    event_add(reap_timer_, &interval);
}

BackfillManager::~BackfillManager() 
{
    if (reap_timer_) {
        event_free(reap_timer_);
        reap_timer_ = nullptr;
    }
    
    // Clean up any remaining workers
    wait_for_all_workers();
}

void BackfillManager::set_worker_path(const std::filesystem::path& worker_path) 
{
    worker_path_ = worker_path;
    initialized_ = true;
}

BackfillSpawnStatus BackfillManager::spawn_if_required(PgReplicationSource& source) 
{
    if (!initialized_) {
        spdlog::error("[BackfillManager] Not initialized: worker path not set");
        return BackfillSpawnStatus::WorkerBinaryMissing;
    }
    
    if (!source.has_pending_backfill_work()) {
        return BackfillSpawnStatus::NoPendingWork;
    }
    
    if (!std::filesystem::exists(worker_path_)) {
        return BackfillSpawnStatus::WorkerBinaryMissing;
    }

    pid_t pid = fork();
    if (pid < 0) {
        return BackfillSpawnStatus::ForkFailed;
    }
    
    if (pid == 0) {
        // Child process
        execl(worker_path_.c_str(), worker_path_.c_str(),
              "-c", config_path_.c_str(),
              "--store", source.backfill_store_path().c_str(),
              "--slot", source.slot_name().c_str(),
              static_cast<char*>(nullptr));
        _exit(127); // exec failed
    }

    // Parent process
    workers_.push_back({pid, source.slot_name()});
    spdlog::info("[BackfillManager] spawned worker pid={} for source '{}'", 
                 pid, source.slot_name());
    return BackfillSpawnStatus::Spawned;
}

void BackfillManager::reap_finished_workers() 
{
    remove_finished_workers();
}

void BackfillManager::remove_finished_workers() 
{
    for (auto it = workers_.begin(); it != workers_.end();) {
        int status = 0;
        pid_t res = waitpid(it->pid, &status, WNOHANG);
        
        if (res == 0) {
            // Still running
            ++it;
            continue;
        }
        
        if (res < 0) {
            spdlog::error("[BackfillManager] waitpid failed for source '{}' pid={}: {}",
                         it->slot_name, it->pid, strerror(errno));
            it = workers_.erase(it);
            continue;
        }
        
        // Worker finished
        int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        if (exit_code == 0) {
            spdlog::info("[BackfillManager] backfill drain complete for source '{}' (pid={})", 
                        it->slot_name, it->pid);
        } else {
            spdlog::error("[BackfillManager] backfill worker for source '{}' (pid={}) exited with code {} — "
                         "its rows are left 'claimed' in the store, not retried", 
                         it->slot_name, it->pid, exit_code);
        }
        it = workers_.erase(it);
    }
}

void BackfillManager::wait_for_all_workers() 
{
    if (workers_.empty()) {
        return;
    }
    
    spdlog::info("[BackfillManager] Waiting for {} backfill worker(s) to finish...", workers_.size());
    
    // Block until all workers are done
    while (!workers_.empty()) {
        // Wait for any child to finish
        int status;
        pid_t pid = wait(&status);
        
        if (pid < 0) {
            if (errno == EINTR) {
                continue;
            }
            spdlog::error("[BackfillManager] wait() failed: {}", strerror(errno));
            break;
        }
        
        // Find and remove the finished worker
        for (auto it = workers_.begin(); it != workers_.end(); ++it) {
            if (it->pid == pid) {
                int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
                spdlog::info("[BackfillManager] Worker for source '{}' (pid={}) finished with code {}", 
                            it->slot_name, pid, exit_code);
                workers_.erase(it);
                break;
            }
        }
    }
}

void BackfillManager::on_reap_timer(evutil_socket_t, short, void* arg) 
{
    auto* mgr = static_cast<BackfillManager*>(arg);
    mgr->reap_finished_workers();
}

void BackfillManager::log_spawn_status(BackfillSpawnStatus status, 
                                       const PgReplicationSource& source,
                                       const std::filesystem::path& worker_path) 
{
    switch (status) {
        case BackfillSpawnStatus::NoPendingWork:
            spdlog::trace("[BackfillManager] no pending backfill work for source '{}' — not spawning", 
                         source.slot_name());
            break;
        case BackfillSpawnStatus::Spawned:
            // Already logged in spawn_if_required()
            break;
        case BackfillSpawnStatus::WorkerBinaryMissing:
            spdlog::error("[BackfillManager] worker binary not found at {} — skipping backfill for source '{}'",
                         worker_path.string(), source.slot_name());
            break;
        case BackfillSpawnStatus::ForkFailed:
            spdlog::error("[BackfillManager] fork failed for source '{}': {}", 
                         source.slot_name(), strerror(errno));
            break;
    }
}

} // namespace pgcdc
