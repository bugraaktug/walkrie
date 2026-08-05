#pragma once

#include <atomic>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include <libpq-fe.h>
#include <event2/event.h>

#include "backfill_util.hpp"
#include "pgoutput_parser.hpp"
#include "replication_source.hpp"

namespace pgcdc {

struct PgReplicationConfig
{
    std::string host = "localhost";
    std::string port = "5432";
    std::string dbname;
    std::string user;
    std::string password;
    std::string slot_name = "pgcdc_slot";
    std::string publication_name = "pgcdc_pub";

    bool                      backfill = false;
    std::vector<TableMapping> backfill_table_mappings; // <<< ignored unless backfill is true
    std::string               backfill_store_path;     // <<< ignored unless backfill is true

    std::string to_conninfo() const;
};

class PgReplicationSource : public ReplicationSource
{
public:
    explicit PgReplicationSource(SourceId id, PgReplicationConfig config);
    ~PgReplicationSource() override;

    // Non-copyable (owns a raw PGconn*), movable not needed for current use.
    PgReplicationSource(const PgReplicationSource&) = delete;
    PgReplicationSource& operator=(const PgReplicationSource&) = delete;

    bool connect() override;
    bool start_streaming() override;
    bool register_event_loop(event_base* base, ChangeEventFn handle) override;
    std::string last_error() const override;
    void set_confirmed_lsn(uint64_t lsn) override;
    void flush_confirmed_lsn() override;

    bool was_slot_freshly_created() const { return slot_freshly_created_; }
    bool run_backfill_dump_if_required(); // <<< no-op (returns true) unless backfill is enabled and the slot was freshly created this run
    bool has_pending_backfill_work() const; // <<< false if backfill is disabled for this source; caller uses this to decide whether to spawn a drain worker
    const std::string& slot_name() const { return config_.slot_name; }
    const std::string& backfill_store_path() const { return config_.backfill_store_path; }

protected:
    void ping_update();
    void drain_available_messages();

private:
    PgReplicationConfig config_;
    PGconn* conn_ = nullptr;
    std::string last_error_;
    std::string start_lsn_ = "0/0";
    uint64_t last_read_lsn_ = 0;             // stream position we've parsed up to (not yet durably sunk)
    std::atomic<uint64_t> confirmed_lsn_{0}; // durably sunk position, reported to Postgres via ping_update()
    std::time_t last_status_update_ = 0;
    PgOutputParser parser_;
    ChangeEventFn handle_;
    bool slot_freshly_created_ = false;
    std::unique_ptr<BackfillUtil> backfill_util_; // <<< null unless backfill is enabled in config for this source

    event* read_event_ = nullptr;
    event* timer_event_ = nullptr;

    void resume_slot();

    static void on_read(evutil_socket_t fd, short events, void* arg);
    static void on_timer(evutil_socket_t fd, short events, void* arg);
    
    std::vector<TableMapping> filter_to_source_publication(const std::vector<TableMapping>& mappings) const;
};

} // namespace pgcdc
