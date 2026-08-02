#pragma once

#include <atomic>
#include <ctime>
#include <string>

#include <libpq-fe.h>
#include <event2/event.h>

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
    
    event* read_event_ = nullptr;
    event* timer_event_ = nullptr;

    void resume_slot();

    static void on_read(evutil_socket_t fd, short events, void* arg);
    static void on_timer(evutil_socket_t fd, short events, void* arg);
};

} // namespace pgcdc
