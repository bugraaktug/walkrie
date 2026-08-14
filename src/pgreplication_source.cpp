#include "pgreplication_source.hpp"
#include "wal_frame.hpp"

#include <sys/select.h>
#include <event2/event.h>

#include <cstdint>
#include <cstdio>
#include <spdlog/spdlog.h>
#include <sstream>
#include <unordered_set>

namespace pgcdc 
{

namespace 
{

std::string format_lsn(uint64_t lsn) 
{
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%X/%X",
                  static_cast<uint32_t>(lsn >> 32),
                  static_cast<uint32_t>(lsn & 0xFFFFFFFF));
    return buf;
}

uint64_t parse_lsn(const std::string& s) 
{
    uint32_t hi = 0, lo = 0;
    std::sscanf(s.c_str(), "%X/%X", &hi, &lo);
    return (static_cast<uint64_t>(hi) << 32) | lo;
}

} // namespace

std::string PgReplicationConfig::to_conninfo() const 
{
    std::ostringstream out;
    out << "host=" << host
        << " port=" << port
        << " dbname=" << dbname
        << " user=" << user;
    
    if (!password.empty()) {
        out << " password=" << password;
    }
    out << tls.to_conninfo_fragment();
    out << " replication=database";

    return out.str();
}

PgReplicationSource::PgReplicationSource(SourceId id, PgReplicationConfig config)
    : ReplicationSource(id), config_(std::move(config))
{
    if (config_.backfill) {
        backfill_util_ = std::make_unique<BackfillUtil>(
            config_.to_conninfo(), config_.backfill_table_mappings, config_.backfill_store_path);
    }
}

PgReplicationSource::~PgReplicationSource() 
{
    if (read_event_) {
        event_free(read_event_);
        read_event_ = nullptr;
    }

    if (timer_event_) {
        event_free(timer_event_);
        timer_event_ = nullptr;
    }

    if (conn_) {
        PQfinish(conn_);
    }
}

std::string PgReplicationSource::last_error() const 
{
    return last_error_;
}

void PgReplicationSource::resume_slot()
{
    if (!conn_) {
        spdlog::error("[PgReplicationSource] [{}] resume_slot() called before connect()",
                     config_.slot_name.c_str());
        start_lsn_ = "0/0";
        return;
    }
    std::string resume_query = "SELECT confirmed_flush_lsn FROM pg_replication_slots "
                               "WHERE slot_name = '" + config_.slot_name + "'";

    PGresult* resume_res = PQexec(conn_, resume_query.c_str());
    if (PQresultStatus(resume_res) == PGRES_TUPLES_OK &&
        PQntuples(resume_res) > 0 &&
        !PQgetisnull(resume_res, 0, 0)) {
        start_lsn_ = PQgetvalue(resume_res, 0, 0);
        spdlog::warn("[PgReplicationSource] [{}] slot exists, resuming from LSN {}",
                     config_.slot_name.c_str(), start_lsn_.c_str());
    } else {
        start_lsn_ = "0/0";
        spdlog::warn("[PgReplicationSource] [{}] slot exists but no confirmed LSN yet, "
                     "starting from beginning of retained WAL", config_.slot_name.c_str());
    }
    PQclear(resume_res);
}

std::vector<TableMapping> PgReplicationSource::filter_to_source_publication(const std::vector<TableMapping>& mappings) const
{
    std::string sql = "SELECT tablename FROM pg_publication_tables WHERE pubname = '" + config_.publication_name + "'";
    PGresult* res = PQexec(conn_, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::string err = "failed to read publication membership for '" + config_.publication_name + "': " + PQerrorMessage(conn_);
        PQclear(res);
        throw std::runtime_error(err);
    }

    std::unordered_set<std::string> published_tables;
    for (int r = 0; r < PQntuples(res); ++r) {
        published_tables.insert(PQgetvalue(res, r, 0));
    }
    PQclear(res);

    std::vector<TableMapping> filtered;
    for (const auto& tm : mappings) {
        if (published_tables.count(tm.source_table)) {
            filtered.push_back(tm);
            spdlog::trace("[PgReplicationSource] [{}] '{}' is in publication '{}' — included in backfill scope",
                         config_.slot_name, tm.source_table, config_.publication_name);
        } else {
            // <<< table_mappings are global to the sink, not scoped per source — exclude ones this source's publication doesn't own
            spdlog::info("[PgReplicationSource] [{}] '{}' is not a member of publication '{}' — excluding from backfill scope",
                         config_.slot_name, tm.source_table, config_.publication_name);
        }
    }
    return filtered;
}

bool PgReplicationSource::connect()
{
    const std::string conninfo = config_.to_conninfo();
    conn_ = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn_) != CONNECTION_OK) {
        last_error_ = PQerrorMessage(conn_);
        PQfinish(conn_);
        conn_ = nullptr;
        return false;
    }

    std::string create_cmd = "CREATE_REPLICATION_SLOT " + config_.slot_name + " LOGICAL pgoutput";
    PGresult* res = PQexec(conn_, create_cmd.c_str());

    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        start_lsn_ = PQgetvalue(res, 0, 1); // column 1 = consistent_point
        slot_freshly_created_ = true;
        spdlog::info("[PgReplicationSource] created replication slot '{}', starting at LSN {}",
                     config_.slot_name.c_str(), start_lsn_.c_str());
        PQclear(res);
    } else {
        PQclear(res);
        resume_slot();
    }

    if (backfill_util_) {
        try {
            backfill_util_->open();
            backfill_util_->set_table_mappings(filter_to_source_publication(config_.backfill_table_mappings));
            if (slot_freshly_created_) {
                backfill_util_->reset(); // <<< fresh slot = new epoch, discard any stale state from a prior slot by this name
            } else {
                backfill_util_->reset_stale_claims(); // <<< a worker from the previous process (if any) can't still be alive after a restart
            }
        } catch (const std::exception& e) {
            last_error_ = std::string("backfill store open failed: ") + e.what();
            return false;
        }
    }

    last_read_lsn_ = parse_lsn(start_lsn_);
    confirmed_lsn_.store(last_read_lsn_, std::memory_order_relaxed);
    last_status_update_ = std::time(nullptr);
    return true;
}

bool PgReplicationSource::run_backfill_dump_if_required()
{
    if (!backfill_util_) return true;
    // <<< skip only when backfill was never active for this slot (just flipped on for an existing slot) — resume finishes an interrupted dump
    if (!slot_freshly_created_ && !backfill_util_->has_prior_dump_state()) {
        spdlog::trace("[PgReplicationSource] [{}] backfill never active for this slot — skipping dump", config_.slot_name);
        return true;
    }

    spdlog::info("[PgReplicationSource] [{}] running backfill dump (fresh_slot={})", config_.slot_name, slot_freshly_created_);
    if (!backfill_util_->dump_all()) {
        last_error_ = backfill_util_->last_error();
        return false;
    }
    return true;
}

bool PgReplicationSource::has_pending_backfill_work() const
{
    if (!backfill_util_) return false;
    try {
        return backfill_util_->has_pending_work();
    } catch (const std::exception& e) {
        spdlog::error("[PgReplicationSource] [{}] has_pending_backfill_work() check failed: {}",
                     config_.slot_name.c_str(), e.what());
        return false;
    }
}

void PgReplicationSource::set_confirmed_lsn(uint64_t lsn)
{
    confirmed_lsn_.store(lsn, std::memory_order_release);
}

void PgReplicationSource::flush_confirmed_lsn()
{
    if (!conn_) return; // <<< never connected; nothing to flush
    ping_update();
}

bool PgReplicationSource::start_streaming() 
{
    if (!conn_) {
        last_error_ = "start_streaming() called before a successful connect()";
        return false;
    }

    std::ostringstream cmd;
    cmd << "START_REPLICATION SLOT " << config_.slot_name
        << " LOGICAL " << start_lsn_
        << " (proto_version '1', publication_names '" << config_.publication_name << "')";

    PGresult* res = PQexec(conn_, cmd.str().c_str());
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        last_error_ = PQerrorMessage(conn_);
        PQclear(res);
        return false;
    }
    PQclear(res);

    spdlog::info("[PgReplicationsource] streaming changes from publication '{}'...",
                 config_.publication_name.c_str());
    return true;
}

void PgReplicationSource::ping_update() 
{
    unsigned char msg[1 + 8 + 8 + 8 + 8 + 1];
    unsigned char* p = msg;
    *p++ = 'r';

    auto put_u64 = [&p](uint64_t v) {
        for (int i = 7; i >= 0; --i) *p++ = static_cast<unsigned char>((v >> (i * 8)) & 0xFF);
    };

    uint64_t confirmed = confirmed_lsn_.load(std::memory_order_acquire);
    put_u64(confirmed); // last written
    put_u64(confirmed); // last flushed
    put_u64(confirmed); // last applied

    spdlog::trace("[PgReplicationSource] [{}] sending standby status update, confirmed_lsn={}",
                  config_.slot_name.c_str(), format_lsn(confirmed).c_str());

    constexpr uint64_t kPgEpochOffsetSecs = 946684800ULL; // 2000-01-01 vs unix epoch
    uint64_t now_us = (static_cast<uint64_t>(std::time(nullptr)) - kPgEpochOffsetSecs) * 1000000ULL;
    put_u64(now_us);

    *p++ = 0; // reply requested = false

    if (PQputCopyData(conn_, reinterpret_cast<const char*>(msg), static_cast<int>(p - msg)) <= 0) {
        spdlog::warn("[PgReplicationSource] failed to send standby status update: {}",
                     PQerrorMessage(conn_));
        return;
    }
    PQflush(conn_);
}

// PQconsumeInput() pulls whatever's arrived on the socket into libpq's
// internal buffer; PQgetCopyData(async=1) then hands us messages out of
// that buffer without blocking. We loop until it returns 0 (nothing
// more buffered right now) since a single libevent wakeup can carry
// several queued messages, especially after a burst of writes upstream.
void PgReplicationSource::drain_available_messages() 
{
   PQconsumeInput(conn_);

   while (true) {
       char* buf = nullptr;
       int copy_len = PQgetCopyData(conn_, &buf, /*async=*/1);
 
       if (copy_len == 0) {
            break; // nothing more buffered right now; wait for next wakeup
       }
 
       if (copy_len < 0) {
           last_error_ = PQerrorMessage(conn_);
           spdlog::info("[PgReplicationSource] [{}] replication stream ended: {}",
                        config_.slot_name.c_str(), last_error_.c_str());
           // Stream is over — stop watching this fd so libevent doesn't
           // keep calling us back on a dead connection.
           if (read_event_) {
               event_del(read_event_);
           }
           if (timer_event_) {
               event_del(timer_event_);
           }
           return;
       }
       
       if (buf[0] == 'k') {
           auto keepalive = pgcdc::parse_keepalive_message(buf, static_cast<size_t>(copy_len));
           if (!keepalive) {
               spdlog::warn("[PgReplicationSource] [{}] truncated keepalive message",
                            config_.slot_name.c_str());
           } else if (keepalive->reply_requested) {
               ping_update();
               last_status_update_ = std::time(nullptr);
           }
       } else if (buf[0] == 'w') {
           auto header = pgcdc::parse_xlogdata_header(buf, static_cast<size_t>(copy_len));
           if (!header) {
               spdlog::warn("[PgReplicationSource] [{}] truncated XLogData message",
                            config_.slot_name.c_str());
           } else {
               try {
                   auto events = parser_.parse(header->payload, header->payload_len);
                   if (events) {
                       for (auto& event : *events) {
                           event.commit_lsn = header->wal_start;
                           bool absorbed = backfill_util_ && backfill_util_->absorb_event(event);
                           if (!absorbed) handle_(event, id());
                       }
                   }
                } catch (const std::exception& e) {
                    spdlog::error("[PgReplicationSource] [{}] error decoding message at LSN {}: {} (skipping)",
                            config_.slot_name.c_str(),
                            format_lsn(header->wal_start).c_str(),
                            e.what());
                }
           }
           last_read_lsn_ = header->wal_start;
       }
       PQfreemem(buf);
   }
}

void PgReplicationSource::on_read(evutil_socket_t /*fd*/, short /*events*/, void* arg) {
    static_cast<PgReplicationSource*>(arg)->drain_available_messages();
}

void PgReplicationSource::on_timer(evutil_socket_t /*fd*/, short /*events*/, void* arg) {
    static_cast<PgReplicationSource*>(arg)->ping_update();
}

bool PgReplicationSource::register_event_loop(event_base* base, ChangeEventFn handle) 
{
     if (!conn_) {
        last_error_ = "register_event_loop() called before a successful connect()";
        return false;
    }
 
    handle_ = handle;
 
    int sock = PQsocket(conn_);
    if (sock < 0) {
        last_error_ = "PQsocket() returned an invalid descriptor";
        return false;
    }
 
    read_event_ = event_new(base, sock, EV_READ | EV_PERSIST, &PgReplicationSource::on_read, this);
    if (!read_event_) {
        last_error_ = "event_new() failed for the replication socket";
        return false;
    }
    if (event_add(read_event_, nullptr) != 0) {
        last_error_ = "event_add() failed for the replication socket";
        return false;
    }
 
    timer_event_ = event_new(base, -1, EV_PERSIST, &PgReplicationSource::on_timer, this);
    if (!timer_event_) {
        last_error_ = "event_new() failed for the standby-status timer";
        return false;
    }
    struct timeval interval{10, 0}; // 10 seconds, matching the old manual check
    if (event_add(timer_event_, &interval) != 0) {
        last_error_ = "event_add() failed for the standby-status timer";
        return false;
    }
 
    return true;
}

} // namespace pgcdc
