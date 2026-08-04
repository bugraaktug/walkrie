#include "backfill_worker.hpp"

#include <spdlog/spdlog.h>

#include "config.hpp"

namespace pgcdc
{

BackfillWorker::BackfillWorker(std::string config_path, std::string store_path, size_t claim_batch_size)
    : config_path_(std::move(config_path))
    , store_path_(std::move(store_path))
    , claim_batch_size_(claim_batch_size) {}

ChangeEvent BackfillWorker::to_change_event(const BackfillStore::ClaimedRow& row)
{
    DecodedRow decoded;
    decoded.table_name = row.source_table;
    decoded.kind = TupleKind::Insert;

    ordered_json j = ordered_json::parse(row.row_data);
    for (auto it = j.begin(); it != j.end(); ++it) {
        ColumnValue col;
        col.name = it.key();
        if (it.value().is_null()) {
            col.is_null = true;
        } else {
            col.text_value = it.value().get<std::string>();
        }
        decoded.columns.push_back(std::move(col));
    }

    ChangeEvent event;
    event.op = ChangeEvent::Op::Insert; // <<< dump only ever captures pre-existing rows, never a toast-omitted column
    event.table_name = row.source_table;
    event.new_row = std::move(decoded);
    return event;
}

bool BackfillWorker::run()
{
    AppConfig cfg;
    try {
        cfg = load_config(config_path_);
    } catch (const std::exception& e) {
        last_error_ = std::string("config load failed: ") + e.what();
        return false;
    }

    std::vector<std::shared_ptr<EventSink>> sinks;
    try {
        for (const auto& sink_instance : cfg.sinks) {
            sinks.push_back(sink_instance->create_sink(cfg.embedding));
        }
    } catch (const std::exception& e) {
        last_error_ = std::string("sink init failed: ") + e.what();
        return false;
    }

    BackfillStore store(store_path_);
    try {
        store.open();
    } catch (const std::exception& e) {
        last_error_ = std::string("store open failed: ") + e.what();
        return false;
    }

    for (;;) {
        auto claimed = store.claim_pending(claim_batch_size_);
        if (claimed.empty()) break; // <<< dump already ran before this worker was spawned — empty means truly done

        std::vector<ChangeEvent> events;
        events.reserve(claimed.size());
        for (const auto& row : claimed) events.push_back(to_change_event(row));

        try {
            for (auto& sink : sinks) sink->call_batch(events);
        } catch (const std::exception& e) {
            // <<< real exception (not a logged-and-swallowed upsert failure) — leave rows 'claimed' for slice 6's stale-claim reset rather than mark undelivered rows done
            last_error_ = std::string("sink dispatch failed: ") + e.what();
            return false;
        }

        for (const auto& row : claimed) store.mark_done(row.source_table, row.row_id);
        spdlog::info("[BackfillWorker] drained {} row(s)", claimed.size());
    }

    return true;
}

} // namespace pgcdc
