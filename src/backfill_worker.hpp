#pragma once

#include <string>

#include "backfill_store.hpp"
#include "event.hpp"

namespace pgcdc
{

// <<< claims rows from one source's BackfillStore, dispatches via the normal sinks, deletes them
class BackfillWorker
{
public:
    BackfillWorker(std::string config_path, std::string store_path, size_t claim_batch_size = 200);

    bool run(); // <<< drains until claim_pending() is empty; false + last_error() on an unrecoverable error
    std::string last_error() const { return last_error_; }

    static ChangeEvent to_change_event(const BackfillStore::ClaimedRow& row); // <<< rehydrates a stored row as an Insert event; public for unit testing

private:
    std::string config_path_;
    std::string store_path_;
    size_t claim_batch_size_;
    std::string last_error_;
};

} // namespace pgcdc
