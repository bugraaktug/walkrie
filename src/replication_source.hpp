#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <functional>

using SourceId = size_t;
using ChangeEventFn = std::function<void(const pgcdc::ChangeEvent&, SourceId)>;
struct event_base;

namespace pgcdc
{

class ReplicationSource
{
public:
    explicit ReplicationSource(SourceId id) : id_(id) {}
    virtual ~ReplicationSource() = default;

    SourceId id() const { return id_; }

    virtual bool connect() = 0;
    virtual bool start_streaming() = 0;
    virtual bool register_event_loop(event_base* base, ChangeEventFn handle) = 0;
    virtual std::string last_error() const = 0;
    virtual void set_confirmed_lsn(uint64_t lsn) = 0;

private:
    SourceId id_;
};

} // namespace pgcdc
