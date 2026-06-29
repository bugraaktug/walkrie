#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <functional> 

using ChangeEventFn = std::function<void(const pgcdc::ChangeEvent&)>;
struct event_base;

namespace pgcdc 
{

class ReplicationSource 
{
public:
    virtual ~ReplicationSource() = default;

    virtual bool connect() = 0;
    virtual bool start_streaming() = 0;
    virtual bool register_event_loop(event_base* base, ChangeEventFn handle) = 0;
    virtual std::string last_error() const = 0;
};

} // namespace pgcdc
