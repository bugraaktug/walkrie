#pragma once

#include <iostream>
#include <string>
#include <thread>

#include "event.hpp"

namespace pgcdc 
{

struct EventSink
{
    virtual void call(const pgcdc::ChangeEvent&) = 0;
    virtual void call_batch(const std::vector<ChangeEvent>& events) { for (const auto& e : events) call(e); }
    virtual bool default_required() const { return true; } // <<< config `required =` overrides this per sink
    virtual std::string name() const { return "sink"; } // <<< for log traceability (which sink stalled/failed)
    virtual ~EventSink() = default;
};

class EmbeddingSink : public EventSink
{
public:
    virtual ~EmbeddingSink() = default;
 
    virtual void init() = 0;
    virtual void call(const ChangeEvent& event) = 0;
};

} 
