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
