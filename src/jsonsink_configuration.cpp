#include "jsonsink_configuration.hpp"

#include <sstream>
#include <spdlog/spdlog.h>

namespace pgcdc
{
namespace {

class JsonSink : public EventSink {
public:
    explicit JsonSink(std::string target) : target_(std::move(target)) {
        if (target_ != "stdout" && target_ != "discard") {
            file_.open(target_, std::ios::out | std::ios::app);
            if (!file_.is_open()) {
                throw std::runtime_error("JsonSink: failed to open output file: " + target_);
            }
        }
    }

    bool default_required() const override { return false; } // <<< best-effort by default, unlike pg sinks
    std::string name() const override { return "json-output"; }

    void call(const ChangeEvent& event) override {
        ordered_json j = event;
        std::string line = j.dump();

        if (target_ == "discard") {
            // Still pay the serialization cost — this isolates "decode +
            // serialize" from "decode + serialize + write", rather than
            // skipping serialization entirely and understating cost.
            volatile size_t len = line.size();
            (void)len;
            return;
        }
        if (target_ == "stdout") {
            std::cout << line << "\n";
            return;
        }
        file_ << line << "\n";
    }

private:
    std::string target_;
    std::ofstream file_;
};

} // namespace

void JsonSinkConfiguration::load_from_config(const toml::table& t) 
{
    auto v = t.get_as<std::string>("output_target");
    if (v) output_target = **v;

    if (auto req = t.get_as<bool>("required")) required_override = **req;
}

std::shared_ptr<EventSink> JsonSinkConfiguration::create_sink(const EmbeddingConfig&) const 
{
    spdlog::info("[SinkConfiguration] initialized sink — {}", type());
    return std::make_shared<JsonSink>(output_target);
}

} // namespace pgcdc
