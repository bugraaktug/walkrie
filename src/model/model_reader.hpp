#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace pgcdc
{

struct ModelMetadata
{
    bool parsed = false;                          // true if this reader recognized/parsed the file at all
    std::optional<int64_t> embedding_dimensions;   // nullopt if the format/file doesn't declare one
};

class ModelReader
{
public:
    virtual ~ModelReader() = default;
    virtual ModelMetadata read(const std::string& model_path) const { (void)model_path; return ModelMetadata{}; }
};

} // namespace pgcdc
