#pragma once

#include "model_reader.hpp"

namespace pgcdc
{

// Reads embedding dimension from a GGUF file's header/KV section only —
// never loads tensor weight data (gguf_init_from_file is called with
// no_alloc=true), so cost is negligible a few KB
class GgufMetadataReader : public ModelReader
{
public:
    ModelMetadata read(const std::string& model_path) const override;
};

} // namespace pgcdc
