#include "gguf_metadata_reader.hpp"

#include <cstdint>
#include <memory>

#include <gguf.h>

namespace pgcdc
{

namespace
{

std::optional<int64_t> read_int_kv(const gguf_context* ctx, const std::string& key)
{
    int64_t key_id = gguf_find_key(ctx, key.c_str());
    if (key_id < 0) return std::nullopt;

    switch (gguf_get_kv_type(ctx, key_id)) {
        case GGUF_TYPE_UINT32: return gguf_get_val_u32(ctx, key_id);
        case GGUF_TYPE_INT32:  return gguf_get_val_i32(ctx, key_id);
        case GGUF_TYPE_UINT64: return static_cast<int64_t>(gguf_get_val_u64(ctx, key_id));
        case GGUF_TYPE_INT64:  return gguf_get_val_i64(ctx, key_id);
        default:               return std::nullopt; // unexpected type for this key — don't guess
    }
}

} // namespace

ModelMetadata GgufMetadataReader::read(const std::string& model_path) const
{
    ModelMetadata meta;

    gguf_init_params params;
    params.no_alloc = true;
    params.ctx = nullptr;

    std::unique_ptr<gguf_context, decltype(&gguf_free)> ctx(gguf_init_from_file(model_path.c_str(), params), &gguf_free);
    if (!ctx) {
        return meta; // not a valid GGUF file at all — parsed stays false
    }
    meta.parsed = true;

    int64_t arch_key = gguf_find_key(ctx.get(), "general.architecture");
    if (arch_key < 0 || gguf_get_kv_type(ctx.get(), arch_key) != GGUF_TYPE_STRING) {
        return meta; // valid GGUF, but no architecture declared — nothing to look up embedding_length under
    }
    std::string arch = gguf_get_val_str(ctx.get(), arch_key);

    // Prefer the output-projection dimension when a model declares one (a
    // handful of architectures add a final projection layer that changes
    // the size of the vector actually returned) — fall back to the base
    // embedding_length, which every architecture with an embedding sets.
    meta.embedding_dimensions = read_int_kv(ctx.get(), arch + ".embedding_length_out");
    if (!meta.embedding_dimensions) {
        meta.embedding_dimensions = read_int_kv(ctx.get(), arch + ".embedding_length");
    }

    return meta;
}

} // namespace pgcdc
