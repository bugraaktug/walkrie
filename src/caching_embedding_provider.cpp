#include "caching_embedding_provider.hpp"

namespace pgcdc
{

size_t cache_size_for_batch(size_t batch_size)
{
    size_t n = std::max(batch_size, size_t(16));
    size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

CachingEmbeddingProvider::CachingEmbeddingProvider(std::shared_ptr<EmbeddingProvider> inner, size_t max_entries)
    : inner_(std::move(inner))
    , max_entries_(max_entries)
{
}

void CachingEmbeddingProvider::init()
{
    inner_->init();
}

const std::vector<float>* CachingEmbeddingProvider::try_get(const std::string& text)
{
    auto it = index_.find(text);
    if (it == index_.end()) return nullptr;
    lru_.splice(lru_.begin(), lru_, it->second); // move to front — most recently used
    return &it->second->second;
}

void CachingEmbeddingProvider::put(const std::string& text, const std::vector<float>& vec)
{
    auto it = index_.find(text);
    if (it != index_.end()) {
        it->second->second = vec;
        lru_.splice(lru_.begin(), lru_, it->second);
        return;
    }

    if (index_.size() >= max_entries_) {
        auto& lru_entry = lru_.back();
        index_.erase(lru_entry.first);
        lru_.pop_back();
    }

    lru_.emplace_front(text, vec);
    index_[text] = lru_.begin();
}

std::vector<float> CachingEmbeddingProvider::embed(const std::string& text)
{
    if (const auto* cached = try_get(text)) return *cached;
    auto vec = inner_->embed(text);
    put(text, vec);
    return vec;
}

std::vector<std::vector<float>> CachingEmbeddingProvider::embed_batch(const std::vector<std::string>& texts)
{
    std::vector<std::vector<float>> results(texts.size());
    std::vector<std::string> uncached_texts;
    std::vector<size_t> uncached_idx;

    for (size_t i = 0; i < texts.size(); ++i) {
        if (const auto* cached = try_get(texts[i])) {
            results[i] = *cached;
        } else {
            uncached_texts.push_back(texts[i]);
            uncached_idx.push_back(i);
        }
    }

    if (!uncached_texts.empty()) {
        auto computed = inner_->embed_batch(uncached_texts);
        if (computed.size() != uncached_texts.size()) {
            return {}; // size mismatch — let the caller's own size check catch this, same as an uncached provider would
        }
        for (size_t j = 0; j < uncached_texts.size(); ++j) {
            results[uncached_idx[j]] = computed[j];
            put(uncached_texts[j], computed[j]);
        }
    }

    return results;
}

} // namespace pgcdc
