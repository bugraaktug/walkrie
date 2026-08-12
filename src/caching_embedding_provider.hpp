#pragma once

#include <algorithm>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "embedding_provider.hpp"

namespace pgcdc
{

size_t cache_size_for_batch(size_t batch_size); // <<< smallest power of 2 >= max(batch_size, 16)

class CachingEmbeddingProvider : public EmbeddingProvider
{
public:
    explicit CachingEmbeddingProvider(std::shared_ptr<EmbeddingProvider> inner, size_t max_entries = 64);

    void init() override;
    std::vector<float> embed(const std::string& text) override;
    std::vector<std::vector<float>> embed_batch(const std::vector<std::string>& texts) override;
    int dimensions() const override { return inner_->dimensions(); }
    std::string name() const override { return inner_->name(); }

private:
    using Entry = std::pair<std::string, std::vector<float>>;

    std::shared_ptr<EmbeddingProvider> inner_;
    size_t max_entries_;
    std::list<Entry> lru_;                                            // front = most recently used
    std::unordered_map<std::string, std::list<Entry>::iterator> index_;

    const std::vector<float>* try_get(const std::string& text); // touches LRU order on hit
    void put(const std::string& text, const std::vector<float>& vec);
};

} // namespace pgcdc
