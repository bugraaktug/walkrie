#include <doctest.h>

#include <memory>
#include <string>
#include <vector>

#include "caching_embedding_provider.hpp"
#include "config.hpp"
#include "embedding_provider.hpp"

namespace
{

// Records call counts/args instead of computing real embeddings — deterministic
// per-text output lets tests assert on cache hit/miss behavior directly.
class CountingEmbeddingProvider : public pgcdc::EmbeddingProvider
{
public:
    void init() override {}

    std::vector<float> embed(const std::string& text) override
    {
        ++embed_calls;
        return vec_for(text);
    }

    std::vector<std::vector<float>> embed_batch(const std::vector<std::string>& texts) override
    {
        ++embed_batch_calls;
        last_batch = texts;
        std::vector<std::vector<float>> out;
        out.reserve(texts.size());
        for (const auto& t : texts) out.push_back(vec_for(t));
        return out;
    }

    int dimensions() const override { return 1; }
    std::string name() const override { return "counting_test_provider"; }

    int embed_calls = 0;
    int embed_batch_calls = 0;
    std::vector<std::string> last_batch;

private:
    static std::vector<float> vec_for(const std::string& text)
    {
        return {static_cast<float>(std::hash<std::string>{}(text) % 1000)};
    }
};

} // namespace

TEST_SUITE("caching_embedding_provider")
{

    TEST_CASE("embed() cache hit avoids a second inner call, returns identical vector")
    {
        auto inner = std::make_shared<CountingEmbeddingProvider>();
        pgcdc::CachingEmbeddingProvider cache(inner, /*max_entries=*/4);

        auto a = cache.embed("hello");
        CHECK(inner->embed_calls == 1);

        auto b = cache.embed("hello");
        CHECK(inner->embed_calls == 1);
        CHECK(a == b);
    }

    TEST_CASE("embed() cache miss for a different text calls inner again")
    {
        auto inner = std::make_shared<CountingEmbeddingProvider>();
        pgcdc::CachingEmbeddingProvider cache(inner, /*max_entries=*/4);

        cache.embed("hello");
        cache.embed("world");
        CHECK(inner->embed_calls == 2);
    }

    TEST_CASE("embed_batch() only forwards cache misses to inner")
    {
        auto inner = std::make_shared<CountingEmbeddingProvider>();
        pgcdc::CachingEmbeddingProvider cache(inner, /*max_entries=*/4);

        cache.embed("a"); // warm the cache

        auto results = cache.embed_batch({"a", "b", "a", "c"});
        REQUIRE(results.size() == 4);
        CHECK(results[0] == results[2]); // both "a"
        CHECK(inner->last_batch == std::vector<std::string>{"b", "c"});
    }

    TEST_CASE("LRU eviction drops the least recently used entry once max_entries is exceeded")
    {
        auto inner = std::make_shared<CountingEmbeddingProvider>();
        pgcdc::CachingEmbeddingProvider cache(inner, /*max_entries=*/2);

        cache.embed("a");
        cache.embed("b");
        cache.embed("c"); // evicts "a"

        int calls_before = inner->embed_calls;
        cache.embed("a"); // miss again
        CHECK(inner->embed_calls == calls_before + 1);
    }

    TEST_CASE("accessing an entry marks it most-recently-used, protecting it from eviction")
    {
        auto inner = std::make_shared<CountingEmbeddingProvider>();
        pgcdc::CachingEmbeddingProvider cache(inner, /*max_entries=*/2);

        cache.embed("a");
        cache.embed("b");
        cache.embed("a"); // touch "a" -- "b" becomes the LRU entry
        cache.embed("c"); // should evict "b", not "a"

        int calls_before = inner->embed_calls;
        cache.embed("a");
        CHECK(inner->embed_calls == calls_before); // still cached
    }
}

TEST_SUITE("cache_size_for_batch")
{

    TEST_CASE("floors small batch sizes to 16")
    {
        CHECK(pgcdc::cache_size_for_batch(1) == 16);
        CHECK(pgcdc::cache_size_for_batch(16) == 16);
    }

    TEST_CASE("rounds up to the next power of 2 above the floor")
    {
        CHECK(pgcdc::cache_size_for_batch(17) == 32);
        CHECK(pgcdc::cache_size_for_batch(30) == 32);
    }

    TEST_CASE("leaves an exact power of 2 unchanged")
    {
        CHECK(pgcdc::cache_size_for_batch(64) == 64);
    }

    TEST_CASE("scales for large batch sizes")
    {
        CHECK(pgcdc::cache_size_for_batch(1000) == 1024);
    }
}

TEST_SUITE("create_initialized_embedding_provider")
{

    TEST_CASE("unknown provider name throws instead of returning null")
    {
        pgcdc::EmbeddingConfig cfg;
        cfg.provider = "not-a-real-provider";

        CHECK_THROWS_AS(pgcdc::create_initialized_embedding_provider(cfg), std::runtime_error);
    }
}
