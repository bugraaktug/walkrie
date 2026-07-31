#include <doctest.h>

#include <filesystem>
#include <fstream>

#include "config.hpp"
#include "pgsink_configuration.hpp"

namespace {

bool errors_contain(const std::vector<std::string>& errors, const std::string& needle) {
    for (const auto& e : errors) {
        if (e.find(needle) != std::string::npos) return true;
    }
    return false;
}

// Mirrors the minimal valid config used elsewhere in the suite, but kept
// local so this file's tests don't depend on test_config_model.cpp.
pgcdc::AppConfig make_minimal_valid_config() {
    pgcdc::AppConfig cfg;
    cfg.settings.log_level = "info";

    pgcdc::SourceConfig src;
    src.dbname = "testdb";
    src.user   = "testuser";
    cfg.sources.push_back(src);

    auto sink = std::make_unique<pgcdc::PgSinkConfiguration>();
    sink->dbname            = "testdb";
    sink->user              = "testuser";
    sink->table             = "test_embeddings";
    sink->embedding_column  = "embedding";

    pgcdc::TableMapping tm;
    tm.source_table  = "test_table";
    tm.id_source_    = "id";
    tm.embed_source_ = "body";
    sink->table_mappings.push_back(tm);

    cfg.sinks.push_back(std::move(sink));

    cfg.embedding.provider   = "llama";
    cfg.embedding.dimensions = 1024;

    return cfg;
}

} // namespace

TEST_SUITE("validate_configuration")
{

    TEST_CASE("validate() catches missing dbname")
    {
        pgcdc::AppConfig cfg;
        std::unique_ptr<pgcdc::PgSinkConfiguration> sink = std::make_unique<pgcdc::PgSinkConfiguration>();
        cfg.sinks.push_back(std::move(sink));
        auto errors = cfg.validate();
        CHECK(errors.size() > 0);
        bool found = false;
        for (auto& e : errors)
            if (e.find("dbname is required") != std::string::npos) 
                found = true;
        CHECK(found);
    }

    TEST_CASE("validate() catches missing embed mapping") 
    {
        pgcdc::AppConfig cfg;
        cfg.sources.push_back({});
        cfg.sources[0].dbname = "qdb";
        cfg.sources[0].user   = "quser";
        std::unique_ptr<pgcdc::PgSinkConfiguration> sink = std::make_unique<pgcdc::PgSinkConfiguration>();
        sink->dbname = "qdb";
        sink->user   = "quser";
        cfg.sinks.push_back(std::move(sink));
    
        // No table_mappings added — should fail
        auto errors = cfg.validate();
        bool found = false;
        for (auto& e : errors)
            if (e.find("at least one [[sink.table_mapping]] block is required") != std::string::npos) 
                found = true;
        CHECK(found);
    }


    TEST_CASE("validate() reports for wrong llm provider name") 
    {
        pgcdc::AppConfig cfg;
        cfg.embedding.provider = "invalid";
        cfg.embedding.model_path = "test_path";
    
        auto errors = cfg.validate();
        bool found = false;
        for (auto& e : errors)
            if (e.find("provider must be 'llama' or 'openai'") != std::string::npos)
                found = true;
        CHECK(found);
    }

    TEST_CASE("validate() accepts a fully populated minimal config")
    {
        auto cfg = make_minimal_valid_config();
        std::filesystem::path model = std::filesystem::temp_directory_path() / "walkrie_test_config_valid.gguf";
        {
            std::ofstream f(model);
            f << "not a real gguf but non-empty and readable";
        }
        cfg.embedding.model_path = model.string();

        auto errors = cfg.validate();
        CHECK(errors.empty());

        std::filesystem::remove(model);
    }

    TEST_CASE("validate() catches invalid log_level")
    {
        auto cfg = make_minimal_valid_config();
        cfg.settings.log_level = "verbose";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "log_level must be one of"));
    }

    TEST_CASE("validate() catches batch_size below 1")
    {
        auto cfg = make_minimal_valid_config();
        cfg.settings.batch_size = 0;

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "batch_size must be >= 1"));
    }

    TEST_CASE("validate() catches non-positive batch_timeout_ms")
    {
        auto cfg = make_minimal_valid_config();
        cfg.settings.batch_timeout_ms = 0;

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "batch_timeout_ms must be > 0"));
    }

    TEST_CASE("validate() catches an empty source list")
    {
        pgcdc::AppConfig cfg;
        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "at least one [[source]] block is required"));
    }

    TEST_CASE("validate() catches missing source user")
    {
        auto cfg = make_minimal_valid_config();
        cfg.sources[0].user = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "[source][0] user is required"));
    }

    TEST_CASE("validate() catches non-positive embedding dimensions")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.dimensions = 0;

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "dimensions must be > 0"));
    }

    TEST_CASE("validate() catches missing api_key for openai provider")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.provider = "openai";
        cfg.embedding.model    = "text-embedding-3-small";
        cfg.embedding.api_key  = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "api_key is required when provider = 'openai'"));
    }

    TEST_CASE("validate() catches missing model for openai provider")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.provider = "openai";
        cfg.embedding.api_key  = "sk-test";
        cfg.embedding.model    = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "model is required when provider = 'openai'"));
    }

    TEST_CASE("validate() catches oversized dimensions for text-embedding-3-small")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.provider   = "openai";
        cfg.embedding.api_key    = "sk-test";
        cfg.embedding.model      = "text-embedding-3-small";
        cfg.embedding.dimensions = 2000;

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "dimensions must be <= 1536 for text-embedding-3-small"));
    }

    TEST_CASE("validate() catches oversized dimensions for text-embedding-3-large")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.provider   = "openai";
        cfg.embedding.api_key    = "sk-test";
        cfg.embedding.model      = "text-embedding-3-large";
        cfg.embedding.dimensions = 4000;

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "dimensions must be <= 3072 for text-embedding-3-large"));
    }

    TEST_CASE("validate() catches wrong dimensions for text-embedding-ada-002")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.provider   = "openai";
        cfg.embedding.api_key    = "sk-test";
        cfg.embedding.model      = "text-embedding-ada-002";
        cfg.embedding.dimensions = 1024;

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "dimensions must be exactly 1536"));
    }

    TEST_CASE("validate() accepts text-embedding-ada-002 with exactly 1536 dimensions")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.provider   = "openai";
        cfg.embedding.api_key    = "sk-test";
        cfg.embedding.model      = "text-embedding-ada-002";
        cfg.embedding.dimensions = 1536;

        auto errors = cfg.validate();
        CHECK(!errors_contain(errors, "text-embedding-ada-002"));
    }

    TEST_CASE("validate() catches missing sink user")
    {
        auto cfg = make_minimal_valid_config();
        static_cast<pgcdc::PgSinkConfiguration&>(*cfg.sinks[0]).user = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "[sink] user is required"));
    }

    TEST_CASE("validate() catches missing sink table")
    {
        auto cfg = make_minimal_valid_config();
        static_cast<pgcdc::PgSinkConfiguration&>(*cfg.sinks[0]).table = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "[sink] table is required"));
    }

    TEST_CASE("validate() catches missing sink embedding column")
    {
        auto cfg = make_minimal_valid_config();
        static_cast<pgcdc::PgSinkConfiguration&>(*cfg.sinks[0]).embedding_column = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "embedding column is required"));
    }

    TEST_CASE("validate() catches table_mapping missing source_table")
    {
        auto cfg = make_minimal_valid_config();
        static_cast<pgcdc::PgSinkConfiguration&>(*cfg.sinks[0]).table_mappings[0].source_table = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "source_table is required"));
    }

    TEST_CASE("validate() catches table_mapping missing id role")
    {
        auto cfg = make_minimal_valid_config();
        static_cast<pgcdc::PgSinkConfiguration&>(*cfg.sinks[0]).table_mappings[0].id_source_ = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "needs a mapping with role='id'"));
    }

    TEST_CASE("validate() catches table_mapping missing embed role")
    {
        auto cfg = make_minimal_valid_config();
        static_cast<pgcdc::PgSinkConfiguration&>(*cfg.sinks[0]).table_mappings[0].embed_source_ = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "needs a mapping with role='embed'"));
    }

    TEST_CASE("validate() catches discriminator_column set without a label")
    {
        auto cfg = make_minimal_valid_config();
        auto& tm = static_cast<pgcdc::PgSinkConfiguration&>(*cfg.sinks[0]).table_mappings[0];
        tm.has_discriminator_   = true;
        tm.discriminator_sink_  = "category";
        tm.discriminator_label_ = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "discriminator_column is set but discriminator_label is empty"));
    }

    TEST_CASE("validate() catches a null sink pointer")
    {
        auto cfg = make_minimal_valid_config();
        cfg.sinks.push_back(nullptr);

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "a valid sink type is required"));
    }
}
