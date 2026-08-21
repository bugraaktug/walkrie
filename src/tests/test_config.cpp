#include <doctest.h>

#include <filesystem>
#include <fstream>

#include "config.hpp"
#include "pgsink_configuration.hpp"

namespace 
{

bool errors_contain(const std::vector<std::string>& errors, const std::string& needle) 
{
    for (const auto& e : errors) {
        if (e.find(needle) != std::string::npos) return true;
    }
    return false;
}

pgcdc::AppConfig make_minimal_valid_config() 
{
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

    TEST_CASE("TlsConfig::validate() is a no-op when every field is left unset")
    {
        pgcdc::TlsConfig tls;
        CHECK(tls.validate("[source][0]").empty());
        CHECK(tls.to_conninfo_fragment().empty());
    }

    TEST_CASE("TlsConfig::validate() catches an unknown sslmode")
    {
        pgcdc::TlsConfig tls;
        tls.sslmode = "verify-strict"; // not a real libpq sslmode value
        CHECK(errors_contain(tls.validate("[source][0]"), "sslmode must be one of"));
    }

    TEST_CASE("TlsConfig::validate() catches sslcert set without sslkey")
    {
        pgcdc::TlsConfig tls;
        tls.sslcert = "/etc/walkrie/tls/client.crt";
        CHECK(errors_contain(tls.validate("[source][0]"), "sslcert and sslkey must both be set together"));
    }

    TEST_CASE("TlsConfig::validate() catches sslkey set without sslcert")
    {
        pgcdc::TlsConfig tls;
        tls.sslkey = "/etc/walkrie/tls/client.key";
        CHECK(errors_contain(tls.validate("[source][0]"), "sslcert and sslkey must both be set together"));
    }

    TEST_CASE("TlsConfig::validate() catches a missing sslrootcert file")
    {
        pgcdc::TlsConfig tls;
        tls.sslrootcert = "/nonexistent/ca.pem";
        CHECK(errors_contain(tls.validate("[source][0]"), "sslrootcert does not exist"));
    }

    TEST_CASE("TlsConfig::validate() accepts a fully populated mutual-TLS config")
    {
        namespace fs = std::filesystem;
        fs::path dir = fs::temp_directory_path() / "walkrie_test_tls";
        fs::create_directories(dir);
        for (const auto* name : {"ca.pem", "client.crt", "client.key"}) {
            std::ofstream f(dir / name);
            f << "not a real cert but non-empty and readable";
        }

        pgcdc::TlsConfig tls;
        tls.sslmode     = "verify-full";
        tls.sslrootcert = (dir / "ca.pem").string();
        tls.sslcert     = (dir / "client.crt").string();
        tls.sslkey      = (dir / "client.key").string();

        CHECK(tls.validate("[source][0]").empty());
        CHECK(tls.to_conninfo_fragment() ==
              " sslmode=verify-full sslrootcert=" + (dir / "ca.pem").string() +
              " sslcert=" + (dir / "client.crt").string() +
              " sslkey=" + (dir / "client.key").string());

        fs::remove_all(dir);
    }
}

namespace
{

pgcdc::AppConfig load_config_from_toml(const std::string& contents)
{
    std::filesystem::path path = std::filesystem::temp_directory_path() / "walkrie_test_config.toml";
    {
        std::ofstream f(path);
        f << contents;
    }
    auto cfg = pgcdc::load_config(path.string());
    std::filesystem::remove(path);
    return cfg;
}

} // namespace

TEST_SUITE("load_config parsing")
{
    TEST_CASE("[[source]] backfill defaults to false when omitted")
    {
        auto cfg = load_config_from_toml(R"(
            [[source]]
            dbname = "testdb"
            user   = "testuser"
        )");
        REQUIRE(cfg.sources.size() == 1);
        CHECK(cfg.sources[0].backfill == false);
    }

    TEST_CASE("[[source]] backfill parses true")
    {
        auto cfg = load_config_from_toml(R"(
            [[source]]
            dbname   = "testdb"
            user     = "testuser"
            backfill = true
        )");
        REQUIRE(cfg.sources.size() == 1);
        CHECK(cfg.sources[0].backfill == true);
    }

    TEST_CASE("[[source]] backfill parses explicit false")
    {
        auto cfg = load_config_from_toml(R"(
            [[source]]
            dbname   = "testdb"
            user     = "testuser"
            backfill = false
        )");
        REQUIRE(cfg.sources.size() == 1);
        CHECK(cfg.sources[0].backfill == false);
    }

    TEST_CASE("[[source]] tls fields are empty (undefined) when omitted")
    {
        auto cfg = load_config_from_toml(R"(
            [[source]]
            dbname = "testdb"
            user   = "testuser"
        )");
        REQUIRE(cfg.sources.size() == 1);
        const auto& tls = cfg.sources[0].tls;
        CHECK(tls.sslmode.empty());
        CHECK(tls.sslrootcert.empty());
        CHECK(tls.sslcert.empty());
        CHECK(tls.sslkey.empty());
        CHECK(tls.sslpassword.empty());
        CHECK(tls.to_conninfo_fragment().empty());
        CHECK(tls.validate("[source][0]").empty());
    }

    TEST_CASE("[[source]] tls fields parse when present")
    {
        auto cfg = load_config_from_toml(R"(
            [[source]]
            dbname      = "testdb"
            user        = "testuser"
            sslmode     = "verify-full"
            sslrootcert = "/etc/walkrie/tls/ca.pem"
            sslcert     = "/etc/walkrie/tls/client.crt"
            sslkey      = "/etc/walkrie/tls/client.key"
            sslpassword = "keypass"
        )");
        REQUIRE(cfg.sources.size() == 1);
        const auto& tls = cfg.sources[0].tls;
        CHECK(tls.sslmode == "verify-full");
        CHECK(tls.sslrootcert == "/etc/walkrie/tls/ca.pem");
        CHECK(tls.sslcert == "/etc/walkrie/tls/client.crt");
        CHECK(tls.sslkey == "/etc/walkrie/tls/client.key");
        CHECK(tls.sslpassword == "keypass");
    }

    TEST_CASE("[embedding] lora_path defaults empty and lora_scale defaults to 1.0 when omitted")
    {
        auto cfg = load_config_from_toml(R"(
            [embedding]
            provider   = "llama"
            model_path = "/opt/models/model.gguf"
        )");
        CHECK(cfg.embedding.lora_path.empty());
        CHECK(cfg.embedding.lora_scale == doctest::Approx(1.0f));
    }

    TEST_CASE("[embedding] lora_path and lora_scale parse when present")
    {
        auto cfg = load_config_from_toml(R"(
            [embedding]
            provider    = "llama"
            model_path  = "/opt/models/model.gguf"
            lora_path   = "/opt/models/lora-retrieval.passage.gguf"
            lora_scale  = 0.5
        )");
        CHECK(cfg.embedding.lora_path == "/opt/models/lora-retrieval.passage.gguf");
        CHECK(cfg.embedding.lora_scale == doctest::Approx(0.5f));
    }
}
