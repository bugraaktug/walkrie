#include <doctest.h>

#include <filesystem>
#include <fstream>
#include <unistd.h>

#include "config.hpp"
#include "pgsink_configuration.hpp"

namespace fs = std::filesystem;

namespace {

pgcdc::AppConfig make_minimal_valid_config() {
    pgcdc::AppConfig cfg;
    cfg.settings.log_level = "info";

    pgcdc::SourceConfig src;
    src.dbname = "testdb";
    src.user   = "testuser";
    cfg.sources.push_back(src);

    auto sink = std::make_unique<pgcdc::PgSinkConfiguration>();
    sink->dbname           = "testdb";
    sink->user             = "testuser";
    sink->table            = "test_embeddings";
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

bool errors_contain(const std::vector<std::string>& errors, const std::string& needle) {
    for (const auto& e : errors) {
        if (e.find(needle) != std::string::npos) return true;
    }
    return false;
}

} // namespace

TEST_SUITE("AppConfig::validate - embedding model_path checks") {

    TEST_CASE("empty model_path string fails validation") {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.model_path = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "model_path is required"));
    }

    TEST_CASE("nonexistent file path fails validation") {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.model_path = "/tmp/walkrie_test_definitely_does_not_exist_12345.gguf";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "does not exist"));
    }

    TEST_CASE("directory instead of file fails validation") {
        auto cfg = make_minimal_valid_config();
        fs::path dir = fs::temp_directory_path() / "walkrie_test_model_dir";
        fs::create_directories(dir);
        cfg.embedding.model_path = dir.string();

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "not a regular file"));

        fs::remove_all(dir);
    }

    TEST_CASE("unreadable file fails validation") {
        auto cfg = make_minimal_valid_config();
        fs::path file = fs::temp_directory_path() / "walkrie_test_unreadable.gguf";
        {
            std::ofstream f(file);
            f << "fake gguf content";
        }
        fs::permissions(file, fs::perms::none);
        cfg.embedding.model_path = file.string();

        auto errors = cfg.validate();

        // Root bypasses filesystem permission checks entirely, so this
        // assertion is only meaningful for a non-root test runner (a plain
        // CI container running as root would otherwise false-fail here).
        if (geteuid() != 0) {
            CHECK(errors_contain(errors, "not readable"));
        } else {
            MESSAGE("skipped: running as root, permission check cannot be exercised");
        }

        fs::permissions(file, fs::perms::owner_all);
        fs::remove(file);
    }

    TEST_CASE("empty (0-byte) file fails validation") {
        auto cfg = make_minimal_valid_config();
        fs::path file = fs::temp_directory_path() / "walkrie_test_empty.gguf";
        { std::ofstream f(file); } // create, write nothing

        cfg.embedding.model_path = file.string();
        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "empty (0-byte)"));

        fs::remove(file);
    }

    TEST_CASE("valid, readable, non-empty file passes model_path checks") {
        auto cfg = make_minimal_valid_config();
        fs::path file = fs::temp_directory_path() / "walkrie_test_valid.gguf";
        {
            std::ofstream f(file);
            f << "not a real gguf but non-empty and readable";
        }

        cfg.embedding.model_path = file.string();
        auto errors = cfg.validate();

        // No error should mention model_path at all for a genuinely valid file.
        CHECK(!errors_contain(errors, "model_path"));

        fs::remove(file);
    }
}
