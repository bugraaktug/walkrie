#include <doctest.h>

#include <filesystem>
#include <fstream>
#include <unistd.h>

#include <gguf.h>

#include "config.hpp"
#include "pgsink_configuration.hpp"

namespace fs = std::filesystem;

namespace 
{

void write_test_gguf(const std::string& path, const std::string& arch, uint32_t embedding_length) 
{
    gguf_context* ctx = gguf_init_empty();
    gguf_set_val_str(ctx, "general.architecture", arch.c_str());
    gguf_set_val_u32(ctx, (arch + ".embedding_length").c_str(), embedding_length);
    gguf_write_to_file(ctx, path.c_str(), false);
    gguf_free(ctx);
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

bool errors_contain(const std::vector<std::string>& errors, const std::string& needle) 
{
    for (const auto& e : errors) {
        if (e.find(needle) != std::string::npos) return true;
    }
    return false;
}

} // namespace

TEST_SUITE("AppConfig::validate - embedding model_path checks") 
{

    TEST_CASE("empty model_path string fails validation") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.model_path = "";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "model_path is required"));
    }

    TEST_CASE("nonexistent file path fails validation") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.model_path = "/tmp/walkrie_test_definitely_does_not_exist_12345.gguf";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "does not exist"));
    }

    TEST_CASE("directory instead of file fails validation") 
    {
        auto cfg = make_minimal_valid_config();
        fs::path dir = fs::temp_directory_path() / "walkrie_test_model_dir";
        fs::create_directories(dir);
        cfg.embedding.model_path = dir.string();

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "not a regular file"));

        fs::remove_all(dir);
    }

    TEST_CASE("unreadable file fails validation") 
    {
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

    TEST_CASE("empty (0-byte) file fails validation") 
    {
        auto cfg = make_minimal_valid_config();
        fs::path file = fs::temp_directory_path() / "walkrie_test_empty.gguf";
        { std::ofstream f(file); } // create, write nothing

        cfg.embedding.model_path = file.string();
        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "empty (0-byte)"));

        fs::remove(file);
    }

    TEST_CASE("valid, readable, non-empty file passes model_path checks") 
    {
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

TEST_SUITE("AppConfig::validate - [embedding] validate severity (GGUF dims check)") 
{

    TEST_CASE("default validate is 'none' — a garbage (non-GGUF) file is not even opened, no error") 
    {
        auto cfg = make_minimal_valid_config();
        CHECK(cfg.embedding.validate == "none");

        fs::path file = fs::temp_directory_path() / "walkrie_test_garbage_default.gguf";
        { std::ofstream f(file); f << "not a real gguf"; }
        cfg.embedding.model_path = file.string();

        auto errors = cfg.validate();
        CHECK(!errors_contain(errors, "model_path"));

        fs::remove(file);
    }

    TEST_CASE("an unrecognized validate value fails validation") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.validate = "bogus";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "validate must be one of: none, warn, force"));
    }

    TEST_CASE("validate='force': non-GGUF file is a hard error") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.validate = "force";

        fs::path file = fs::temp_directory_path() / "walkrie_test_garbage_force.gguf";
        { std::ofstream f(file); f << "not a real gguf"; }
        cfg.embedding.model_path = file.string();

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "not a valid GGUF file"));

        fs::remove(file);
    }

    TEST_CASE("validate='force': matching dims passes with no error") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.validate = "force";
        cfg.embedding.dimensions = 1024;

        fs::path file = fs::temp_directory_path() / "walkrie_test_match_force.gguf";
        write_test_gguf(file.string(), "bert", 1024);
        cfg.embedding.model_path = file.string();

        auto errors = cfg.validate();
        CHECK(!errors_contain(errors, "model_path"));
        CHECK(!errors_contain(errors, "dimensions"));

        fs::remove(file);
    }

    TEST_CASE("validate='force': mismatched dims is a hard error naming both values") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.validate = "force";
        cfg.embedding.dimensions = 1024;

        fs::path file = fs::temp_directory_path() / "walkrie_test_mismatch_force.gguf";
        write_test_gguf(file.string(), "bert", 768);
        cfg.embedding.model_path = file.string();

        auto errors = cfg.validate();
        REQUIRE(errors_contain(errors, "does not match the embedding size"));
        CHECK(errors_contain(errors, "1024"));
        CHECK(errors_contain(errors, "768"));

        fs::remove(file);
    }

    TEST_CASE("validate='warn': mismatched dims logs but does not fail validation") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.validate = "warn";
        cfg.embedding.dimensions = 1024;

        fs::path file = fs::temp_directory_path() / "walkrie_test_mismatch_warn.gguf";
        write_test_gguf(file.string(), "bert", 768);
        cfg.embedding.model_path = file.string();

        auto errors = cfg.validate();
        // Same problem as the 'force' case above, but it must not land in
        // the errors vector — validation should still pass, since 'warn'
        // means "continue", not "stop running".
        CHECK(!errors_contain(errors, "does not match the embedding size"));

        fs::remove(file);
    }

    TEST_CASE("validate='force': a GGUF file whose architecture has no embedding_length is not an error") 
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.validate = "force";

        fs::path file = fs::temp_directory_path() / "walkrie_test_no_embedding_length.gguf";
        gguf_context* ctx = gguf_init_empty();
        gguf_set_val_str(ctx, "general.architecture", "mystery_arch");
        gguf_write_to_file(ctx, file.string().c_str(), false);
        gguf_free(ctx);
        cfg.embedding.model_path = file.string();

        auto errors = cfg.validate();
        CHECK(!errors_contain(errors, "model_path"));
        CHECK(!errors_contain(errors, "dimensions"));
        CHECK(!errors_contain(errors, "embedding size"));

        fs::remove(file);
    }
}

TEST_SUITE("AppConfig::validate - embedding lora_path checks")
{

    TEST_CASE("empty lora_path is optional and passes validation")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.lora_path = "";

        auto errors = cfg.validate();
        CHECK(!errors_contain(errors, "lora_path"));
    }

    TEST_CASE("nonexistent lora_path fails validation")
    {
        auto cfg = make_minimal_valid_config();
        cfg.embedding.lora_path = "/tmp/walkrie_test_lora_definitely_does_not_exist_12345.gguf";

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "lora_path does not exist"));
    }

    TEST_CASE("directory instead of file fails lora_path validation")
    {
        auto cfg = make_minimal_valid_config();
        fs::path dir = fs::temp_directory_path() / "walkrie_test_lora_dir";
        fs::create_directories(dir);
        cfg.embedding.lora_path = dir.string();

        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "lora_path exists but is not a regular file"));

        fs::remove_all(dir);
    }

    TEST_CASE("unreadable lora_path fails validation")
    {
        auto cfg = make_minimal_valid_config();
        fs::path file = fs::temp_directory_path() / "walkrie_test_lora_unreadable.gguf";
        {
            std::ofstream f(file);
            f << "fake lora adapter content";
        }
        fs::permissions(file, fs::perms::none);
        cfg.embedding.lora_path = file.string();

        auto errors = cfg.validate();

        // Same root-bypass caveat as the model_path unreadable-file test above.
        if (geteuid() != 0) {
            CHECK(errors_contain(errors, "lora_path exists but is not readable"));
        } else {
            MESSAGE("skipped: running as root, permission check cannot be exercised");
        }

        fs::permissions(file, fs::perms::owner_all);
        fs::remove(file);
    }

    TEST_CASE("empty (0-byte) lora_path fails validation")
    {
        auto cfg = make_minimal_valid_config();
        fs::path file = fs::temp_directory_path() / "walkrie_test_lora_empty.gguf";
        { std::ofstream f(file); } // create, write nothing

        cfg.embedding.lora_path = file.string();
        auto errors = cfg.validate();
        CHECK(errors_contain(errors, "lora_path points to an empty (0-byte) file"));

        fs::remove(file);
    }

    TEST_CASE("valid, readable, non-empty lora_path passes validation")
    {
        auto cfg = make_minimal_valid_config();
        fs::path file = fs::temp_directory_path() / "walkrie_test_lora_valid.gguf";
        {
            std::ofstream f(file);
            f << "not a real gguf but non-empty and readable";
        }

        cfg.embedding.lora_path = file.string();
        auto errors = cfg.validate();
        CHECK(!errors_contain(errors, "lora_path"));

        fs::remove(file);
    }
}
