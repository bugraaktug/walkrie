#include <doctest.h>
#include "include/config.hpp"

TEST_SUITE("validate_configuration") {

    TEST_CASE("validate() catches missing dbname") {
        pgcdc::AppConfig cfg;
        auto errors = cfg.validate();
        CHECK(errors.size() > 0);
        bool found = false;
        for (auto& e : errors)
            if (e.find("dbname is required") != std::string::npos) 
                found = true;
        CHECK(found);
    }

    TEST_CASE("validate() catches missing embed mapping") {
        pgcdc::AppConfig cfg;
        cfg.sources.push_back({});
        cfg.sources[0].dbname = "qdb";
        cfg.sources[0].user   = "quser";
        cfg.sink.dbname = "qdb";
        cfg.sink.user   = "quser";
    
        // No table_mappings added — should fail
        auto errors = cfg.validate();
        bool found = false;
        for (auto& e : errors)
            if (e.find("at least one [[sink.table_mapping]] block is required") != std::string::npos) 
                found = true;
        CHECK(found);
    }


    TEST_CASE("validate() reports for wrong llm provider name") {
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
}
