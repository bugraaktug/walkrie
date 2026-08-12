#include <doctest.h>

#include "config.hpp"
#include "qdrantsink_configuration.hpp"
#include "sink_configuration.hpp"

namespace
{

pgcdc::QdrantSinkConfiguration make_minimal_valid_sink()
{
    pgcdc::QdrantSinkConfiguration sink;
    sink.url        = "http://localhost:6333";
    sink.collection = "test_embeddings";

    pgcdc::TableMapping tm;
    tm.source_table  = "test_table";
    tm.id_source_    = "id";
    tm.embed_source_ = "body";
    sink.table_mappings.push_back(tm);

    return sink;
}

bool errors_contain(const std::vector<std::string>& errors, const std::string& needle)
{
    for (const auto& e : errors) {
        if (e.find(needle) != std::string::npos) return true;
    }
    return false;
}

} // namespace

TEST_SUITE("qdrant_sink_configuration")
{

    TEST_CASE("type() reports 'qdrant'")
    {
        pgcdc::QdrantSinkConfiguration sink;
        CHECK(sink.type() == "qdrant");
    }

    TEST_CASE("needs_embedding_provider() defaults true, unlike json-output")
    {
        pgcdc::QdrantSinkConfiguration sink;
        CHECK(sink.needs_embedding_provider());
    }

    TEST_CASE("validate() accepts a fully populated minimal config")
    {
        auto sink = make_minimal_valid_sink();
        CHECK(sink.validate().empty());
    }

    TEST_CASE("validate() catches missing url")
    {
        auto sink = make_minimal_valid_sink();
        sink.url = "";
        CHECK(errors_contain(sink.validate(), "url is required"));
    }

    TEST_CASE("validate() catches missing collection")
    {
        auto sink = make_minimal_valid_sink();
        sink.collection = "";
        CHECK(errors_contain(sink.validate(), "collection is required"));
    }

    TEST_CASE("validate() catches missing table_mapping")
    {
        pgcdc::QdrantSinkConfiguration sink;
        sink.url        = "http://localhost:6333";
        sink.collection = "test_embeddings";
        CHECK(errors_contain(sink.validate(), "at least one [[sink.table_mapping]] block is required"));
    }

    TEST_CASE("validate() catches table_mapping missing source_table")
    {
        auto sink = make_minimal_valid_sink();
        sink.table_mappings[0].source_table = "";
        CHECK(errors_contain(sink.validate(), "source_table is required"));
    }

    TEST_CASE("validate() catches table_mapping missing id role")
    {
        auto sink = make_minimal_valid_sink();
        sink.table_mappings[0].id_source_ = "";
        CHECK(errors_contain(sink.validate(), "role='id'"));
    }

    TEST_CASE("validate() catches table_mapping missing embed role")
    {
        auto sink = make_minimal_valid_sink();
        sink.table_mappings[0].embed_source_ = "";
        CHECK(errors_contain(sink.validate(), "role='embed'"));
    }

    TEST_CASE("validate() catches discriminator_column set without a label")
    {
        auto sink = make_minimal_valid_sink();
        sink.table_mappings[0].has_discriminator_   = true;
        sink.table_mappings[0].discriminator_sink_  = "category";
        sink.table_mappings[0].discriminator_label_ = "";
        CHECK(errors_contain(sink.validate(), "discriminator_label is empty"));
    }

    TEST_CASE("load_from_config() parses url/api_key/collection/required")
    {
        toml::table t = toml::parse(R"(
            type       = "qdrant"
            url        = "http://qdrant.local:6333"
            api_key    = "secret"
            collection = "my_collection"
            required   = false
        )");

        pgcdc::QdrantSinkConfiguration sink;
        sink.load_from_config(t);

        CHECK(sink.url == "http://qdrant.local:6333");
        CHECK(sink.api_key == "secret");
        CHECK(sink.collection == "my_collection");
        REQUIRE(sink.required_override.has_value());
        CHECK_FALSE(*sink.required_override);
    }

    TEST_CASE("load_from_config() parses table_mapping columns and discriminator")
    {
        toml::table t = toml::parse(R"(
            type       = "qdrant"
            url        = "http://localhost:6333"
            collection = "test_embeddings"

            [[table_mapping]]
            source_table = "test_table"
            discriminator_column = "category"
            discriminator_label  = "test"

                [[table_mapping.columns]]
                source_column = "id"
                sink_column   = "item_id"
                role          = "id"

                [[table_mapping.columns]]
                source_column = "name"
                sink_column   = "item_name"
                role          = "embed"
        )");

        pgcdc::QdrantSinkConfiguration sink;
        sink.load_from_config(t);

        REQUIRE(sink.table_mappings.size() == 1);
        const auto& tm = sink.table_mappings[0];
        CHECK(tm.source_table == "test_table");
        CHECK(tm.has_discriminator_);
        CHECK(tm.discriminator_sink_ == "category");
        CHECK(tm.discriminator_label_ == "test");
        CHECK(tm.id_source_ == "id");
        CHECK(tm.embed_source_ == "name");
        CHECK(sink.validate().empty());
    }

    TEST_CASE("instantiate_sink('qdrant') dispatches to QdrantSinkConfiguration")
    {
        auto sink = pgcdc::instantiate_sink("qdrant");
        CHECK(sink->type() == "qdrant");
        CHECK(dynamic_cast<pgcdc::QdrantSinkConfiguration*>(sink.get()) != nullptr);
    }

}
