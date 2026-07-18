#include <doctest.h>
#include <string>
#include <vector>

#include "config.hpp"
#include "pg_sql_builder.hpp"

namespace 
{

pgcdc::TableMapping make_basic_mapping() {
    pgcdc::TableMapping tm;
    tm.source_table = "test_table";
    tm.id_source_ = "id";
    tm.id_sink_ = "item_id";
    tm.embed_source_ = "body";
    tm.embed_sink_ = "item_body";
    return tm;
}

} // namespace

TEST_SUITE("PgSqlBuilder") 
{

    TEST_CASE("build_upsert_sql produces a valid statement, no metadata/discriminator") 
    {
        pgcdc::PgSqlBuilder builder("test_embeddings", "embedding_column");
        auto tm = make_basic_mapping();

        std::string sql = builder.build_upsert_sql(tm);

        CHECK(sql ==
            "INSERT INTO test_embeddings "
            "(item_id, item_body, embedding_column) VALUES ($1, $2, $3::vector) "
            "ON CONFLICT (item_id) DO UPDATE SET item_body = EXCLUDED.item_body, "
            "embedding_column = EXCLUDED.embedding_column");
    }

    TEST_CASE("build_upsert_sql includes metadata columns") 
    {
        pgcdc::PgSqlBuilder builder("test_embeddings", "embedding_column");
        auto tm = make_basic_mapping();

        pgcdc::ColumnMapping cm;
        cm.source_column = "category";
        cm.sink_column = "category";
        cm.role = "metadata";
        tm.columns.push_back(cm);

        std::string sql = builder.build_upsert_sql(tm);

        CHECK(sql ==
            "INSERT INTO test_embeddings "
            "(item_id, item_body, category, embedding_column) VALUES ($1, $2, $3, $4::vector) "
            "ON CONFLICT (item_id) DO UPDATE SET item_body = EXCLUDED.item_body, "
            "category = EXCLUDED.category, embedding_column = EXCLUDED.embedding_column");
    }

    TEST_CASE("build_upsert_sql includes discriminator in conflict key, not in update set") 
    {
        pgcdc::PgSqlBuilder builder("test_embeddings_msource", "embedding_column");
        auto tm = make_basic_mapping();
        tm.has_discriminator_ = true;
        tm.discriminator_sink_ = "category";
        tm.discriminator_label_ = "test_table";

        std::string sql = builder.build_upsert_sql(tm);

        CHECK(sql ==
            "INSERT INTO test_embeddings_msource "
            "(item_id, category, item_body, embedding_column) VALUES ($1, $2, $3, $4::vector) "
            "ON CONFLICT (item_id, category) DO UPDATE SET item_body = EXCLUDED.item_body, "
            "embedding_column = EXCLUDED.embedding_column");
    }

    TEST_CASE("build_delete_sql without discriminator") 
    {
        pgcdc::PgSqlBuilder builder("test_embeddings", "embedding_column");
        auto tm = make_basic_mapping();

        std::string sql = builder.build_delete_sql(tm);

        CHECK(sql == "DELETE FROM test_embeddings WHERE item_id = $1");
    }

    TEST_CASE("build_delete_sql with discriminator") 
    {
        pgcdc::PgSqlBuilder builder("test_embeddings_msource", "embedding_column");
        auto tm = make_basic_mapping();
        tm.has_discriminator_ = true;
        tm.discriminator_sink_ = "category";
        tm.discriminator_label_ = "documents";

        std::string sql = builder.build_delete_sql(tm);

        CHECK(sql == "DELETE FROM test_embeddings_msource WHERE item_id = $1 AND category = $2");
    }
}
