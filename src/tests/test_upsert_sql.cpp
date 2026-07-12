#include <doctest.h>
#include <string>
#include <vector>

#include "include/config.hpp"
#include "embedding_provider.hpp"
#include "pgembedding_sink.hpp"

#define BUILD_SQL_ONE "INSERT INTO test_embeddings " \
                      "(item_id, item_body, embedding_column) VALUES ($1, $2, $3::vector) "\
                      "ON CONFLICT (item_id) DO UPDATE SET item_body = EXCLUDED.item_body, "\
                      "embedding_column = EXCLUDED.embedding_column"

#define BUILD_SQL_TWO "INSERT INTO test_embeddings "\
                      "(item_id, item_body, category, embedding_column) VALUES ($1, $2, $3, $4::vector) "\
                      "ON CONFLICT (item_id) DO UPDATE SET item_body = EXCLUDED.item_body, "\
                      "category = EXCLUDED.category, embedding_column = EXCLUDED.embedding_column"

namespace pgcdc {

class TestEmbedSink : public pgcdc::PgEmbeddingSink
{
public:
    using PgEmbeddingSink::PgEmbeddingSink;  // inherit constructor

    std::string build_upsert_sql_public(const TableMapping& tm) {
        return build_upsert_sql(tm);
    }
};

}

struct EmbeddingConfig; 

class TestEmbedProvider : public pgcdc::EmbeddingProvider 
{
public:
    TestEmbedProvider(const pgcdc::EmbeddingConfig& /*cfg*/) {}
    ~TestEmbedProvider() {}

    void init() {
    }
    
    std::vector<float> embed(const std::string& /*text*/) {
        return {};
    }

    int dimensions() const {
        return 1024;
    }

    std::string name() const {
        return "test_embed_model";
    }
};


TEST_SUITE("build_upsert_sql") {

    TEST_CASE("build_upsert() builds a valid sql") {
        pgcdc::PgEmbeddingSinkConfig sink_cfg;
        pgcdc::TableMapping tm;
        pgcdc::EmbeddingConfig embed_cfg;
        
        std::shared_ptr<pgcdc::EmbeddingProvider> provider = std::make_shared<TestEmbedProvider>(embed_cfg);
        provider->init();
       
        tm.id_source_ = "id";
        tm.id_sink_ ="item_id";
        tm.embed_source_ = "body";
        tm.embed_sink_ = "item_body";
        
        std::ostringstream conn;
        conn << "host="     << "localhost"
             << " port="    << "5432"
             << " dbname="  << "postgres"
             << " user="    << "test"
             << " password=" << "test";
        
        sink_cfg.pg_conninfo = conn.str();
        sink_cfg.sink_table  = "test_embeddings";
        sink_cfg.sink_column = "embedding_column";
        sink_cfg.mappings.push_back(tm);
        
        pgcdc::TestEmbedSink sink(sink_cfg, provider);
        std::string sql = sink.build_upsert_sql_public(tm);

        CHECK(sql == BUILD_SQL_ONE);
    }

    TEST_CASE("build_upsert() with metadat columns builds a valid sql") {
        pgcdc::PgEmbeddingSinkConfig sink_cfg;
        pgcdc::TableMapping tm;
        pgcdc::EmbeddingConfig embed_cfg;
        
        std::shared_ptr<pgcdc::EmbeddingProvider> provider = std::make_shared<TestEmbedProvider>(embed_cfg);
        provider->init();
       
        tm.id_source_ = "id";
        tm.id_sink_ ="item_id";
        tm.embed_source_ = "body";
        tm.embed_sink_ = "item_body";

        pgcdc::ColumnMapping cm_metadata;
        cm_metadata.source_column = "category";
        cm_metadata.sink_column = "category";
        cm_metadata.role = "metadata";
        tm.columns.push_back(cm_metadata);
        
        sink_cfg.pg_conninfo = "host=localhost port=5432 dbname=postgres user=test password=test";
        sink_cfg.sink_table  = "test_embeddings";
        sink_cfg.sink_column = "embedding_column";
        sink_cfg.mappings.push_back(tm);
        
        pgcdc::TestEmbedSink sink(sink_cfg, provider);
        std::string sql = sink.build_upsert_sql_public(tm);

        CHECK(sql == BUILD_SQL_TWO);
    }

}
