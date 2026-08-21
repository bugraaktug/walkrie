-- product_search.sql
--
-- Sink table for the walkrie multilingual e-commerce semantic search demo.
-- Run once, against the same database as products:
--
--   psql -d ecommerce_demo -f demo/product_search.sql

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS product_search (
    id          bigserial PRIMARY KEY,
    product_id  text NOT NULL UNIQUE,
    sku         text NOT NULL,
    title       text NOT NULL,
    category    text,
    language    text,
    price       numeric(10,2),
    currency    text,
    description text NOT NULL,
    embedding   vector(1024)
);

-- HNSW is recommended over IVFFlat for a table receiving continuous
-- writes — see TECHNICAL.md's "Vector Indexing" section for why.
CREATE INDEX IF NOT EXISTS product_search_embedding_idx
    ON product_search USING hnsw (embedding vector_cosine_ops);
