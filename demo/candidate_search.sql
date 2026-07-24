-- candidate_search.sql
--
-- Sink table for the walkrie CV/HR semantic search demo. Run once,
-- against the same database as candidates (or a separate one, if your
-- setup keeps source and sink physically apart):
--
--   psql -d hr_demo -f demo/candidate_search.sql

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS candidate_search (
    id                bigserial PRIMARY KEY,
    candidate_id      text NOT NULL UNIQUE,
    full_name         text NOT NULL,
    years_experience  int NOT NULL,
    location          text,
    cv_text           text NOT NULL,
    embedding         vector(1024)
);

-- HNSW is recommended over IVFFlat for a table receiving continuous
-- writes — see TECHNICAL.md's "Vector Indexing" section for why.
CREATE INDEX IF NOT EXISTS candidate_search_embedding_idx
    ON candidate_search USING hnsw (embedding vector_cosine_ops);
