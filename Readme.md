Walkrie (WAL -> Knowledge Retrieval & Indexing Engine) streams PostgreSQL changes and keeps your embeddings and vector indexes up to date.

A minimal C++ daemon that streams logical replication changes from Postgres
(via the pgoutput plugin) 

Postgres WAL
    ↓
PgReplicationSource (libevent, one thread)
    ↓ handler callback (non-blocking, O(1))
EventDispatcher::post_job()
    ↓ SPSC queue
worker thread → ChangeEventSink::call()
    ↓
JsonPrintSink (today)
EmbeddingSink (calls llama or OpenAI, writes to pgvector)
