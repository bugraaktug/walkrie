# Walkrie

**WAL → Knowledge Retrieval & Indexing Engine**

A low-latency PostgreSQL-to-vector sync engine that keeps your embeddings up to date as your source data changes.

Walkrie streams, validates, and syncs PostgreSQL transactional data into vector embeddings (`pgvector`, with pluggable OpenAI or local Llama backends) in real time — no batch jobs, no polling, no cron.

Built in native C++ using asynchronous `libevent` I/O and a lock-free single-producer/single-consumer (SPSC) queue, Walkrie is designed to isolate slow, unpredictable embedding API calls from your live Postgres replication stream, so a slow embedding provider never causes replication lag to build up unbounded.

## Why Walkrie

Most CDC-to-vector pipelines today are built from general-purpose tools (Debezium + Kafka + a custom consumer, or scripted polling jobs) that carry real operational cost: a mandatory message broker, WAL disk bloat from slow consumers, table locks during initial snapshots, and fragile handling of schema changes. Walkrie is a single native binary with no external message broker dependency — it reads the replication slot directly and writes to your vector sink directly.

* **Lock-Free Pipeline Architecture** — Isolates network-bound embedding API calls from the core replication read path using a lock-free SPSC queue, so a slow or rate-limited embedding provider doesn't block WAL consumption.
* **Native Logical Replication Listener** — Reads directly from a PostgreSQL logical replication slot (`pgoutput`) using `libevent`'s non-blocking I/O — no polling, no external broker.
* **Pluggable Embedding Backends** — Local embeddings via `llama.cpp` (fully offline, no API costs or data leaving your infrastructure) or OpenAI's embeddings API, selectable per deployment.
* **Configurable Column Mapping** — Map specific source columns to embedding input, ID, and metadata roles per table via TOML config — no code changes needed to onboard a new table.

## MVP Feature Set

* **Real-time WAL streaming** via native logical replication slot listener.
* **Multi-table mapping** — configure multiple source tables, each with independent column mappings, in a single config file. Multiple sources can share one sink table using a static discriminator label, so overlapping IDs across tables never collide.
* **OpenAI & local Llama integrations** — switch embedding provider via a single config field.
* **Pre-flight data validation layer** (null/duplicate filtering before embedding calls).
* **Upsert-based sink writes** — idempotent by design; replays and reconnects don't duplicate rows.

## Roadmap

* Batched embedding API requests (currently one HTTP call per row for OpenAI provider).
* Vector index management helpers (HNSW index creation/verification on sink tables).
* Published performance benchmarks (throughput, replication lag, memory footprint) under real load.

## Target Customers

* **Teams running RAG or semantic search on top of an existing Postgres database** who want embeddings to stay current without building and maintaining a custom sync pipeline.
* **Resource-constrained engineering teams** who want a single lightweight binary instead of standing up Kafka or a scripted batch job to keep a vector store in sync.

## Security & Deployment

Walkrie runs entirely within your own infrastructure. Database credentials, replicated data, and schema details stay local to wherever you deploy the binary — nothing is sent to any third party. 
If you configure the OpenAI embedding provider, only the specific text fields you've mapped for embedding are sent to OpenAI's API, under OpenAI's own data handling terms — Walkrie itself does not collect or transmit any data.

---

Licensed under a Developer-First Commercial License. Built by a solo developer for engineers who care about mechanical sympathy.

