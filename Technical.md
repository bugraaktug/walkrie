# Walkrie: Technical Specification & Architecture

This document describes the internal architecture of Walkrie's PostgreSQL-to-vector-database sync engine.

## Architectural Topology

```
 PostgreSQL Logical Replication Slot
              │
              │  pgoutput binary protocol (CopyData: 'w' / 'k' messages)
              ▼
      libevent Ingestor
   (non-blocking I/O; epoll on Linux)
              │
              │  push, lock-free (SPSC)
              ▼
      SPSC Ring Buffer
  (moodycamel::BlockingReaderWriterQueue)
              │
              │  pop, worker thread
              ▼
   Event Dispatcher / Sinks
  (pgoutput decode → embedding provider → pgvector upsert)
```

## Core Systems Design

### 1. Ingestion Layer (`libevent`)

* **Asynchronous event loop** using `libevent`'s non-blocking I/O (`epoll` on Linux) to read the binary `pgoutput` logical replication stream directly off the replication slot connection.
* **Non-blocking status updates** — standby status updates (keepalive acknowledgements) are sent in response to primary keepalive messages, avoiding the need for a dedicated polling thread.
* **Frame-level parsing** — the raw `CopyData` payload is parsed into `XLogData` (`'w'`) and `PrimaryKeepalive` (`'k'`) messages as a distinct, independently tested layer before any `pgoutput` message content is decoded.

### 2. Isolation Layer (Lock-Free SPSC Queue)

* **Single-producer/single-consumer queue** (`moodycamel::BlockingReaderWriterQueue`) decouples the replication read path (producer) from the embedding/sink write path (consumer).
* **Blocking consumer wait** — the worker thread blocks on the queue rather than polling, so an idle pipeline consumes negligible CPU. (Earlier iterations used a spin-wait with a short sleep; this was identified as a CPU-usage regression and replaced with a proper blocking wait — see engineering notes below if this section becomes a public changelog.)
* **Purpose**: an embedding API call (especially over HTTP to a remote provider) can take anywhere from milliseconds to seconds. Isolating this behind a queue means a slow embedding call doesn't block WAL consumption or risk the replication slot falling behind.

### 3. Decode Layer (`pgoutput` Parser)

* Decodes `Begin`, `Relation`, `Insert`, `Update`, `Delete`, and `Commit` messages per the PostgreSQL logical replication protocol.
* Maintains a relation (table schema) cache keyed by relation OID, since `Insert`/`Update`/`Delete` messages carry only column values, not names — the schema is sent once via a `Relation` message and cached client-side.
* Supports both `REPLICA IDENTITY DEFAULT` (key-only old-tuple data on Delete, no old-tuple on Update unless the key changed) and `REPLICA IDENTITY FULL` (complete old-tuple data on Update and Delete).

### 4. Sink Layer

* **Config-driven column mapping** — each source table maps to a sink table with per-column roles (`id`, `embed`, `metadata`) declared in TOML, resolved once at config-load time rather than re-parsed per event.
* **Upsert-based writes** — sink writes use `INSERT ... ON CONFLICT (id) DO UPDATE`, making replays and reconnects safe without manual deduplication.
* **Pluggable embedding providers** — a common `EmbeddingProvider` interface abstracts over local (`llama.cpp`) and remote (OpenAI HTTP API) backends; new providers implement `init()`, `embed()`, `dimensions()`, and `name()`.

## Prerequisites

* PostgreSQL 15+ with logical replication enabled (`wal_level = logical`), a replication slot, and a publication covering the tables you want to sync.
* `libpq`, `libevent`, `spdlog`, `toml++`, `nlohmann-json` development packages.
* `libcurl` development package (`libcurl4-openssl-dev` on Debian/Ubuntu) if using the OpenAI embedding provider.
* A `llama.cpp` checkout (built as a CMake subdirectory) and a compatible GGUF embedding model if using the local Llama provider.
* pgvector installed on the sink database, with the sink table's vector column indexed appropriately (HNSW recommended for tables receiving continuous writes — see note below).

## Compilation

```bash
git clone <repo-url> && cd walkrie
mkdir build && cd build
cmake .. -DLLAMA_DIR=/path/to/llama.cpp
make
```

## Configuration Syntax

```toml
[app]
log_level = "debug"   # trace / debug / info / warn / error / critical
log_file  = "/tmp/logs/walkrie.log"
log_max_size_mb  = 10
log_max_files    = 5

[[source]]
host        = "localhost"
port        = "5432"
dbname      = "qdb"
user        = "quser"
password    = "quser1234"
slot_name   = "cdc_slot"
publication = "test_pub"

[sink]
host            = "localhost"
port            = "5432"
dbname          = "qdb"
user            = "quser"
password        = "quser1234"
table           = "public.test_embeddings"
embed_column    = "embedding"

 [[sink.table_mapping]]
 source_table = "test_table"

    [[sink.table_mapping.columns]]
    source_column = "id"
    sink_column   = "item_id"
    role          = "id"

    [[sink.table_mapping.columns]]
    source_column = "name"
    sink_column   = "item_name"
    role          = "embed"

[embedding]
provider   = "llama"
model_path = "/home/debian/models/bge-m3/bge-m3-Q4_K_M.gguf"
dimensions = 1024
n_threads  = 4
n_ctx      = 512
```

To use multiple sources and collect the embeddings in same sink table, usei discriminator column and labels within the config:

```toml
[app]
log_level = "debug"   # trace / debug / info / warn / error / critical
log_file  = "/tmp/logs/walkrie.log"
log_max_size_mb  = 10
log_max_files    = 5

[[source]]
host        = "localhost"
port        = "5432"
dbname      = "qdb"
user        = "quser"
password    = "quser1234"
slot_name   = "cdc_slot"
publication = "test_pub"

[[source]]
host        = "localhost"
port        = "5432"
dbname      = "qdb"
user        = "quser"
password    = "quser1234"
slot_name   = "pgcdc_slot"
publication = "pgcdc_pub"

[sink]
host            = "localhost"
port            = "5432"
dbname          = "qdb"
user            = "quser"
password        = "quser1234"
table           = "public.test_embeddings_msource"
embed_column    = "embedding"

 [[sink.table_mapping]]
 source_table = "test_table"
 discriminator_column = "category"
 discriminator_label  = "test"
 
    [[sink.table_mapping.columns]]
    source_column = "id"
    sink_column   = "item_id"
    role          = "id"

    [[sink.table_mapping.columns]]
    source_column = "name"
    sink_column   = "item_name"
    role          = "embed"

 [[sink.table_mapping]]
 source_table = "documents"
 discriminator_column = "category"
 discriminator_label  = "documents"

    [[sink.table_mapping.columns]]
    source_column = "id"
    sink_column   = "item_id"
    role          = "id"

    [[sink.table_mapping.columns]]
    source_column = "body"
    sink_column   = "item_name"
    role          = "embed"

[embedding]
provider   = "llama"
model_path = "/home/debian/models/bge-m3/bge-m3-Q4_K_M.gguf"
api_key    = ""
dimensions = 1024
n_threads  = 4
n_ctx      = 512

```

**Note on `discriminator_column`:** this column is used  as the sources might use the same ids, thus, should also exists in a real unique constraint on the sink table.Otherwise, the generated SQL will use ON CONFLICT (item_id) and others ON CONFLICT (item_id, category), and Postgres will throw "no unique or exclusion constraint matching" for whichever ones don't match your actual table's constraint.

To use OpenAI instead of a local model, replace the `[embedding]` block:

```toml
[embedding]
provider   = "openai"
model      = "text-embedding-3-small"
api_key    = "sk-proj-....."
dimensions = 1536
```

**Note on `dimensions`:** this value must match both the embedding model's actual output size and the sink table's `vector(N)` column width. `text-embedding-3-small` supports truncation down to any size ≤ 1536; `text-embedding-3-large` down to any size ≤ 3072; `text-embedding-ada-002` does not support truncation and must be set to exactly 1536.

## Execution

```bash
./walkrie ../config_sample.toml
```

## Vector Indexing (Operator Responsibility)

Walkrie writes to the sink table but does not currently create or manage the vector index itself — this is a manual step on the sink database:

```sql
CREATE INDEX ON test_embeddings USING hnsw (embedding vector_cosine_ops);
```

HNSW is recommended over IVFFlat for tables receiving continuous writes: IVFFlat's clustering is fixed at index-build time and degrades in recall quality as new data drifts from the original distribution, requiring periodic `REINDEX`. HNSW updates incrementally as rows are inserted, which matches Walkrie's continuous-write pattern.

## Performance

*Benchmarks pending — a load test (bulk insert of 1M rows, measuring end-to-end replication lag and resource usage) is planned. Numbers will be published here once available.*
