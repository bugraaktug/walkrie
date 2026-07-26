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
* **Blocking consumer wait** — the worker thread blocks on the queue rather than polling, so an idle pipeline consumes negligible CPU.
* **Purpose**: an embedding API call (especially over HTTP to a remote provider) can take anywhere from milliseconds to seconds. Isolating this behind a queue means a slow embedding call doesn't block WAL consumption or risk the replication slot falling behind.
* **Optional event batching** — `EventDispatcher` can group up to `batch_size` events (bounded by `batch_timeout_ms`, so low-traffic periods still flush promptly rather than waiting indefinitely to fill a batch) into a single `call_batch()` invocation per sink, instead of dispatching one event at a time. This is what allows a provider's `embed_batch()` to turn N embedding calls into fewer, larger ones (see Sink Layer below). Default `batch_size = 1` reproduces the original one-event-at-a-time behavior exactly — batching is strictly opt-in via config.
* **Batching applies uniformly across all configured sinks in one dispatch cycle** — grouping happens once, upstream of the per-sink loop, not independently per sink. A `json-output` sink (which has no real batching work to do) still experiences the same grouping delay as a batching-aware `postgres-embedding` sink in the same job, and sinks are processed sequentially in `[[sink]]` config order within a batch — list latency-sensitive sinks first if running multiple sinks together and this matters for your use case.

### 3. Decode Layer (`pgoutput` Parser)

* Decodes `Begin`, `Relation`, `Insert`, `Update`, `Delete`, and `Commit` messages per the PostgreSQL logical replication protocol.
* Maintains a relation (table schema) cache keyed by relation OID, since `Insert`/`Update`/`Delete` messages carry only column values, not names — the schema is sent once via a `Relation` message and cached client-side.
* Supports both `REPLICA IDENTITY DEFAULT` (key-only old-tuple data on Delete, no old-tuple on Update unless the key changed) and `REPLICA IDENTITY FULL` (complete old-tuple data on Update and Delete).
* Propagates transaction `commit_timestamp` (decoded from the `Begin` message) onto every `ChangeEvent`, used downstream for replication-lag measurement.

### 4. Sink Layer

* **Config-driven column mapping** — each source table maps to a sink table with per-column roles (`id`, `embed`, `metadata`) declared in TOML, resolved once at config-load time rather than re-parsed per event.
* **Upsert-based writes** — sink writes use `INSERT ... ON CONFLICT (id) DO UPDATE`, making replays and reconnects safe without manual deduplication.
* **Skip-unchanged updates** — on `Update` events, `PgEmbeddingSink` skips the embedding call entirely if the embed column is TOAST-unchanged or textually identical to the prior value, avoiding unnecessary embedding cost on updates that didn't touch the embedded field. This check applies identically whether an event arrives via the single-event or batched path.
* **Pluggable embedding providers** — a common `EmbeddingProvider` interface abstracts over local (`llama.cpp`) and remote (OpenAI HTTP API) backends; new providers implement `init()`, `embed()`, `dimensions()`, and `name()`. Providers additionally expose `embed_batch()`, defaulting to a loop over `embed()` for providers that haven't implemented real batching — `OpenAIProvider` overrides this with a genuine single-request batched call (`"input": [text1, text2, ...]`); `LlamaProvider` currently uses the default loop (see Known Limitations below).
* **Pluggable sink types** — a `SinkConfiguration` interface (`postgres-embedding`, `json-output`) allows multiple sink blocks in one config, each independently constructing and owning its runtime `EventSink`. `EventSink` similarly exposes `call_batch()`, defaulting to a loop over `call()`; `PgEmbeddingSink` overrides it with real batching (below).
* **Batched upsert ordering (`PgEmbeddingSink::call_batch`)** — validates every event in a batch first (skip-unchanged checks, null/empty-field checks — no database writes yet), collects everything needing an embedding into one `embed_batch()` call, then applies every resulting action (upserts *and* deletes) in a second pass, strictly preserving the batch's original event order. This ordering guarantee matters concretely: if an insert and a delete for the same row land in the same batch, applying them out of original order (e.g. running the delete immediately during validation, before a same-batch insert has been upserted) can silently resurrect a row that should have ended up deleted. The two-pass design exists specifically to prevent that. Covered by both a unit test (`test_pgembedding_sink_batch_ordering.cpp`) and a live-database integration test (see Testing below).

## Known Limitations

* **`LlamaProvider` does not yet implement real batched embedding.** `embed_batch()` currently falls back to the default base-class loop (one `embed()` call per text), so configuring `batch_size > 1` with the local Llama provider groups events at the dispatcher/sink level (fewer, larger DB-write batches) but does not yet reduce the number of `llama_encode()` calls. Real batching requires raising `n_seq_max` in `llama_context_params` and building a multi-sequence `llama_batch` — planned, not yet implemented. `OpenAIProvider` already benefits fully from `batch_size > 1` today (see PERFORMANCE.md for measured numbers).
* Once `LlamaProvider::embed_batch()` is implemented with genuine multi-sequence computation, batched and sequential calls for the same input are not guaranteed to produce bit-identical embeddings (floating-point summation order differs inside a batched matmul vs. independent sequential calls) — this is a well-documented phenomenon in comparable transformer implementations, though not something llama.cpp's own documentation currently addresses directly. The integration test suite's equality tolerance will need loosening from near-zero to an appropriate drift tolerance at that point — see `integration_tests/README.md` for the full discussion and cited references.

## Prerequisites

* PostgreSQL 15+ with logical replication enabled (`wal_level = logical`), a replication slot, and a publication covering the tables you want to sync.
* `libpq`, `libevent`, `spdlog`, `toml++`, `nlohmann-json` development packages.
* `libcurl` development package (`libcurl4-openssl-dev` on Debian/Ubuntu) if using the OpenAI embedding provider.
* A `llama.cpp` checkout and a compatible GGUF embedding model if using the local Llama provider. See [Model Installation](#model-installation) below.
* pgvector installed on the sink database, with the sink table's vector column indexed appropriately (HNSW recommended for tables receiving continuous writes — see note below).

## Compilation (from source)

`llama.cpp` is vendored as a git submodule — a plain `git clone` leaves it as an empty directory, so submodules must be initialized explicitly:

```bash
git clone --recurse-submodules <repo-url> && cd walkrie
# if you forgot --recurse-submodules above:
git submodule update --init --recursive

mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

`LLAMA_DIR` defaults to `third_party/llama.cpp` (relative to the repo root) — override with `-DLLAMA_DIR=/path/to/llama.cpp` only if using a checkout elsewhere.

## Installation (pre-built `.deb` package)

```bash
sudo dpkg -i walkrie_1.0.1~alpha1-1_amd64.deb
sudo apt install -f
```

This installs the binary to `/usr/bin/walkrie`, a config template to `/etc/walkrie/config.toml`, a systemd unit (enabled, not auto-started), and creates `/var/lib/walkrie/models/` and `/var/log/walkrie/`. See the README's [Installation](./README.md#installation-debianubuntu) section for the post-install steps (model placement, config editing, service start).

### Model Installation

The local Llama provider requires a GGUF-format embedding model. The package does not bundle one (models are large and licensed separately from Walkrie itself). To use `BGE-M3` (the model used throughout this project's benchmarks — see PERFORMANCE.md):

```bash
# Example — adjust source/filename to whichever GGUF quantization you want.
sudo -u walkrie wget -O /var/lib/walkrie/models/bge-m3-Q4_K_M.gguf \
    <URL to your chosen GGUF file, e.g. from a Hugging Face GGUF repository>

sudo chown walkrie:walkrie /var/lib/walkrie/models/bge-m3-Q4_K_M.gguf
```

Set `model_path` in `/etc/walkrie/config.toml` to match. At startup, Walkrie validates that `model_path` exists, is a regular file, is readable by the running user, and is non-empty — a missing or misconfigured model produces a clear config-validation error rather than a runtime crash.

## Configuration Syntax

```toml
[app]
log_level = "info"   # trace / debug / info / warn / error / critical
log_file  = "/var/log/walkrie/walkrie.log"
log_max_size_mb  = 10
log_max_files    = 5
batch_size       = 1    # 1 = no batching (default); group up to N events per sink call_batch()
batch_timeout_ms = 50   # max wait to fill a batch before flushing what's collected so far

[[source]]
host        = "localhost"
port        = "5432"
dbname      = "qdb"
user        = "quser"
password    = "quser1234"
slot_name   = "cdc_slot"
publication = "test_pub"

[[sink]]
type            = "postgres-embedding"
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
model_path = "/var/lib/walkrie/models/bge-m3-Q4_K_M.gguf"
dimensions = 1024
n_threads  = 4
n_ctx      = 512
```

**Note on `type` in `[[sink]]`:** if omitted, defaults to `postgres-embedding`.

### Batching (`batch_size` / `batch_timeout_ms`)

Set in `[app]` (process-wide, applies to all sinks — not per-sink config):

```toml
[app]
batch_size       = 8
batch_timeout_ms = 50
```

* `batch_size` (default `1`) — maximum number of events `EventDispatcher` groups into a single `call_batch()` invocation per sink. `1` reproduces the pre-batching one-event-at-a-time behavior exactly.
* `batch_timeout_ms` (default `50`) — maximum time to wait for a batch to fill before processing whatever's been collected so far. Ensures a lone event during a quiet period still gets processed promptly rather than waiting indefinitely for more events that may never arrive.
* Only `OpenAIProvider` currently benefits from `batch_size > 1` with a genuine reduction in HTTP round-trips (measured ~7.3× lower per-row latency at `batch_size=10` — see PERFORMANCE.md). `LlamaProvider` does not yet implement real batched computation (see Known Limitations above) — raising `batch_size` with the local provider changes DB-write batching only, not embedding call count.
* Batching groups events across **all** configured sinks in one dispatch cycle, not independently per sink — see the Isolation Layer note above if running `json-output` alongside `postgres-embedding`.

### Multiple sources into one sink table (discriminator)

When multiple source tables share a sink table and may use overlapping id values, add a static `discriminator_column`/`discriminator_label` per table mapping:

```toml
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

[[sink]]
type            = "postgres-embedding"
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
model_path = "/var/lib/walkrie/models/bge-m3-Q4_K_M.gguf"
dimensions = 1024
n_threads  = 4
n_ctx      = 512
```

**Note on `discriminator_column`:** the discriminator column must exist as part of a real unique constraint on the sink table — otherwise different mappings will generate mismatched `ON CONFLICT` targets (e.g. some as `(item_id)`, others as `(item_id, category)`), and Postgres will reject whichever ones don't match the actual constraint on the table.

### Multiple sink blocks

More than one `[[sink]]` block can be declared — e.g. a `postgres-embedding` sink alongside a `json-output` debug sink:

```toml
[[sink]]
type           = "json-output"
output_target  = "stdout"   # "stdout", "discard", or a file path

[[sink]]
type            = "postgres-embedding"
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
 # ...
```

### OpenAI provider

```toml
[embedding]
provider   = "openai"
model      = "text-embedding-3-small"
api_key    = "sk-proj-....."
dimensions = 1536
```

**Note on `dimensions`:** this value must match both the embedding model's actual output size and the sink table's `vector(N)` column width. `text-embedding-3-small` supports truncation down to any size ≤ 1536; `text-embedding-3-large` down to any size ≤ 3072; `text-embedding-ada-002` does not support truncation and must be set to exactly 1536.

## Execution

**Foreground** (recommended under systemd — see `walkrie.service`):
```bash
walkrie -f -c /etc/walkrie/config.toml
```

**Daemonized** (manual/non-systemd use — double-fork, detaches from the controlling terminal, writes a PID file):
```bash
walkrie -c /etc/walkrie/config.toml [--pid-file /run/walkrie/walkrie.pid]
```

**Graceful shutdown**, either mode: send `SIGTERM` or `SIGINT` (Ctrl-C in foreground mode). Walkrie drains the event queue and shuts down cleanly rather than terminating abruptly.

```
usage: walkrie -c <config.toml> [-f] [--pid-file <path>]

  -c, --config <path>     path to config.toml (required)
  -f, --foreground        run in the foreground; do not daemonize
                          (use this under systemd — see walkrie.service)
      --pid-file <path>   PID file path when daemonizing (default: /run/walkrie/walkrie.pid)
  -h, --help               show this message
```

## Testing

### Unit tests (`walkrie_tests`, doctest, no live DB required)

Covers WAL frame parsing, `pgoutput` message decoding, SQL builder output, config validation (including the model_path existence/readability/non-empty checks), `EventDispatcher`'s batching/timeout grouping behavior, and `PgEmbeddingSink::call_batch()`'s insert/delete-same-batch ordering guarantee via a recording test double (no real database connection needed for this specific test, since it only asserts the *order* of `upsert()`/`remove()` calls, not their SQL effects).

```bash
./walkrie_tests
```

### Integration tests (live Postgres + real embedding provider required)

`test_sink_batch_mode` verifies that `PgEmbeddingSink::call()` (single-event path) and `call_batch()` (batched path) produce **identical final database state** for the same sequence of events, across 5 deterministic scenarios — including the exact insert+delete-same-batch regression case described above. Runs against two fixed, self-managed tables (`walkrie_it_single`, `walkrie_it_batch`) rather than live WAL streaming, so it's fast and fully repeatable.

```bash
./test_sink_batch_mode <config.toml> --conninfo "<pg conninfo>" [--epsilon 1e-6]
```

See `integration_tests/README.md` for the full scenario list and an important caveat: the current near-zero equality tolerance is only valid because `LlamaProvider` doesn't yet do real batched computation (see Known Limitations) — it will need loosening once that changes.

## Vector Indexing (Operator Responsibility)

Walkrie writes to the sink table but does not currently create or manage the vector index itself — this is a manual step on the sink database:

```sql
CREATE INDEX ON test_embeddings USING hnsw (embedding vector_cosine_ops);
```

HNSW is recommended over IVFFlat for tables receiving continuous writes: IVFFlat's clustering is fixed at index-build time and degrades in recall quality as new data drifts from the original distribution, requiring periodic `REINDEX`. HNSW updates incrementally as rows are inserted, which matches Walkrie's continuous-write pattern.

## Performance

Benchmark methodology and results (lag, throughput, CPU/RAM) are tracked separately in [PERFORMANCE.md](./PERFORMANCE.md), including a breakdown of pipeline overhead vs. embedding provider latency across both local (Llama) and remote (OpenAI) providers.
