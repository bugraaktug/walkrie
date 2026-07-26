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
* **Skip-unchanged & null-safety checks** — update events skip re-embedding when the source text didn't actually change (TOAST-unchanged column, or identical old/new value), and rows with a missing id or embed value are dropped before any embedding call — avoiding wasted API/inference cost on no-op updates.
* **Optional event batching** — group multiple change events into a single batched embedding call (`batch_size`/`batch_timeout_ms` in config) instead of one call per row. Measured ~7.3× lower per-row latency with the OpenAI provider at a batch size of 10 (see PERFORMANCE.md). Off by default (`batch_size = 1`); the local Llama provider does not yet implement real batched computation under the hood (see TECHNICAL.md's Known Limitations) — full multi-sequence Llama batching is the next roadmap item.
* **Upsert-based sink writes** — idempotent by design; replays and reconnects don't duplicate rows.
* **Config validation at startup** — required fields, embedding provider settings, and (for the local Llama provider) the model file's existence, type, readability, and non-zero size are all checked before the daemon starts, so misconfiguration produces a clear error message instead of a crash loop.
* **Foreground and daemon modes** — run under systemd (`-f` foreground) or as a classic detached daemon (double-fork, PID file, signal-based graceful shutdown on SIGTERM/SIGINT).

## Roadmap

* Real batched embedding computation for the local Llama provider (multi-sequence `llama_encode()` via `n_seq_max`) — the dispatcher/config-level batching support already exists and is measured working end-to-end with OpenAI; Llama-side batching is the next step to get the same throughput win locally.
* Vector index management helpers (HNSW index creation/verification on sink tables).
* Multi-threaded embedding worker pool (multiple `llama_context` instances sharing one loaded model) to use more available CPU cores concurrently.

## Target Customers

* **Teams running RAG or semantic search on top of an existing Postgres database** who want embeddings to stay current without building and maintaining a custom sync pipeline.
* **Resource-constrained engineering teams** who want a single lightweight binary instead of standing up Kafka or a scripted batch job to keep a vector store in sync.

## Installation (Debian/Ubuntu)

Walkrie ships as a `.deb` package. After installing:

```bash
sudo dpkg -i walkrie_1.0.1~alpha1-1_amd64.deb
sudo apt install -f   # resolve any missing runtime dependencies
```

The package creates a dedicated `walkrie` system user, installs a systemd unit (enabled but **not started** — see below), and creates:

| Path | Purpose |
|---|---|
| `/etc/walkrie/config.toml` | Configuration file (edit this before starting) |
| `/var/lib/walkrie/models/` | Place your local embedding model (`.gguf`) file here |
| `/var/log/walkrie/` | Log output |
| `/run/walkrie/walkrie.pid` | PID file (systemd-managed) |

**The package does not bundle a model file** — it must be downloaded separately (models are multi-gigabyte and licensed independently of Walkrie). See [TECHNICAL.md](./TECHNICAL.md#model-installation) for download instructions.

Before starting the service:
1. Place a compatible GGUF model at `/var/lib/walkrie/models/` (if using the local Llama provider), and ensure it's readable by the `walkrie` user.
2. Edit `/etc/walkrie/config.toml` with real database credentials and (if applicable) your model path or OpenAI API key.
3. Start the service:
   ```bash
   sudo systemctl start walkrie
   sudo systemctl status walkrie
   journalctl -u walkrie -f
   ```

If `config.toml` is invalid or the model file is missing/unreadable, `walkrie` will refuse to start and log a clear, specific error rather than crash-looping.

## Security & Deployment

Walkrie runs entirely within your own infrastructure. Database credentials, replicated data, and schema details stay local to wherever you deploy the binary — nothing is sent to any third party.
If you configure the OpenAI embedding provider, only the specific text fields you've mapped for embedding are sent to OpenAI's API, under OpenAI's own data handling terms — Walkrie itself does not collect or transmit any data.

---

Licensed under a Developer-First Commercial License. Built by a solo developer for engineers who care about mechanical sympathy.
