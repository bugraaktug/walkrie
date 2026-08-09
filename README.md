# Walkrie

**WAL → Knowledge Retrieval & Indexing Engine**

A low-latency PostgreSQL-to-vector sync engine that keeps your embeddings up to date as your source data changes.

Walkrie streams, validates, and syncs PostgreSQL transactional data into vector embeddings (`pgvector`, with pluggable OpenAI or local Llama backends) in real time — no batch jobs, no polling, no cron.

Built in native C++ using asynchronous `libevent` I/O and a lock-free single-producer/single-consumer (SPSC) queue, Walkrie is designed to isolate slow, unpredictable embedding API calls from your live Postgres replication stream, so a slow embedding provider never causes replication lag to build up unbounded.

## Why Walkrie

Most CDC-to-vector pipelines today are built from general-purpose tools (Debezium + Kafka + a custom consumer, or scripted polling jobs) that carry real operational cost: a mandatory message broker, WAL disk bloat from slow consumers, table locks during initial snapshots, and fragile handling of schema changes. Walkrie is a single native binary with no external message broker dependency — it reads the replication slot directly and writes to your vector sink directly, and its initial backfill scan's dump step briefly retains WAL until it finishes but never takes a table lock. **WAL disk bloat from a stalled or lagging sink isn't solved by this, though** — Walkrie only advances its confirmed replication position once a required sink durably applies a batch, so a stuck sink still grows the slot's WAL retention until it recovers, the same as any logical-replication consumer (Debezium included).

* **Lock-Free Pipeline Architecture** — Isolates network-bound embedding API calls from the core replication read path using a lock-free SPSC queue, so a slow or rate-limited embedding provider doesn't block WAL consumption.
* **Native Logical Replication Listener** — Reads directly from a PostgreSQL logical replication slot (`pgoutput`) using `libevent`'s non-blocking I/O — no polling, no external broker.
* **Pluggable Embedding Backends** — Local embeddings via `llama.cpp` (fully offline, no API costs or data leaving your infrastructure) or OpenAI's embeddings API, selectable per deployment.
* **Configurable Column Mapping** — Map specific source columns to embedding input, ID, and metadata roles per table via TOML config — no code changes needed to onboard a new table.

## MVP Feature Set

* **Real-time WAL streaming** via native logical replication slot listener.
* **Multi-table mapping** — configure multiple source tables, each with independent column mappings, in a single config file. Multiple sources can share one sink table using a static discriminator label, so overlapping IDs across tables never collide.
* **OpenAI & local Llama integrations** — switch embedding provider via a single config field.
* **Skip-unchanged & null-safety checks** — update events skip re-embedding when the source text didn't actually change (TOAST-unchanged column, or identical old/new value), and rows with a missing id or embed value are dropped before any embedding call — avoiding wasted API/inference cost on no-op updates.
* **`TRUNCATE` support** — truncating a watched table (including via `CASCADE`) is no longer a silent gap: it's decoded per relation and applied as a bulk delete against the sink table, scoped to that table's discriminator when configured (see TECHNICAL.md).
* **Optional event batching** — group multiple change events into a single batched embedding call (`batch_size`/`batch_timeout_ms` in config) instead of one call per row. Off by default (`batch_size = 1`). Both providers now implement real batched embedding: the OpenAI provider measures ~7.3× lower per-row latency at a batch size of 10 (one HTTP call instead of ten). The local Llama provider now does genuine multi-sequence `llama_encode()` batching too, but measured, its effect is hardware-dependent rather than a fixed win — ~20% *slower* per row on one machine, up to ~35% *faster* on another with the same model and batch size (see PERFORMANCE.md and TECHNICAL.md's Known Limitations for both environments' numbers and the working hypothesis why) — profile on your own deployment hardware before relying on either direction.
* **Optional GPU offload for the local Llama provider** (`n_gpu_layers` in `[embedding]`, default `0` = CPU-only) — offload some or all of the model's transformer layers to a CUDA GPU (requires building with `-DGGML_CUDA=ON`). Only pays off when paired with batching (`batch_size > 1`): on the entry-level GPU this was benchmarked against, it improved batched throughput by up to ~29% but made sequential (`batch_size = 1`) calls slower — see PERFORMANCE.md §5 and TECHNICAL.md before enabling in production.
* **Initial backfill scan for pre-existing rows** (`backfill = true` per `[[source]]`) — CDC alone only ever captures rows changed *after* a replication slot exists, so on first-ever slot creation Walkrie additionally scans and embeds whatever rows were already in each mapped table, without pausing live streaming while it does. The scan runs in a separate `walkrie_worker` process (real OS-level parallelism for the embedding calls, not just threads sharing one provider), reconciles correctly against concurrent live writes to the same rows during the scan window, and resumes cleanly across a crash of either the main process or the worker — see TECHNICAL.md for the design.
* **Upsert-based sink writes** — idempotent by design; replays and reconnects don't duplicate rows.
* **Config validation at startup** — required fields, embedding provider settings, and (for the local Llama provider) the model file's existence, type, readability, and non-zero size are all checked before the daemon starts, so misconfiguration produces a clear error message instead of a crash loop.
* **Foreground and daemon modes** — run under systemd (`-f` foreground) or as a classic detached daemon (double-fork, PID file, signal-based graceful shutdown on SIGTERM/SIGINT).

## Roadmap

* Root-cause why local Llama batching's effect on latency reverses between machines. Re-benchmarking on non-VM (bare-metal, GPU-capable) hardware is done and confirms the effect is real and hardware-dependent, not measurement noise — but *why* (dense non-causal attention over the combined ubatch vs. fixed per-call overhead amortization are the two competing working hypotheses — see TECHNICAL.md's Known Limitations) is still unconfirmed by profiling.
* Vector index management helpers (HNSW index creation/verification on sink tables).
* Multi-threaded embedding worker pool (multiple `llama_context` instances sharing one loaded model) to use more available CPU cores concurrently.

## Target Customers

* **Teams running RAG or semantic search on top of an existing Postgres database** who want embeddings to stay current without building and maintaining a custom sync pipeline.
* **Resource-constrained engineering teams** who want a single lightweight binary instead of standing up Kafka or a scripted batch job to keep a vector store in sync.

## Installation (Debian/Ubuntu)

Walkrie ships as a `.deb` package. After installing:

```bash
sudo dpkg -i walkrie_1.2.0~alpha1-1_amd64.deb
sudo apt install -f   # resolve any missing runtime dependencies
```

The package creates a dedicated `walkrie` system user, installs a systemd unit (enabled but **not started** — see below), and creates:

| Path | Purpose |
|---|---|
| `/etc/walkrie/config.toml` | Configuration file (edit this before starting) |
| `/var/lib/walkrie/models/` | Place your local embedding model (`.gguf`) file here |
| `/var/lib/walkrie/backfill/` | Per-source SQLite staging store for `backfill = true` sources (`<slot_name>.sqlite3`) — managed automatically, nothing to place here |
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

## Installation (Docker)

```bash
git submodule update --init --recursive   # if not already done
docker build -t walkrie:1.2.0-alpha1 .
```

No config is baked into the image — `config_sample.toml`'s placeholder credentials and `host = "localhost"` mean something different inside a container, so walkrie refuses to start with the same clear config-validation error described above until you bind-mount your own:

```bash
docker run --rm \
    -v "$(pwd)/config.toml:/etc/walkrie/config.toml:ro" \
    -v "$(pwd)/models:/var/lib/walkrie/models:ro" \
    -v "$(pwd)/logs:/var/log/walkrie" \
    walkrie:1.2.0-alpha1
```

* If your Postgres source/sink runs on the Docker host rather than in the same container network, point `config.toml`'s `host` at `host.docker.internal` (works out of the box with Docker Desktop on Mac/Windows) rather than `localhost`.
* The local Llama provider's `.gguf` model file still isn't bundled in the image (same licensing/size reasons as the `.deb` package) — bind-mount it from a `models/` directory as shown above.
* `docker-compose.sample.yml` in the repo root has a working starting point with these volumes pre-wired; copy it to `docker-compose.yml` and adjust.
* See [TECHNICAL.md](./TECHNICAL.md#docker-build) for what the image actually contains and why it's built the way it is.

## Security & Deployment

Walkrie runs entirely within your own infrastructure. Database credentials, replicated data, and schema details stay local to wherever you deploy the binary — nothing is sent to any third party.
If you configure the OpenAI embedding provider, only the specific text fields you've mapped for embedding are sent to OpenAI's API, under OpenAI's own data handling terms — Walkrie itself does not collect or transmit any data.


## License

Walkrie is licensed under the [Apache License, Version 2.0](./LICENSE).

Third-party dependencies (moodycamel, nlohmann/json, spdlog, toml++,
llama.cpp, libpq, libevent, libcurl) are used under their own permissive
licenses — see [THIRD-PARTY-LICENSES.md](./THIRD-PARTY-LICENSES.md) for
the full list and license texts. See also [NOTICE](./NOTICE).

---

Built by a solo developer for engineers who care about mechanical sympathy.
