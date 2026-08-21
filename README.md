# Walkrie

**WAL → Knowledge Retrieval & Indexing Engine**

A low-latency PostgreSQL-to-vector sync engine that keeps your embeddings up to date as your source data changes.

Walkrie streams, validates, and syncs PostgreSQL transactional data into vector embeddings (`pgvector`, Qdrant, or Milvus, with pluggable OpenAI or local Llama embedding backends) in real time — no batch jobs, no polling, no cron.

Built in native C++ using asynchronous `libevent` I/O and a lock-free single-producer/single-consumer (SPSC) queue, Walkrie is designed to isolate slow, unpredictable embedding API calls from your live Postgres replication stream, so a slow embedding provider never causes replication lag to build up unbounded.

## Why Walkrie

Most CDC-to-vector pipelines today are built from general-purpose tools (Debezium + Kafka + a custom consumer, or scripted polling jobs) that carry real operational cost: a mandatory message broker, table locks during initial snapshots, and fragile handling of schema changes. Walkrie is a single native binary with no external message broker dependency — it reads the replication slot directly and writes to your vector sink directly, and its initial backfill scan's dump step briefly retains WAL until it finishes but never takes a table lock.

Like any logical-replication consumer (Debezium included), Walkrie's replication slot retains WAL until a stalled or lagging sink recovers — it only advances its confirmed position once a required sink durably applies a batch, which is what makes that guarantee crash-safe in the first place. This is inherent to how logical replication works, not a gap specific to Walkrie.

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
* **Qdrant sink** — write embeddings to a Qdrant collection over REST instead of (or alongside — dual-write is supported) `pgvector`, using the same table-mapping, batching, and discriminator-scoped truncate config as the `pgvector` sink. Source ids are mapped to Qdrant's required point-id format via a fixed-namespace UUIDv5. See TECHNICAL.md for config and details.
* **Milvus sink** — same idea over Milvus's REST API v2, dual-write-compatible with `pgvector`/`qdrant`. The collection's primary-key and vector fields are auto-discovered at startup rather than configured; source ids are hashed the same way as the Qdrant sink. See TECHNICAL.md for config and details.
* **Optional event batching** — group multiple change events into a single embedding call instead of one call per row (`batch_size`/`batch_timeout_ms` in config, off by default). Throughput effect is provider- and hardware-dependent — see PERFORMANCE.md for measured numbers before enabling in production.
* **Optional GPU offload for the local Llama provider** (`n_gpu_layers` in config, requires building with `-DGGML_CUDA=ON`) — offload model layers to a CUDA GPU. Whether this helps depends on your hardware and whether batching is also enabled — see PERFORMANCE.md.
* **LoRA adapter support for the local Llama provider** (`lora_path`/`lora_scale` in config) — load a task-specific LoRA adapter alongside the base GGUF model, for instruction-tuned multilingual models like `jina-embeddings-v3` that split retrieval-query vs. retrieval-passage behavior into separate adapter files. See TECHNICAL.md for config details and the multilingual e-commerce demo below for a full worked example.
* **Initial backfill scan for pre-existing rows** (`backfill = true` per `[[source]]`) — scans and embeds rows that already existed in a mapped table when its replication slot was first created, without pausing live streaming, and resumes cleanly across a crash. See TECHNICAL.md for the design.
* **Upsert-based sink writes** — idempotent by design; replays and reconnects don't duplicate rows.
* **Config validation at startup** — required fields, embedding provider settings, and (for the local Llama provider) the model file's existence, type, readability, and non-zero size are all checked before the daemon starts, so misconfiguration produces a clear error message instead of a crash loop.
* **Foreground and daemon modes** — run under systemd (`-f` foreground) or as a classic detached daemon (double-fork, PID file, signal-based graceful shutdown on SIGTERM/SIGINT).

## Roadmap

* Benchmark local Llama batching and GPU offload across more hardware — results vary significantly by machine so far (see PERFORMANCE.md), and more data points are needed before drawing firm conclusions.
* Multi-threaded embedding worker pool (multiple `llama_context` instances sharing one loaded model) — unverified: needs hardware with CPU cores to spare beyond what a single `embed()` call's `n_threads` already consumes to show a real throughput gain rather than just thread contention. Mainly a memory win over today's separate-process approach (one shared model load instead of N) until benchmarked on suitable hardware.

## Target Customers

* **Teams running RAG or semantic search on top of an existing Postgres database** who want embeddings to stay current without building and maintaining a custom sync pipeline.
* **Resource-constrained engineering teams** who want a single lightweight binary instead of standing up Kafka or a scripted batch job to keep a vector store in sync.

## Build from Source

```bash
git clone --recurse-submodules https://github.com/bugraaktug/walkrie.git
cd walkrie
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

Requires system dev packages: `libpq`, `libevent`, `spdlog`, `toml++`,
`nlohmann-json`, `libcurl`, `uuid-dev`, `sqlite3` (≥3.35.0, or let CMake
fetch/build it — see below). Add `-DGGML_CUDA=ON` for GPU offload support.
See [TECHNICAL.md](./TECHNICAL.md#compilation-from-source) for the full
prerequisite list and platform-specific notes (e.g. RHEL/Rocky 9's system
sqlite3 being too old).

## Installation (Debian/Ubuntu)

Walkrie ships as a `.deb` package. After installing:

```bash
sudo dpkg -i walkrie_1.2.2-1_amd64.deb
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

## Installation (Rocky Linux / RHEL / Fedora-family)

Download the `.rpm` from the [GitHub Releases page](https://github.com/bugraaktug/walkrie/releases), or build your own (see below):

```bash
sudo dnf install ./walkrie-1.2.2-1.el10.x86_64.rpm   # resolves runtime deps automatically
```

Same user, paths (see the table above), and systemd unit as the `.deb` package — post-install steps (model placement, config editing, service start) are identical.

RPM packaging lives in `packaging/rpm/` — see [TECHNICAL.md](./TECHNICAL.md#installation-rpm-package) for how to build one yourself (e.g. targeting a different RHEL/Rocky/Fedora major version) and the required build dependencies.

Post-install steps (model placement, config editing, service start) are the same as the `.deb` instructions above — same paths, same systemd unit.

## Installation (Docker)

```bash
git submodule update --init --recursive   # if not already done
docker build -t walkrie:1.2.2 .
```

No config is baked into the image — `config_sample.toml`'s placeholder credentials and `host = "localhost"` mean something different inside a container, so walkrie refuses to start with the same clear config-validation error described above until you bind-mount your own:

```bash
docker run --rm \
    -v "$(pwd)/config.toml:/etc/walkrie/config.toml:ro" \
    -v "$(pwd)/models:/var/lib/walkrie/models:ro" \
    -v "$(pwd)/logs:/var/log/walkrie" \
    walkrie:1.2.2
```

* If your Postgres source/sink runs on the Docker host rather than in the same container network, point `config.toml`'s `host` at `host.docker.internal` (works out of the box with Docker Desktop on Mac/Windows) rather than `localhost`.
* The local Llama provider's `.gguf` model file still isn't bundled in the image (same licensing/size reasons as the `.deb` package) — bind-mount it from a `models/` directory as shown above.
* `docker-compose.sample.yml` in the repo root has a working starting point with these volumes pre-wired; copy it to `docker-compose.yml` and adjust.
* See [TECHNICAL.md](./TECHNICAL.md#docker-build) for what the image actually contains and why it's built the way it is.

## Security & Deployment

Walkrie runs entirely within your own infrastructure. Database credentials, replicated data, and schema details stay local to wherever you deploy the binary — nothing is sent to any third party.
If you configure the OpenAI embedding provider, only the specific text fields you've mapped for embedding are sent to OpenAI's API, under OpenAI's own data handling terms — Walkrie itself does not collect or transmit any data.

## Try It: Search Demos

The fastest way to see walkrie actually work — not just read about it — is
one of the worked examples in [`demo/`](./demo/README.md): seed a Postgres
table, run walkrie against it with local (offline) embeddings (including an
initial backfill of the pre-existing rows), and query it with a small
hybrid semantic + SQL search CLI. Both walk through every step, including
the couple of Postgres/backfill gotchas that trip people up on a first run,
and end with inserting a live row and watching it show up in search results
with no batch job or manual sync step.

* **[CV / HR search](./demo/README.md)** — synthetic English CVs, `BGE-M3`.
* **[Multilingual e-commerce search](./demo/README_ecommerce.md)** — synthetic Japanese/Turkish product catalog, `jina-embeddings-v3` with LoRA task adapters (see the LoRA feature above) — a concrete example of embedding languages other than English.

## License

Walkrie is licensed under the [Apache License, Version 2.0](./LICENSE).

Third-party dependencies (moodycamel, nlohmann/json, spdlog, toml++,
llama.cpp, libpq, libevent, libcurl) are used under their own permissive
licenses — see [THIRD-PARTY-LICENSES.md](./THIRD-PARTY-LICENSES.md) for
the full list and license texts. See also [NOTICE](./NOTICE).

Developed with AI assistance via [Claude Code](https://claude.com/claude-code).

---

