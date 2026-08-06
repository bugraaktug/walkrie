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

The diagram above shows the forward (decode → sink) path only; there is also a feedback path back to Postgres — once a batch's required sinks have durably applied it, the originating source's confirmed LSN is reported back to the replication slot via a standby status update, so a restart resumes from the correct position rather than replaying already-applied data. See "LSN acknowledgment tied to durable sink writes" and "`Commit` marker for LSN correctness" below.

## Core Systems Design

### 1. Ingestion Layer (`libevent`)

* **Asynchronous event loop** using `libevent`'s non-blocking I/O (`epoll` on Linux) to read the binary `pgoutput` logical replication stream directly off the replication slot connection.
* **Non-blocking status updates** — standby status updates (keepalive acknowledgements) are sent in response to primary keepalive messages, avoiding the need for a dedicated polling thread. A periodic libevent timer (10s) additionally sends one independently, so the slot advances even during a lull with no primary keepalives. Every standby status update reports the source's `confirmed_lsn_` — the durably-sunk position (see Isolation Layer below), not merely the parsed-so-far position — and is logged at `trace` level (`PgReplicationSource::ping_update()`) with the exact LSN sent.
* **Final acknowledgment on shutdown** — the periodic timer only fires while the libevent loop is running, so it won't fire again once `SIGTERM`/`SIGINT` breaks the loop. `main.cpp` calls `ReplicationSource::flush_confirmed_lsn()` once per source right after `EventDispatcher` has fully drained, sending one last standby status update with whatever was confirmed during drain — otherwise a clean, fast shutdown landing between periodic ticks could leave Postgres's slot one confirmation behind.
* **Frame-level parsing** — the raw `CopyData` payload is parsed into `XLogData` (`'w'`) and `PrimaryKeepalive` (`'k'`) messages as a distinct, independently tested layer before any `pgoutput` message content is decoded.

### 2. Isolation Layer (Lock-Free SPSC Queue)

* **Single-producer/single-consumer queue** (`moodycamel::BlockingReaderWriterQueue`) decouples the replication read path (producer) from the embedding/sink write path (consumer).
* **Blocking consumer wait** — the worker thread blocks on the queue rather than polling, so an idle pipeline consumes negligible CPU.
* **Purpose**: an embedding API call (especially over HTTP to a remote provider) can take anywhere from milliseconds to seconds. Isolating this behind a queue means a slow embedding call doesn't block WAL consumption or risk the replication slot falling behind.
* **Optional event batching** — `EventDispatcher` can group up to `batch_size` events (bounded by `batch_timeout_ms`, so low-traffic periods still flush promptly rather than waiting indefinitely to fill a batch) into a single `call_batch()` invocation per sink, instead of dispatching one event at a time. This is what allows a provider's `embed_batch()` to turn N embedding calls into fewer, larger ones (see Sink Layer below). Default `batch_size = 1` reproduces the original one-event-at-a-time behavior exactly — batching is strictly opt-in via config.
* **Batching applies uniformly across all configured sinks in one dispatch cycle** — grouping happens once, upstream of the per-sink loop, not independently per sink. A `json-output` sink (which has no real batching work to do) still experiences the same grouping delay as a batching-aware `postgres-embedding` sink in the same job, and sinks are processed sequentially in `[[sink]]` config order within a batch — list latency-sensitive sinks first if running multiple sinks together and this matters for your use case.
* **LSN acknowledgment tied to durable sink writes, not to parsing** — `EventDispatcher` tracks the maximum `commit_lsn` seen per source across a processed batch and only fires `on_confirmed_` (which calls `set_confirmed_lsn()` on the originating source) once that batch's required sinks have durably applied it. This closes the gap where acking a WAL position the instant it was decoded meant a crash between decode and sink write caused silent data loss on restart.
* **Sink durability tiers (`required` / best-effort)** — each configured sink is either `required` (a failure blocks confirmation and triggers retry) or best-effort (a failure is logged and the batch proceeds). Default comes from `EventSink::default_required()` (`true`; `JsonSink` overrides to `false`), overridable per-sink via `required = true/false` in that sink's `[[sink]]` block. `SinkHandle{sink, required}` carries the resolved flag alongside the `shared_ptr` through `EventJob.sinks`.
* **Retry with exponential backoff, bounded by a time ceiling** — `EventDispatcher::dispatch()` iterates a batch's sinks; `dispatch_with_retry()` handles one sink's outcome. A best-effort sink that throws is logged (identified by `EventSink::name()`, e.g. `postgres-embedding`/`json-output`) and skipped immediately. A required sink that throws is retried with exponential backoff (500ms initial, ×2 multiplier) up to a 60-second total stall ceiling — both configurable via `EventDispatcher`'s constructor, though `main.cpp` currently uses the production defaults with no config-file knob yet. If a required sink is still failing once the ceiling is reached, that batch is never confirmed — its LSN is never reported to Postgres, so the affected WAL range replays on restart against (hopefully by then) a recovered sink — and the dispatcher triggers a fatal shutdown rather than looping forever or silently dropping data.
* **Fatal shutdown avoids new cross-thread libevent calls** — a required sink's ceiling breach fires `EventDispatcher`'s `on_fatal` callback (once, guarded internally) from the dispatcher's worker thread. Rather than waking the libevent thread directly (which would need `evthread_use_pthreads()` plus linking `libevent_pthreads`, neither present in this project), the callback sets a `std::atomic<bool> terminate` owned by `main.cpp`; a small dedicated libevent timer (`on_terminate_poll`, 200ms interval) polls it and calls `event_base_loopbreak()` when set — reusing the exact same shutdown path as `SIGTERM`/`SIGINT` (`on_shutdown_signal`), one shutdown path, not two. 200ms is negligible added latency for what should be a rare event.
* **Blast radius (current, v1)** — one shared `EventDispatcher` (one queue, one worker thread) serves every configured source and sink. A required sink stuck retrying stalls the whole pipeline, not just the source/sink pair that's actually failing. Per-sink queues, isolating a stuck required sink to just that sink, are a stated future direction — not yet designed or implemented.

### 3. Decode Layer (`pgoutput` Parser)

* Decodes `Begin`, `Relation`, `Insert`, `Update`, `Delete`, `Truncate`, and `Commit` messages per the PostgreSQL logical replication protocol.
* Maintains a relation (table schema) cache keyed by relation OID, since `Insert`/`Update`/`Delete`/`Truncate` messages carry only relation OIDs (and, for DML, column values), not names — the schema is sent once via a `Relation` message and cached client-side.
* Supports both `REPLICA IDENTITY DEFAULT` (key-only old-tuple data on Delete, no old-tuple on Update unless the key changed) and `REPLICA IDENTITY FULL` (complete old-tuple data on Update and Delete).
* Propagates transaction `commit_timestamp` (decoded from the `Begin` message) onto every `ChangeEvent`, used downstream for replication-lag measurement.
* **`Truncate` fan-out** — a single `Truncate` WAL message can name several relations at once (e.g. `TRUNCATE parent CASCADE`), so `PgOutputParser::parse()` returns `std::optional<std::vector<ChangeEvent>>` rather than a single event: `std::nullopt` for message types that never produce events (`Begin`/`Relation`/`Type`/`Origin`), and a vector — one `ChangeEvent::Op::Truncate` per resolvable relation — for `Insert`/`Update`/`Delete`/`Truncate`. A relation OID in a `Truncate` message that isn't in the relation cache is logged and skipped rather than throwing, so one unresolvable relation in a multi-table `CASCADE` doesn't drop the events for the others in the same message.
* **`Commit` marker for LSN correctness** — every `ChangeEvent` is stamped with the WAL position of its *own* message (`event.commit_lsn = header->wal_start`, applied uniformly in `pgreplication_source.cpp` regardless of event type), not the transaction's actual commit LSN. The `Commit` (`'C'`) message used to produce no `ChangeEvent` at all, so a transaction's real commit boundary was never captured anywhere in the pipeline. Since Postgres's logical decoding only suppresses resending a transaction on reconnect once the client has confirmed a position at or past that transaction's actual commit LSN, this meant whichever transaction was last before a shutdown always replayed on the next restart — verified via a live restart test; continuous traffic silently self-heals it, since the next transaction's higher WAL positions incidentally push the confirmed position past the previous transaction's commit, which is why this went unnoticed until an idle-then-shutdown scenario was tested. Fixed by giving `Commit` its own `ChangeEvent::Op::Commit` marker event carrying no row data — the existing uniform `commit_lsn` assignment already gives it the correct value (for a `'C'` submessage, the outer XLogData frame's position *is* the commit record's LSN), so no changes were needed to LSN plumbing itself, only to what the parser emits for that message type. The marker flows through the exact same batching/dispatch/confirm pipeline as any other event — no special-casing in `EventDispatcher` — so the existing per-batch max-LSN tracking picks it up for free. Sinks that don't care about transaction boundaries skip it explicitly: `PgEmbeddingSink::call_batch` does, at the very top of its per-event loop, before its table-mapping lookup would otherwise log a warning for every single commit (a marker's `table_name` is empty). `JsonSink` does not skip it, so its output includes one `"op":"commit"` line per transaction boundary.
* **Rolled-back transactions never reach the parser** — this is entirely Postgres's responsibility, not something Walkrie decodes or filters itself. Logical decoding buffers a transaction's changes in an in-memory reorder buffer as WAL is read; the output-plugin callbacks that produce `Begin`/`Relation`/`Insert`/`Update`/`Delete`/`Truncate`/`Commit` messages only fire once decoding reaches that transaction's actual `Commit` WAL record. A rolled-back transaction has no such record — Postgres discards its buffered changes server-side, and nothing is ever sent over the replication connection for it. There is no explicit rollback message at the `proto_version '1'` this project uses (`start_streaming()`). This would change if streaming of large in-progress transactions were ever adopted (`proto_version` 2+, `streaming = 'on'`, meant for very large transactions) — that mode can send changes before commit, and a later rollback then produces an explicit Stream Abort message the client must handle by discarding what it already received — but that mode isn't in use today.

### 4. Sink Layer

* **Config-driven column mapping** — each source table maps to a sink table with per-column roles (`id`, `embed`, `metadata`) declared in TOML, resolved once at config-load time rather than re-parsed per event.
* **Upsert-based writes** — sink writes use `INSERT ... ON CONFLICT (id) DO UPDATE`, making replays and reconnects safe without manual deduplication.
* **Skip-unchanged updates** — on `Update` events, `PgEmbeddingSink` skips the embedding call entirely if the embed column is TOAST-unchanged or textually identical to the prior value, avoiding unnecessary embedding cost on updates that didn't touch the embedded field. This check applies identically whether an event arrives via the single-event or batched path.
* **Pluggable embedding providers** — a common `EmbeddingProvider` interface abstracts over local (`llama.cpp`) and remote (OpenAI HTTP API) backends; new providers implement `init()`, `embed()`, `dimensions()`, and `name()`. Providers additionally expose `embed_batch()`, defaulting to a loop over `embed()` for providers that haven't implemented real batching — `OpenAIProvider` overrides this with a genuine single-request batched call (`"input": [text1, text2, ...]`); `LlamaProvider` now overrides it too, with a real multi-sequence `llama_encode()` call (see Known Limitations below for the measured performance and correctness findings).
* **Pluggable sink types** — a `SinkConfiguration` interface (`postgres-embedding`, `json-output`) allows multiple sink blocks in one config, each independently constructing and owning its runtime `EventSink`. `EventSink` similarly exposes `call_batch()`, defaulting to a loop over `call()`; `PgEmbeddingSink` overrides it with real batching (below).
* **Batched upsert ordering (`PgEmbeddingSink::call_batch`)** — validates every event in a batch first (skip-unchanged checks, null/empty-field checks — no database writes yet), collects everything needing an embedding into one `embed_batch()` call, then applies every resulting action (upserts *and* deletes) in a second pass, strictly preserving the batch's original event order. This ordering guarantee matters concretely: if an insert and a delete for the same row land in the same batch, applying them out of original order (e.g. running the delete immediately during validation, before a same-batch insert has been upserted) can silently resurrect a row that should have ended up deleted. The two-pass design exists specifically to prevent that. Covered by both a unit test (`test_pgembedding_sink_batch_ordering.cpp`) and a live-database integration test (see Testing below).
* **`TRUNCATE` handling** — a `Truncate` event runs a bulk `DELETE` against the sink table instead of the per-row keyed delete used for `Delete` events, since a truncated source table carries no row identifiers to key off. When the table mapping has a `discriminator_column` configured, the delete is scoped to that mapping's `discriminator_label` (`DELETE FROM <sink> WHERE <discriminator> = <label>`); without one, there is no column that identifies which sink rows came from which source table, so the delete has no `WHERE` clause at all and removes every row in the sink table — logged as a warning at execution time. `TRUNCATE ... CASCADE` on a watched table's dependents is handled the same way, one bulk delete per relation named in the WAL message (see Decode Layer above). Same two-pass ordering as upserts/deletes: a truncate is applied at its original position in the batch, not deferred or reordered relative to same-batch inserts/deletes for other tables.

### 5. Initial Backfill Scan

CDC alone only ever captures rows changed *after* a replication slot exists — pre-existing rows in a newly-mapped table never reach the sink unless backfilled separately. `backfill = true` per `[[source]]` (default `false`) opts a source into scanning and embedding whatever rows were already present, without pausing live streaming while it does.

* **Only takes effect at fresh slot creation.** Flipping `backfill = true` on for an already-running source (existing slot) does *not* retroactively trigger a scan — the semantics are "catch what existed before this slot started," not "rescan on demand."
* **`BackfillStore`** (`src/backfill_store.hpp/.cpp`) — one SQLite file per source (`<backfill_dir>/<slot_name>.sqlite3`, `backfill_dir` set in `[app]`), deliberately agnostic to `TableMapping`: a dumb `(source_table, row_id) -> row_data` blob queue with a `status` (`pending`/`claimed`; there is no `done` state — done means the row is deleted). A second table tracks per-source-table dump progress (`status`: `in_progress`/`complete`, plus a `dumped_row_count` recorded on completion). `claim_pending` is a single atomic `UPDATE ... RETURNING` (no separate select+update race); the file survives process restarts by design — its existence with incomplete rows *is* the durable "unfinished backfill" marker (see Crash-Resume below).
* **Dump phase** (`BackfillUtil::dump_all()`) — for each mapped table not yet `complete`, runs a plain `SELECT` (only the id/embed/metadata columns actually needed, not `SELECT *`) over a separate `PGconn` from the replication connection (which is mid-`COPY BOTH` once streaming starts) and stages the results as JSON blobs. **Scoped to the source's own publication** — `PgReplicationSource::filter_to_source_publication()` queries `pg_publication_tables` (reusing the source's already-connected `conn_`, no extra connection needed) and narrows the table-mapping list before `BackfillUtil` ever sees it. This matters because `[[sink.table_mapping]]` blocks are global to the sink, not scoped per source — without this filter, a `backfill=true` source would attempt to dump every table any sink maps, including ones that belong to a *different* source's publication entirely, which both fails outright (no `SELECT` permission on a table this source's role was never granted) and — even if permissions happened to allow it — would break the live-reconciliation guarantee below, since a table outside this source's publication can never generate a live event for this source to reconcile against.
* **No snapshot pinning.** Correctness instead comes from gating: live streaming (`start_streaming()`) only waits on the dump phase (Postgres→SQLite, cheap, no embedding) to finish, not the full drain — draining runs concurrently with live streaming. Two reasons this was chosen over `EXPORT_SNAPSHOT`-style pinning, both about not coupling correctness to how long the slow parts take: dump itself isn't the bottleneck (~130 rows/s vs. drain's ~57 rows/s per batch in PERFORMANCE.md's real-OpenAI backfill benchmark — the gap only widens on a rate-limited or local-inference embedding provider, potentially days for a very large table), so gating live streaming on dump alone already avoids blocking replication on drain's duration. More importantly, an exported snapshot is only valid while the transaction that exported it stays open — a crash mid-dump would invalidate it permanently, with no way to resume under the same consistent view. The per-table, `INSERT OR IGNORE`-idempotent dump this design uses instead (see Crash-Resume below) has no such constraint: each table's `SELECT` is independent, so a crash mid-dump loses at most the in-flight table, not the whole scan.
* **Live-event reconciliation** (`BackfillUtil::absorb_event(event) -> bool`, return value = "suppress dispatch to sinks") keeps a still-pending backfill row correct while both paths could touch it concurrently:
  * **Delete** — always removes the row from the store (a no-op if it wasn't backfilled) and always returns `false` (still dispatched to sinks — `EventSink::remove()` on a row never upserted is a safe no-op).
  * **Update** — merges only the *known* (non-unchanged-toast) columns from the live event into the row's *existing* stored data, so an unknown column keeps whatever a prior dump/merge captured. Returns `true` (suppress dispatch) iff the row was actually still present (pending) in the store; `false` means this id was never backfilled or has already drained, so the event flows to sinks exactly as it always would.
  * **Insert** — always `false`, no store lookup — a dump only ever captures rows that pre-date the slot's consistent point, so a live Insert can never match one.
  * **Commit/Truncate** — always `false` (`Commit` must keep flowing for LSN-confirm tracking regardless of backfill state; `Truncate`-during-backfill is a known unhandled gap).
* **Drain phase runs in a separate `walkrie_worker` process per source, not in-process threads** — the key non-obvious design decision. `LlamaProvider::embed_batch()` holds its context mutex around the *entire* call, so multiple in-process threads calling it would just serialize, zero real parallelism for the local llama.cpp path. Separate OS processes each get their own `llama_context` (no shared mutex) — llama.cpp's default mmap of the GGUF file means the read-only model weights are still shared via the page cache rather than duplicated per process, so this is genuine parallelism without the mutex problem, plus crash isolation (a fault in embedding computation can't take down live CDC streaming). `walkrie_worker`'s claim→rehydrate→dispatch→mark_done loop reuses the exact same `sink->call_batch()` path live events use (rehydrated as an `Insert`-shaped `ChangeEvent`, since Op::Insert correctly skips the toast/unchanged-value checks that only apply to updates). CLI: `-c/--config <path>` `--store <path>` `--slot <name>` (logging only) `--batch-size <n>` (default 200, rows claimed per iteration).
* **Spawn/reap** happens in `main.cpp`'s per-source setup, right after the dump succeeds and before `start_streaming()` — only if `has_pending_backfill_work()` (nothing to drain is not an error, just nothing to do). The worker binary path resolves via `/proc/self/exe` rather than `argv[0]`, since `daemonize()`'s `chdir("/")` would otherwise break a relative path in standalone-daemon mode. A non-blocking 2-second libevent timer reaps finished workers (`waitpid(WNOHANG)`) without stalling the shared event loop that's still serving every other source's replication socket; a worker exiting non-zero is logged only, no in-process retry (see Crash-Resume below for what *does* retry it).
* **Crash-resume** covers both a `walkrie_worker` crash and a full `walkrie` process crash/restart, unified around two mechanisms:
  * **Stale claim reset** — on every *resumed* `connect()` (never on a fresh slot, where the whole store is discarded anyway, see below), any row still `claimed` is unconditionally reset to `pending`. This is correct by construction in the current one-worker-per-source architecture: a `claimed` row found at connect time can only belong to a worker that died along with the previous process.
  * **Interrupted dump resumption** — a crash *during* `dump_all()` used to be silently lost: `run_backfill_dump_if_required()` only ran on a freshly-created slot, and any crash always leaves the slot already existing on restart (hitting the resume path instead). Simply always dumping on resume isn't right either — it would violate the "no retroactive scan" rule above. The fix: `mark_table_dump_started()` writes an `in_progress` marker *before* a table's `SELECT` runs, not just `complete` after — so even a crash before finishing the very first table leaves a trace, and `has_any_dump_state()` (any row in `backfill_tables` at all) becomes an accurate signal for "backfill was already active for this slot, possibly interrupted" versus "just flipped on for an untouched slot." `dump_all()` is already idempotent (skips `complete` tables, `INSERT OR IGNORE`s rows), so resuming an interrupted table just re-runs its `SELECT` safely.
  * A **freshly (re)created** slot — first-ever run, or a slot manually dropped and recreated — always resets the entire store (`BackfillStore::reset()`) rather than trusting any leftover state, since a new slot is by definition a new epoch: whatever a prior epoch's table-status/row state says has no bearing on what this slot's consistent point actually captured.

```toml
[app]
backfill_dir = "/var/lib/walkrie/backfill"   # one SQLite file per source, named <slot_name>.sqlite3

[[source]]
host        = "localhost"
port        = "5432"
dbname      = "qdb"
user        = "quser"
password    = "quser1234"
slot_name   = "cdc_backfill_slot"
publication = "backfill_pub"
backfill    = true
```

## Known Limitations

* **`LlamaProvider::embed_batch()` now does real multi-sequence embedding, but it is not currently a throughput win.** It raises `n_seq_max` in `llama_context_params` and packs each chunk of texts into one `llama_encode()` call as parallel sequences (`build_batch()` in `llama_provider.cpp`), so `batch_size > 1` now genuinely reduces the number of `llama_encode()` calls, not just DB-write grouping. Measured, though, it's ~20% *slower* per row than sequential `embed()` calls on this project's benchmark hardware (see PERFORMANCE.md §5) — the opposite of what batching does for `OpenAIProvider`. Working hypothesis: llama.cpp's non-causal encode computes a dense attention matrix over the *combined* ubatch (all sequences together), so attention cost scales with `(combined tokens)²` rather than `Σ(tokens per sequence)²` — a real cost that can outweigh the GEMM efficiency gained in the QKV/FFN projections, especially for the short text this project benchmarks with. Not confirmed via profiling; flagged as a follow-up. Also measured on a single resource-constrained VM against a `llama.cpp` checkout still under active upstream development, so treat the specific numbers as a snapshot, not a final verdict — this is a real, working milestone (multi-sequence Llama batching is implemented and correct, see below), just not yet a proven performance win.
* **Batched and sequential calls do *not* produce bit-identical embeddings for the same input — confirmed, and quantization-dependent.** With `bge-m3-Q4_K_M.gguf`, `embed()` vs. `embed_batch()` output for the same text diverges well past a near-zero tolerance. With `bge-m3-Q8_0.gguf`, the two match within the integration test's epsilon. Likely explanation: `Q8_0` uses flat per-block int8 scaling close to the original F16 weights, so its own baseline quantization error is small; `Q4_K_M`'s k-quant format uses hierarchical per-sub-block scales at mixed low bit-width, carrying substantially more baseline quantization error. The floating-point summation/tiling order genuinely differs between a batched GEMM (`embed_batch()`) and an independent per-sequence GEMV (`embed()`) regardless of quantization (a well-documented phenomenon in comparable transformer implementations, see `integration_tests/README.md`'s cited references) — `Q8_0`'s small baseline error absorbs that perturbation without exceeding tolerance, `Q4_K_M`'s larger baseline error does not.
  * **Practical takeaway**: use `Q8_0` (or a higher-precision quantization) rather than `Q4_K_M` if your deployment mixes `embed_batch()` and `embed()` calls (i.e. `batch_size > 1`) and embedding consistency across that boundary matters — e.g. the same row re-embedded via a different code path should land near-identical, not measurably different.
  * **To reproduce this yourself**: run `test_sink_batch_mode` (see Testing below) once per model file and compare the cross-comparison epsilon result — `Q4_K_M` should fail a tight `--epsilon`, `Q8_0` should pass. A quick manual `embed()` vs. `embed_batch()` diff check (per-element max-abs-diff, first-N-values printout) is also kept, commented out, near the top of `main()` in `src/tests/integration/test_sink_batch_mode.cpp` if you want to inspect raw values against your own model rather than just a pass/fail.

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
sudo dpkg -i walkrie_1.2.0~alpha1-1_amd64.deb
sudo apt install -f
```

This installs `/usr/bin/walkrie` and `/usr/bin/walkrie_worker` (the latter spawned automatically by `walkrie` to drain `backfill = true` sources, resolved from the same directory — see Spawn/Reap below), a config template to `/etc/walkrie/config.toml`, a systemd unit (enabled, not auto-started), and creates `/var/lib/walkrie/models/`, `/var/lib/walkrie/backfill/` (per-source SQLite staging stores, see Initial Backfill Scan below), and `/var/log/walkrie/`. See the README's [Installation](./README.md#installation-debianubuntu) section for the post-install steps (model placement, config editing, service start).

### Model Installation

The local Llama provider requires a GGUF-format embedding model. The package does not bundle one (models are large and licensed separately from Walkrie itself). To use `BGE-M3` (the model used throughout this project's benchmarks — see PERFORMANCE.md):

```bash
# Example — adjust source/filename to whichever GGUF quantization you want.
sudo -u walkrie wget -O /var/lib/walkrie/models/bge-m3-Q4_K_M.gguf \
    <URL to your chosen GGUF file, e.g. from a Hugging Face GGUF repository>

sudo chown walkrie:walkrie /var/lib/walkrie/models/bge-m3-Q4_K_M.gguf
```

Set `model_path` in `/etc/walkrie/config.toml` to match. At startup, Walkrie validates that `model_path` exists, is a regular file, is readable by the running user, and is non-empty — a missing or misconfigured model produces a clear config-validation error rather than a runtime crash.

## Docker Build

`Dockerfile` (repo root) is a two-stage build:

1. **`build` stage** (`debian:12-slim` + build toolchain) — `cmake --build build --target walkrie`, building only the `walkrie` binary target (not `walkrie_tests`/benches/demo, which the image doesn't need). `llama.cpp`/`ggml` are statically linked (`-DBUILD_SHARED_LIBS=OFF`, same as `packaging/debian/rules`), so nothing from `third_party/llama.cpp` needs to exist at runtime.
2. **`runtime` stage** (`debian:12-slim` + `libpq5`/`libevent-2.1-7`/`libcurl4`/`ca-certificates` only) — copies just the compiled binary out of the build stage, runs as a dedicated non-root `walkrie` system user, same UID-isolation intent as the systemd unit's `User=walkrie`.

The build stage needs `third_party/llama.cpp` already checked out (`git submodule update --init --recursive`) — Docker doesn't resolve git submodules on its own, and the Dockerfile fails fast with an explicit message rather than a confusing CMake error if that step was skipped.

**No config is copied into `/etc/walkrie/config.toml`, on purpose** — unlike the `.deb` package, which does install a starter template there. `config_sample.toml` carries a plaintext placeholder password and a `host = "localhost"` that resolves to the container itself, not a useful default inside a container network. Baking that in risks someone starting the container without noticing it silently isn't pointed at their real database. Instead, `/etc/walkrie` is a declared `VOLUME`, and the image relies on the same startup config validation described throughout this document to fail loudly if nothing's bind-mounted there. The pristine reference copy is still shipped, at `/usr/share/doc/walkrie/config.toml.example`, matching where the `.deb` package's `config.toml.example` lives.

Local model files (`/var/lib/walkrie/models`) and logs (`/var/log/walkrie`) are declared volumes for the same reason the `.deb` package doesn't bundle a model — see Model Installation above; bind-mount a `models/` directory containing your `.gguf` file rather than expecting one to be present.

See the README's [Docker installation](./README.md#installation-docker) section for build/run commands and `docker-compose.sample.yml` for a working starting point.

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
validate   = "force"   # "none" (default) | "warn" | "force" — see note below
```

**Note on `validate`** (llama provider only): at startup, `AppConfig::validate()` can cross-check `model_path` against `dimensions` by reading the GGUF file's own declared embedding size (`GgufMetadataReader`, `src/model/`) — catching a `dimensions` mismatch or a corrupt/wrong model file before the daemon starts, rather than failing later inside `LlamaProvider::init()`. This costs a small file read (header + KV metadata only, never the multi-GB tensor blob — see `src/model/gguf_metadata_reader.hpp`), so it's opt-in via `validate`:
* `"none"` (default) — skip the check entirely; `model_path` is not even opened for this purpose. Matches trusted setups (tests, benchmarks, CI) where the config is already known correct and the extra file open is unnecessary cost.
* `"warn"` — run the check; on a mismatch or an invalid GGUF file, log via `spdlog::error` and continue starting up.
* `"force"` — run the check; on a mismatch or an invalid GGUF file, add to `validate()`'s error list, which callers treat as a reason to refuse to start — the same treatment as the existing `model_path` existence/readability/size checks.

**Note on `type` in `[[sink]]`:** if omitted, defaults to `postgres-embedding`.

**Note on `required` in `[[sink]]`:** if omitted, defaults per sink type — `true` for `postgres-embedding`, `false` for `json-output`. A `required` sink's failures are retried with backoff and can trigger a fatal shutdown; a best-effort (`required = false`) sink's failures are logged and skipped. See "Sink durability tiers" in the Isolation Layer section above.

### Batching (`batch_size` / `batch_timeout_ms`)

Set in `[app]` (process-wide, applies to all sinks — not per-sink config):

```toml
[app]
batch_size       = 8
batch_timeout_ms = 50
```

* `batch_size` (default `1`) — maximum number of events `EventDispatcher` groups into a single `call_batch()` invocation per sink. `1` reproduces the pre-batching one-event-at-a-time behavior exactly.
* `batch_timeout_ms` (default `50`) — maximum time to wait for a batch to fill before processing whatever's been collected so far. Ensures a lone event during a quiet period still gets processed promptly rather than waiting indefinitely for more events that may never arrive.
* `batch_size` also drives `EmbeddingConfig::max_batch_size` internally (`cfg.embedding.max_batch_size = cfg.settings.batch_size` in `config.hpp`'s `load_config()`) — there's no separate embedding-specific batch-size field to set. `OpenAIProvider` benefits from `batch_size > 1` with a genuine reduction in HTTP round-trips (measured ~7.3× lower per-row latency at `batch_size=10` — see PERFORMANCE.md §5). `LlamaProvider` now also does real batched computation (see Known Limitations above) — but measured, it's currently a ~20% per-row *slowdown* rather than a win, unlike OpenAI's result.
* Batching groups events across **all** configured sinks in one dispatch cycle, not independently per sink — see the Isolation Layer note above if running `json-output` alongside `postgres-embedding`.
* **`[app] batch_size` and `walkrie_worker --batch-size` are unrelated despite the name.** `[app] batch_size` only governs `EventDispatcher`'s grouping of *live* events — backfill never goes through `EventDispatcher` at all, it's driven entirely by `--batch-size` (see Initial Backfill Scan below).

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

**Note on `TRUNCATE`:** the discriminator also scopes what a `TRUNCATE` on a mapped source table deletes from the shared sink table. If `test_table` above is truncated, only rows with `category = 'test'` are removed — `documents`' rows are untouched. If a mapping shares a sink table with others but has no `discriminator_column` configured, a `TRUNCATE` on that mapping's source table deletes **every** row in the sink table, including rows written by the other mappings — configure a discriminator on every mapping that shares a sink table if this matters for your deployment.

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

The sink-table half of that is checked unconditionally, for every provider, every startup — `PgEmbeddingSink::init()` (`src/pgembedding_sink.cpp`) queries `pg_attribute.atttypmod` for the configured `sink_column` right after confirming the `vector` extension is installed, and compares it against `provider_->dimensions()` (the embedding provider's actual, already-loaded output size — not the config value one hop removed from it). pgvector enforces an *exact* dimension match on every insert regardless of direction (a vector shorter **or** longer than the column's declared width both fail with pgvector's own `expected N dimensions, not M` error — there's no varchar-style truncation/padding for `vector(N)`), so this is a strict equality check. A mismatch throws at startup with both numbers named, rather than failing on the first upsert. If `sink_column` was declared as a bare `vector` with no dimension (`atttypmod = -1`), there's nothing to compare against and the check is silently skipped. Unlike the GGUF `[embedding] validate` check above, this one isn't gated behind a config flag — it's a single indexed catalog lookup against a connection already being opened, not a model file open, so the cost argument for making it opt-in doesn't apply here.

## Execution

**Foreground** (recommended under systemd — see `walkrie.service`):
```bash
walkrie -f -c /etc/walkrie/config.toml
```

**Daemonized** (manual/non-systemd use — double-fork, detaches from the controlling terminal, writes a PID file):
```bash
walkrie -c /etc/walkrie/config.toml [--pid-file /run/walkrie/walkrie.pid]
```

**Graceful shutdown**, either mode: send `SIGTERM` or `SIGINT` (Ctrl-C in foreground mode), or automatically if a required sink stalls past its retry ceiling (see Isolation Layer above). Walkrie drains the event queue, sends one final standby status update per source with the fully-drained confirmed LSN, and shuts down cleanly rather than terminating abruptly.

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

Covers WAL frame parsing, `pgoutput` message decoding (including `Truncate` — single-relation, multi-relation `CASCADE` fan-out, and the skip-unknown-relation case — and `Commit`, which yields exactly one `Op::Commit` marker event carrying the transaction's real commit LSN), SQL builder output (including `build_truncate_sql`'s discriminator-scoped vs. whole-table-delete forms), config validation (including the model_path existence/readability/non-empty checks), `EventDispatcher`'s batching/timeout grouping behavior and its required/best-effort sink retry-and-fatal-shutdown gating (a required sink that recovers within its retry ceiling still confirms; one that never recovers triggers `on_fatal` exactly once and withholds confirmation for that batch; a best-effort sink never retries), and `PgEmbeddingSink::call_batch()`'s insert/delete-same-batch and truncate ordering guarantees via a recording test double (no real database connection needed for this specific test, since it only asserts the *order* of `upsert()`/`remove()`/`truncate()` calls, not their SQL effects).

```bash
./walkrie_tests
```

### Integration tests (live Postgres + real embedding provider required)

`test_sink_batch_mode` verifies that `PgEmbeddingSink::call()` (single-event path) and `call_batch()` (batched path) produce **identical final database state** for the same sequence of events, across 5 deterministic scenarios — including the exact insert+delete-same-batch regression case described above. Runs against two fixed, self-managed tables (`walkrie_it_single`, `walkrie_it_batch`) rather than live WAL streaming, so it's fast and fully repeatable.

```bash
./test_sink_batch_mode <config.toml> --conninfo "<pg conninfo>" [--epsilon 1e-6]
```

See `src/tests/integration/README.md` for the full scenario list. `LlamaProvider` now does real batched computation, so the near-zero default `--epsilon` no longer holds universally — it holds for `Q8_0`-class quantizations (confirmed) but not for `Q4_K_M` (confirmed divergent) — see Known Limitations above before picking a model + epsilon combination for CI.

`test_sink_dims_check` verifies `PgEmbeddingSink::verify_sink_column_dimensions()` — the startup check described in the "Note on `dimensions`" above — across 4 cases: matching dims (no throw), mismatched dims (throws, naming both values), an unconstrained `vector` column with no declared dimension (skipped, no throw), and a missing sink column (throws). Needs a real `pg_attribute` lookup, so it's an integration test too, not a doctest case. Uses its own dedicated tables (`walkrie_it_dims_*`) and config (`config_sample_dims_check_test.toml`), independent of `test_sink_batch_mode`'s.

```bash
./test_sink_dims_check <config.toml> --conninfo "<pg conninfo>"
```

`test_lsn_confirm` verifies the LSN-correctness fix described in the Decode Layer section above, directly against a live Postgres: it drives the real `PgReplicationSource` through one transaction on a dedicated table/publication/slot (`walkrie_it_lsn_*`, dropped and recreated each run so it never collides with a real running instance), waits for the `Op::Commit` marker, confirms it exactly as `EventDispatcher` would after a successful sink write, calls `flush_confirmed_lsn()` (the same call `main.cpp` makes on shutdown), and asserts `pg_replication_slots.confirmed_flush_lsn` reaches that transaction's real commit LSN — the exact invariant that prevents the replay-on-restart bug. Includes a "before" sanity check (the confirmed position is still behind the commit LSN prior to the flush call) so the main assertion isn't vacuous. Uses the `[[source]]` block from the given config (host/port/dbname/user/password), not `--conninfo`, since the test exercises actual replication-slot mechanics.

```bash
./test_lsn_confirm <config.toml> [--slot NAME] [--publication NAME] [--table NAME]
```

`test_backfill_dump` verifies the Initial Backfill Scan design end to end against a live Postgres: fresh-slot dump correctness, a live `Update` on a still-pending row merging into the store and suppressing dispatch, a resumed slot *not* re-dumping, a manually dropped-and-recreated slot re-dumping despite stale state from the prior epoch (the crash-resume fix), and a dump interrupted mid-table (simulated via direct SQLite manipulation) being finished rather than silently skipped on the next resume.

```bash
./test_backfill_dump <config.toml> [--slot NAME] [--publication NAME] [--table NAME] [--conninfo "<pg conninfo>"]
```

`test_backfill_worker_drain` verifies `BackfillWorker::run()` end to end (real claim → embed → upsert → mark_done) against a live Postgres + real embedding provider, seeding the store directly to isolate the drain loop from the dump phase (covered separately above).

```bash
./test_backfill_worker_drain <config.toml> [--sink-table NAME] [--conninfo "<pg conninfo>"]
```

Two bench tools exercise the backfill path under load — see PERFORMANCE.md §§6–7 for results: `embed_backfill_batched_bench` forks N `walkrie_worker` processes against one seeded store, isolating `BackfillStore::claim_pending`'s cross-process concurrency behavior from a raw single-process `embed_batch()` baseline; `backfill_bench` drives the real end-to-end path (real `dump_all()`, real spawned `walkrie_worker`, optional concurrent live-load overlap) with no synthetic batching, and is what OpenAI-scale (e.g. 10k-row) backfill numbers in this repo come from.

## Vector Indexing (Operator Responsibility)

Walkrie writes to the sink table but does not currently create or manage the vector index itself — this is a manual step on the sink database:

```sql
CREATE INDEX ON test_embeddings USING hnsw (embedding vector_cosine_ops);
```

HNSW is recommended over IVFFlat for tables receiving continuous writes: IVFFlat's clustering is fixed at index-build time and degrades in recall quality as new data drifts from the original distribution, requiring periodic `REINDEX`. HNSW updates incrementally as rows are inserted, which matches Walkrie's continuous-write pattern.

## Performance

Benchmark methodology and results (lag, throughput, CPU/RAM) are tracked separately in [PERFORMANCE.md](./PERFORMANCE.md), including a breakdown of pipeline overhead vs. embedding provider latency across both local (Llama) and remote (OpenAI) providers, and separate sections on the Initial Backfill Scan's worker-scaling behavior (local Llama) and real end-to-end throughput (OpenAI).
