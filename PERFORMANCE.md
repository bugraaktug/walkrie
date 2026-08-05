# Walkrie Performance

This document tracks benchmark results for Walkrie's replication and sink pipeline, measured with the `walkrie_bench`, `embedding_bench`, `embed_backfill_batched_bench`, and `backfill_bench` binaries included in this repo. Numbers here are reproducible on the hardware/config noted per section — they are not universal claims about performance on other hardware.

All benchmarks were run locally, source and sink on the same machine (`localhost`), so lag figures are not affected by network latency or clock skew between hosts.

## Methodology

* **`walkrie_bench`** wraps the configured sink in a lag-recording decorator and samples process CPU time (`getrusage`) and RSS (`/proc/self/status`) every 200ms on a background thread, independent of the pipeline's own threads.
* **Lag** is measured as `(time the sink finished processing the row) - (commit_timestamp of the transaction that produced it)`, in milliseconds. This isolates end-to-end pipeline lag, not just decode time.
* **CPU%** is expressed as a percentage of one core's wall-clock time, and can exceed 100% when multiple threads (replication read loop, dispatcher worker, embedding inference) are simultaneously active — it is not capped at 100%.
* Each run generates load via one of two SQL scripts (see `bench/` in the repo):
  * **Single-transaction load**: `INSERT ... SELECT generate_series(1, N)` as one transaction. Fast to generate, but produces one shared `commit_timestamp` for all N rows — the resulting "lag" mostly measures Walkrie's own internal queue drain time, not real replication lag, since Postgres streams all N row messages in one post-commit burst.
  * **Batched load**: N rows split into multiple transactions committed separately (batch size noted per result). This produces a realistic spread of commit timestamps.

**Harness correction**: early `walkrie_bench` runs used a completion check in the wrong thread that never fired correctly, requiring a manual Ctrl-C and silently including that idle wait time in reported "wall time." This was fixed by moving the completion check to a libevent timer running on the event loop's own thread, and by splitting the report into `total wall time` (includes one-time connection/slot setup) and `pure processing time` (first row processed → last row processed). All results in **Section 3** below were captured after this fix; `pure processing time` / `processing throughput` are the figures to trust for steady-state comparisons.

## Environment

All benchmarks in this document were run inside a VirtualBox VM (host: Windows, Intel Core Ultra 7 155H). AVX2 is passed through to the guest (`--cpu-profile host`, required for the local Llama numbers in Section 2 — see that section's AVX2 finding). "Disk" reflects the VM's virtual disk, backed by the host's physical storage, not a directly-attached device.

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H, 8 vCPUs assigned to guest, 1 thread/core |
| RAM | 7.8 GiB (VM-assigned) |
| Disk | VirtualBox virtual disk, 250 GB (`VBOX HARDDISK`) |
| OS | Debian GNU/Linux 12 (bookworm), kernel 6.1.0-10-amd64 |
| PostgreSQL | 15.18 (Debian 15.18-0+deb12u1) |
| Walkrie build | `RelWithDebInfo` (CMakeLists.txt default; not explicitly overridden) |

## 1. Pipeline baseline (json-output sink, `discard` target)

Purpose: measure the cost of WAL decode + dispatch + serialization alone, with no database write and no embedding call — the floor the rest of the pipeline is added on top of.

**Config**: `sink.type = "json-output"`, `output_target = "discard"`

### Single-transaction load (1,000,000 rows, one commit)

| Metric | Value |
|---|---|
| Events processed | 1,000,000 |
| Wall time | 44.29 s |
| Throughput | 22,579.7 events/sec |
| Lag min | 588.2 ms |
| Lag avg | 2,926.8 ms |
| Lag p50 | 3,083.7 ms |
| Lag p95 | 4,598.0 ms |
| Lag p99 | 4,677.7 ms |
| Lag max | 4,697.7 ms |
| Avg CPU | 12.6% |
| Peak CPU | 160.1% |
| Avg RSS | 180.2 MB |
| Peak RSS | 219.2 MB |

**Note on lag in this run**: because all 1,000,000 rows share one `commit_timestamp` (Postgres streams the entire post-commit burst at once for a single-transaction load), these lag figures primarily reflect internal queue drain order, not real per-transaction replication lag. Throughput and resource figures are the meaningful takeaways from this run; see the batched-load results below for a realistic lag figure.

### Batched load (1,000,000 rows, 1,000 transactions of 1,000 rows each)

| Metric | Value |
|---|---|
| Events processed | 1,000,000 |
| Wall time | 107.20 s |
| Throughput | 9,328.6 events/sec |
| Lag min | 2.1 ms |
| Lag avg | 8.6 ms |
| Lag p50 | 7.9 ms |
| Lag p95 | 12.6 ms |
| Lag p99 | 42.8 ms |
| Lag max | 93.1 ms |
| Avg CPU | 8.3% |
| Peak CPU | 40.8% |
| Avg RSS | 20.8 MB |
| Peak RSS | 25.3 MB |

This is the trustworthy lag figure: with each transaction committing separately, lag reflects real time from Postgres commit to Walkrie finishing its (discard-mode) sink call — sub-10ms at the median, under 100ms even at the tail. Throughput is lower than the single-transaction run (9.3K vs 22.6K events/sec) because Postgres itself is now the bottleneck — committing 1,000 separate transactions has real per-commit overhead that a single 1M-row transaction doesn't pay. Resource usage also dropped substantially (20.8 MB vs 180.2 MB avg RSS), consistent with the pipeline never needing to queue a large backlog when input arrives at a realistic pace instead of one giant burst.

## 2. Embedding provider latency (isolated, `embedding_bench`)

Purpose: measure raw `embed()` call latency independent of the replication/dispatch pipeline, to establish whether the embedding provider or the pipeline mechanics are the dominant cost in an end-to-end run.

### Local Llama (BGE-M3, Q4_K_M)

**Config**: `provider = "llama"`, `model_path = ".../bge-m3-Q4_K_M.gguf"`, `dimensions = 1024`, `n_threads = 4`, `n_ctx = 512`

| Metric | Value (200 calls) |
|---|---|
| Model init (load) time | 0.93 s |
| Calls | 200 |
| Latency min | 58.30 ms |
| Latency avg | 63.32 ms |
| Latency p50 | 62.51 ms |
| Latency p95 | 70.44 ms |
| Latency max | 109.75 ms |
| Projected serial throughput | 15.8 rows/sec |

Consistent across two independent runs (100 calls: avg 64.72 ms; 200 calls: avg 63.32 ms), confirming stability rather than a one-off measurement.

**Important finding — CPU feature exposure matters enormously for local inference.** An earlier measurement on this same hardware/model/config showed avg latency of **2,027.59 ms** — roughly **32× slower** than the figures above. The cause was traced to the test VM's hypervisor CPU profile not exposing AVX2 to the guest (a VirtualBox/WHP interaction, not a Walkrie or llama.cpp defect); `llama.cpp`'s GGML backend silently falls back to a much slower scalar/generic kernel path when AVX2 isn't detected, with no error or warning at runtime — the only visible symptom is the latency itself. Once AVX2 was exposed correctly (confirmed via `q4_K_8x8` tensor repacking messages in llama.cpp's load log) and the project was rebuilt with a clean CMake reconfigure, latency dropped to the numbers above.

**Practical takeaway for deployment**: anyone running Walkrie's local embedding provider inside a VM or container should verify AVX2 is exposed to the guest (`cat /proc/cpuinfo | grep avx2`) before drawing conclusions about local-model performance. A misconfigured hypervisor can produce a >30× slowdown that looks identical to "the model is just slow on this hardware" without a closer look. Bare-metal deployments are unaffected by this specific issue, but the same check is a cheap, worthwhile first step when local-model latency looks unexpectedly high in any environment.

**Run-to-run variance observed**: a repeat run under identical config measured avg 96.33 ms (p50 91.63 ms, max 209.74 ms) — about 52% higher than the 63.32 ms figure above. Both runs used the same fixed sample sentence and hardware, so this reflects measurement variance (likely background load or CPU frequency scaling in the VM) rather than a config difference. Treat single-run numbers in this section as indicative, not exact; a tighter figure would come from averaging several repeated runs, which has not yet been done.

### OpenAI (`text-embedding-3-small`)

*(network-dependent; also subject to OpenAI rate limits, so treat as informational rather than a hard ceiling)*

| Metric | Value |
|---|---|
| Calls | 200 |
| Latency min | 212.62 ms |
| Latency avg | 306.66 ms |
| Latency p50 | 276.26 ms |
| Latency p95 | 442.27 ms |
| Latency max | 2,707.74 ms |
| Projected serial throughput | 3.3 rows/sec |

The single 2,707.74 ms max is consistent with an occasional network/API-side latency spike rather than a pattern — p95 (442.27 ms) is a more representative worst case than max for this provider. With AVX2 correctly exposed, local Llama (avg 63.32 ms) is now roughly **4.8× faster** than the OpenAI API (avg 306.66 ms) on this hardware, with no network dependency, no per-call cost, and no data leaving the host — see the earlier note on the AVX2 finding for why an initial (incorrect) measurement showed the opposite result.

## 3. End-to-end pipeline (postgres-embedding sink, local Llama provider)

Purpose: full pipeline, real embedding calls, real pgvector upsert — the number that matters for actual deployment sizing.

**Config**: `sink.type = "postgres-embedding"`, `provider = "llama"`. All three runs below use the corrected harness (see Methodology). Sink table (`test_embeddings`) and source table (`test_table`) were truncated before each run — see the index-growth note below.

| | 500 rows, batched 10×100 | 200 rows, single-transaction burst | 200 rows, batched 10×20 |
|---|---|---|---|
| Total wall time | 57.2 s | 44.7 s | 30.8 s |
| Pure processing time (first→last row) | 50.2 s | 29.9 s | 24.1 s |
| Processing throughput | **10.0 events/sec** | **6.7 events/sec** | **8.3 events/sec** |
| Lag min | 6.2 ms | 19.2 ms | 6.5 ms |
| Lag avg | 25,178.5 ms | 15,444.4 ms | 11,304.7 ms |
| Lag p50 | 25,285.7 ms | 15,585.8 ms | 11,252.4 ms |
| Lag p95 | 47,683.5 ms | 28,578.2 ms | 21,659.3 ms |
| Lag max | 50,164.6 ms | 29,943.8 ms | 22,750.1 ms |
| Avg CPU | 232.2% | 266.8% | 94.3% |
| Peak CPU | 532.2% | 405.5% | 613.4% |
| Avg RSS | 711.0 MB | 714.2 MB | 696.6 MB |
| Peak RSS | 728.2 MB | 728.3 MB | 728.2 MB |

**Steady-state throughput: ~7–10 events/sec** (embed + pgvector upsert, serial, single dispatcher thread) on this hardware, for short text (`'Bench Entry ' || N`, ~15 characters). The spread across these three runs (6.7–10.0 events/sec) reflects normal run-to-run variance already noted in Section 2 (background load / CPU frequency scaling in the VM), not a systematic effect of batch size — per-row embed+upsert cost, visible directly in the sink's per-row debug log across all three runs, stayed consistently in the ~85–125 ms range regardless of load pattern.

**Lag numbers are backlog-bound, not steady-state, in all three runs** — and this is expected, not a defect. In every case here, the load script's `psql` transactions complete in well under a second regardless of batch count, while the pipeline processes at ~100–150 ms/row; input therefore always arrives far faster than Walkrie can drain it, and a real queue backlog forms for the full run. This produces a clean internal consistency check: **max lag ≈ row count ÷ processing throughput** in every run (500÷10.0=50.0s vs. 50.2s max; 200÷6.7=29.9s vs. 29.9s max; 200÷8.3=24.1s vs. 22.8s max) — confirming lag here is measuring "time to drain the queue," not "real per-transaction replication lag." A **paced load generator** (inserts spaced slower than ~150 ms apart, so the queue never backs up) is needed to measure true steady-state commit-to-processed lag; this is flagged as a follow-up, not included in this pass.

**Methodology note — table growth affects results.** An earlier version of this test (run against tables that had accumulated several million rows from repeated benchmark sessions without truncation) showed periodic per-row stalls (occasional upsert latency spikes to 40–60 ms against a normal ~4–8 ms baseline), consistent with B-tree index page splits on a large `item_id` unique index. Truncating both tables before each run (`TRUNCATE TABLE test_embeddings; TRUNCATE TABLE test_table RESTART IDENTITY;`) eliminated this pattern. All results in the table above were captured on freshly truncated tables; benchmarking against a large pre-existing table would show additional, table-size-dependent latency spikes not reflected here.

Resource usage (RSS ~700–730 MB, CPU 400–600%+ peak) is consistent with the loaded BGE-M3 model (~540 MB per the model-load log) plus llama.cpp's 4 compute threads running concurrently with the replication and dispatcher threads — not indicative of a leak.

### Real Llama batching enabled (`batch_size = 8`, Q8_0)

Purpose: same end-to-end pipeline as above, but with `LlamaProvider::embed_batch()`'s real multi-sequence implementation active — earlier numbers in this section predate that implementation, so `batch_size` there only grouped DB writes, not `llama_encode()` calls. Model switched to `bge-m3-Q8_0.gguf` for this run rather than `Q4_K_M`: `embed()` and `embed_batch()` diverge meaningfully for `Q4_K_M` but match within the integration test's epsilon for `Q8_0` — see TECHNICAL.md's Known Limitations for the finding and why. The first column below uses the single-transaction burst load (`generate_load.sql`); the other two use the batched load generator (separate committed transactions, 10 rows each).

**Config**: `sink.type = "postgres-embedding"`, `provider = "llama"`, `model_path = ".../bge-m3-Q8_0.gguf"`, `n_ctx = 512`, `[app] batch_size = 8`, `batch_timeout_ms = 50`.

| Metric | 200 rows, single-transaction burst | 200 rows, batched load | 500 rows, batched load |
|---|---|---|---|
| Total wall time | 22.2 s | 88.7 s | 64.5 s |
| Pure processing time | 22.0 s | 23.1 s | 61.2 s |
| Processing throughput | 9.1 events/sec | 8.6 events/sec | 8.2 events/sec |
| Lag min | 34,359.8 ms | 11.2 ms | 5.0 ms |
| Lag avg | 43,736.2 ms | 9,507.5 ms | 29,371.9 ms |
| Lag p50 | 42,348.2 ms | 8,107.9 ms | 29,311.5 ms |
| Lag p95 | 55,075.7 ms | 20,929.8 ms | 56,056.9 ms |
| Lag p99 | 56,077.5 ms | 22,070.3 ms | 58,448.4 ms |
| Lag max | 56,327.9 ms | 22,352.5 ms | 59,033.7 ms |
| Avg CPU | 392.5% | 104.1% | 370.6% |
| Peak CPU | 404.6% | 405.7% | 556.0% |
| Avg RSS | 786.2 MB | 760.1 MB | 787.1 MB |
| Peak RSS | 787.3 MB | 787.5 MB | 789.7 MB |

Throughput across all three (8.2–9.1 events/sec) sits inside the same ~7–10 events/sec range measured *before* real Llama batching existed — consistent with §5's isolated finding below that real batching doesn't reduce per-row cost on this hardware, rather than a regression specific to any one run.

The batched-load columns are backlog-bound exactly as in the original three runs above (200÷8.6=23.3s vs. 22.4s max; 500÷8.2=61.0s vs. 59.0s max) — same internal consistency check, same caveat that a paced load generator is needed for a true steady-state lag figure. The burst column does *not* satisfy that same check (200÷9.1=22.0s pure processing vs. a 56.3s max lag, nearly 2.5× higher) — its 200 rows share one `commit_timestamp` from a single transaction (Methodology), so lag here also folds in whatever time elapsed between that commit and `walkrie_bench` actually starting to drain the slot, not just this run's own queue-drain time. Treat this burst run's throughput/resource figures as the meaningful takeaway, same caveat as Section 1's single-transaction result — its lag numbers are not a steady-state or even a pure-queue-drain figure.

## 4. End-to-end pipeline (postgres-embedding sink, OpenAI provider)

Not run as a separate end-to-end benchmark in this pass — Section 2 already isolates and quantifies the per-call latency difference between the two embedding providers (Llama avg 63.32 ms vs. OpenAI avg 306.66 ms), which is the dominant variable between an OpenAI-backed and Llama-backed end-to-end run; the pgvector upsert cost (Section 3, ~4–8 ms/row baseline) and pipeline overhead (Section 1) are provider-independent. A full OpenAI end-to-end run would mainly confirm this arithmetic under real network conditions and is left as a future addition if OpenAI-specific network variance becomes a question worth answering directly.

## 5. Batched vs. sequential embedding calls (OpenAI, `embedding_batch_bench`)

Purpose: isolate the effect of batching multiple texts into a single `embed_batch()` call (one HTTP round-trip) versus calling `embed()` sequentially N times (N round-trips) — the change introduced by `EventDispatcher`'s optional event batching (`batch_size`/`batch_timeout_ms` in `[app]`) plus `OpenAIProvider::embed_batch()`'s real batched implementation.

**Config**: `provider = "openai"`, `model = "text-embedding-3-small"`, batch size 10, 20 rounds per method.

| Metric | Sequential (10× `embed()`) | Batched (1× `embed_batch()`) |
|---|---|---|
| Avg total/round | 1,983.14 ms | 271.33 ms |
| Min total/round | 1,661.43 ms | 204.43 ms |
| Max total/round | 4,564.08 ms | 341.67 ms |
| **Avg per-row** | **198.31 ms** | **27.13 ms** |

**~7.3× reduction in per-row latency** from batching 10 texts into one request. This is consistent with the theory behind why batching helps at all for a network-bound provider: N sequential HTTP round-trips each pay their own connection/queueing/network overhead independently, while one batched request pays that overhead once and lets OpenAI process the batch server-side. (For the CPU-bound local Llama provider, the theorized equivalent win was shifting from memory-bandwidth-bound GEMV to compute-bound GEMM — see Section 2's discussion; the local Llama provider's actual measurement, now that `embed_batch()` is implemented, is below, and it does not bear this theory out.)

This specific result (batch size 10) is one data point, not necessarily the optimal batch size — a sweep across batch sizes (5/10/20/50) would complete the picture for OpenAI and is flagged as a follow-up.

### Local Llama (BGE-M3, Q8_0, `embedding_bench_batch`)

Purpose: the corresponding measurement for `LlamaProvider::embed_batch()`, now that it does genuine multi-sequence computation (see TECHNICAL.md's Known Limitations) instead of the default per-`embed()` loop.

**Config**: `provider = "llama"`, `model_path = ".../bge-m3-Q8_0.gguf"`, `dimensions = 1024`, `n_threads = 4`, `n_ctx = 512` (context reports back as `n_ctx=2560 n_batch=10240 n_ubatch=10240 n_seq_max=10` once sized for 10 parallel sequences — see `LlamaProvider::init()`), batch size 10, 20 rounds per method.

| Metric | Sequential (10× `embed()`) | Batched (1× `embed_batch()`) |
|---|---|---|
| Avg total/round | 502.29 ms | 602.37 ms |
| Min total/round | 475.85 ms | 575.01 ms |
| Max total/round | 576.16 ms | 692.03 ms |
| **Avg per-row** | **50.23 ms** | **60.24 ms** |

**~20% *higher* per-row latency when batched** — the opposite of the OpenAI result above, and the opposite of this section's original (pre-implementation) theory that batching would help Llama by shifting per-row GEMV into a single larger GEMM.

**Working hypothesis (not yet confirmed by profiling):** `LlamaProvider::embed_batch()` packs all sequences in a chunk into one `llama_encode()` call as parallel sequences sharing one ubatch (`build_batch()` in `llama_provider.cpp`). For a non-causal (embedding) model, attention is a dense QK^T over the whole ubatch — cross-sequence entries are masked to zero *after* the matmul, not skipped by the kernel — so attention compute scales with `(combined tokens in the ubatch)²` rather than `Σ(tokens per sequence)²`. Batching N short sequences into one ubatch multiplies attention cost roughly by N, while the GEMM efficiency gained in the QKV/FFN projections is a comparatively smaller effect at this model size and these sequence lengths — netting a net loss instead of a win. Worth confirming directly (e.g. profiling time spent in the attention sub-graph, or checking whether the slowdown scales with batch size as this theory predicts) before treating it as settled.

**Caveats on this result**: measured on a single resource-constrained VM (see Environment) and against a `llama.cpp` checkout that is itself still under active upstream development — batching internals here are more likely to shift under a `llama.cpp` bump than the rest of this document. Treat this as one data point on this specific hardware/version, not a final verdict on whether Llama-side batching can ever win; a batch-size sweep and a profiled breakdown are natural follow-ups, same as the OpenAI sweep above.

## 6. Backfill drain: worker-count and thread-count scaling (`embed_backfill_batched_bench`)

Purpose: measure the standalone backfill-worker design (issue #1 / WLK-0001) under real multi-process contention — N `walkrie_worker` processes forked against the *same* `BackfillStore` SQLite file, racing on `claim_pending`, each running its own `LlamaProvider` and PG connection. Compares that end-to-end drain against a single-process `embed_batch()` baseline over the same row count, to see whether spawning more worker processes actually buys throughput or just adds contention.

**Config**: `config_samples/config_sample_backfill.toml` (`provider = "llama"`, `model_path = ".../bge-m3-Q4_K_M.gguf"`), sink table truncated between runs. `[app] batch_size` is unset in this config (defaults to `1`), so `max_batch_size=1` → `n_seq_max=1` in `LlamaProvider::init()` — the "baseline" `embed_batch()` call in this bench is therefore a sequential loop of single-sequence `llama_encode()` calls, not true multi-sequence batching (contrast with Section 5's `embedding_bench_batch`, which explicitly forces `n_seq_max=10`). Treat this section's baseline as directly comparable to Section 2's serial `embed()` throughput, not Section 5's batched figures.

Environment is the same 8-vCPU VM as the rest of this document (see Environment above).

### Default config (`n_threads = 4`)

| rows | workers | baseline throughput | end-to-end throughput | overhead |
|---|---|---|---|---|
| 40 | 1 | 18.9 rows/s | 11.6 rows/s | +63.5% |
| 40 | 2 | 17.1 rows/s | 14.0 rows/s | +22.5% |
| 40 | 4 | 13.1 rows/s | 5.7 rows/s | +130.4% |
| 200 | 1 | 12.9 rows/s | 10.0 rows/s | +29.2% |
| 200 | 2 | 17.3 rows/s | 6.7 rows/s | +158.5% |

At `n_threads=4` (the shipped default), each worker process already claims 4 CPU threads for embedding. 2 workers already saturate all 8 vCPUs; 4 workers oversubscribe 2×. The 200-row `workers=2` run above lands squarely on that oversubscription — worse than its own `workers=1` sibling, and worse than the smaller 40-row `workers=2` sample — while baseline throughput itself swings 12.9–17.3 rows/sec run to run (consistent with this VM's documented variance, Section 2). **At this thread count, single-run comparisons between worker counts aren't reliable — the oversubscription effect and the VM's background variance are the same order of magnitude.**

### Tuned config (`n_threads = 2`, `--workers 2`)

Dropping `n_threads` to 2 per worker keeps 2 workers inside the 8-core budget (2×2=4 threads total, well under 8) instead of exactly at or past it. Three repeated runs, 200 rows each:

| run | baseline throughput | end-to-end throughput | overhead |
|---|---|---|---|
| 1 | 8.40 rows/s | 10.94 rows/s | -23.2% |
| 2 | 8.29 rows/s | 9.06 rows/s | -8.4% |
| 3 | 8.01 rows/s | 10.25 rows/s | -21.8% |

Two changes versus the default-config runs above:
- **Variance dropped substantially.** Baseline throughput now sits in an 8.0–8.4 rows/sec band (~5% spread) versus 12.9–17.3 rows/sec (~34% spread) at `n_threads=4`.
- **End-to-end throughput now consistently *beats* the single-process baseline** (avg ~10.1 rows/sec vs ~8.2 rows/sec baseline, a net speedup) instead of trailing it. This is genuine multi-process parallelism finally paying off: the baseline is one process bound to a 2-thread context running its sequential embed loop (per the `n_seq_max=1` note above), while the two-worker drain runs two independent 2-thread contexts concurrently — real wall-clock overlap that the SQLite/PG overhead doesn't fully offset, because neither worker is starved for CPU.

**Practical takeaway**: `--workers` should be sized against `nproc / n_threads` on the deployment host, not set independently of thread count. On this 8-vCPU environment, the shipped default (`n_threads=4`) leaves no room for more than 1 worker before oversubscription erases any multi-process gain; `n_threads=2` + `--workers 2` is the first combination tested here where spawning more worker processes is a net win rather than a net loss.

## 7. Backfill: real end-to-end (OpenAI, `backfill_bench`)

Purpose: unlike Section 6 (which forks multiple `walkrie_worker` processes against a hand-seeded store to stress-test cross-process contention), `backfill_bench` drives the *real* production path with zero synthetic batching — it inserts genuine pre-existing rows into a real Postgres table, drops/recreates the replication slot so the real `dump_all()` actually runs, spawns the real `walkrie_worker` binary, and lets the real `BackfillWorker` → `PgEmbeddingSink` → `OpenAIProvider::embed_batch()` path do its own batching entirely on its own. This section exists specifically to answer "what does 10k-row OpenAI backfill actually cost, in time and money" with no bench-side approximation in the loop — a dedicated OpenAI run is also a *cleaner* signal than Section 6's llama numbers, since network-bound work doesn't inherit this VM's CPU-variance/oversubscription behavior the way local inference does.

**Config**: `config_samples/config_sample_backfill_openai_batched.toml` (`provider = "openai"`, `model = "text-embedding-3-small"`, `dimensions = 1536`, `[app] batch_size = 10`). `walkrie_worker` was spawned with no `--batch-size` override, so it claimed rows in its own default 200-row increments per `claim_pending()` round — each round dispatched as one `sink->call_batch()` call, which `PgEmbeddingSink`/`OpenAIProvider::embed_batch()` sends as a single HTTP request (well under OpenAI's 2048-input cap). Same 8-vCPU VM as the rest of this document (see Environment above), though CPU/RAM aren't the relevant constraint here — this path is network-bound, not compute-bound.

| Rows | Dump time | Drain time (worker batches) | Total wall time | Throughput | Est. cost |
|---|---|---|---|---|---|
| 1,000 | 7.41 s (135 rows/s) | 19.26 s (5× 200-row batches, avg 3.85 s/batch) | 28.01 s | 35.7 rows/s | ~$0.0005 |
| 10,000 | 77.25 s (130 rows/s) | 176.28 s (50× 200-row batches, avg 3.53 s/batch) | 254.85 s (~4.25 min) | 39.2 rows/s | ~$0.005 |

Zero upsert failures across both runs. Cost estimated at ~27 tokens/row (char/4 heuristic on the benchmark's fixed sample sentence) × `text-embedding-3-small`'s $0.02/1M-token rate — not exact billing, but the right order of magnitude; either run costs a fraction of a cent.

**Dump rate is flat (~130 rows/s) between 1k and 10k rows** — expected, it's a single sequential `SELECT` over an unindexed-by-nothing-special table, no per-row network round-trip. **Drain rate is also essentially flat (51.9 rows/s at 1k vs. 56.7 rows/s at 10k, batch-averaged)** — this is the headline finding of this section: unlike Section 6's llama-based numbers, which swing 12.9–17.3 rows/sec run-to-run at the shipped thread count purely from this VM's CPU contention/scaling variance, the OpenAI drain rate barely moved between a 5-batch and a 50-batch run. A network-bound provider genuinely doesn't inherit this machine's local-CPU noise — 10× more rows produced worker-batch timings within about 10% of the 1k run's, not the kind of spread Section 6 documents for local inference under comparable scaling.

**Practical takeaway**: for deployment sizing with the OpenAI provider, a small-scale backfill bench run (hundreds to low thousands of rows) is a reasonably trustworthy predictor of larger-scale throughput on this pipeline — the per-batch cost doesn't measurably change with scale the way local-model contention does. This does not hold for the local Llama provider (Section 6) — worker/thread-count tuning there is scale- and hardware-contention-sensitive and needs to be re-validated at the row counts you actually intend to run.

## Summary

* **Pipeline mechanics are cheap.** With no embedding call and no database write (Section 1), Walkrie decodes and dispatches WAL events at sub-10ms median lag and under 25 MB RSS under realistic (batched-commit) load — the pipeline itself is not the bottleneck.
* **Embedding latency dominates end-to-end cost.** Local Llama (BGE-M3, Q4_K_M, AVX2-enabled) averages 63.32 ms/call; OpenAI's `text-embedding-3-small` averages 306.66 ms/call — roughly 4.8× slower, plus network dependency and per-call cost (Section 2).
* **End-to-end steady-state throughput is ~7–10 events/sec** with the local Llama provider, serial embed + pgvector upsert on a single dispatcher thread (Section 3). This is the number to use for local-model deployment sizing on comparable hardware.
* **CPU feature exposure is a critical, easy-to-miss deployment variable.** A VM with AVX2 not passed through to the guest measured 32× slower local-embedding latency with no error message — anyone deploying the local provider in a VM or container should check `grep avx2 /proc/cpuinfo` before concluding the model itself is slow.
* **Lag figures in every burst-load test reflect queue drain time, not steady-state replication lag**, since all load-generation scripts used here commit input far faster than the pipeline can process it. A paced load generator is needed for a true steady-state lag number and is the main open item for future benchmarking.
* **Batching delivers a real, measured win for the network-bound OpenAI provider** — ~7.3× lower per-row latency at batch size 10 (Section 5), by collapsing N HTTP round-trips into one.
* **Batching does *not* currently help the local Llama provider** — now that `LlamaProvider::embed_batch()` does genuine multi-sequence computation, the isolated measurement shows ~20% *higher* per-row latency batched vs. sequential (Section 5), and the end-to-end run confirms it (Section 3's real-batching numbers land in the same ~7–10 events/sec range as before batching existed). Working hypothesis: dense non-causal attention over the combined ubatch scales with combined-sequence-length², offsetting the GEMM efficiency gained elsewhere — not yet confirmed by profiling, and measured on a resource-constrained VM against a still-evolving `llama.cpp`, so treat this as a snapshot rather than a ceiling.
* **Backfill worker count must be sized against `n_threads`, not set independently** (Section 6). At the shipped default (`n_threads=4`), 2+ `walkrie_worker` processes draining one source oversubscribe this 8-vCPU VM's cores and throughput *drops* below single-worker — spawning more workers made backfill slower, not faster. Reducing to `n_threads=2` and capping at `--workers 2` (4 threads total, within budget) both cut run-to-run variance roughly in half and turned the multi-worker drain into a real net speedup over single-process embedding. Rule of thumb for deployment: `--workers ≈ nproc / n_threads`.
* **OpenAI-backed backfill is cheap and scales predictably** (Section 7). A real end-to-end 10,000-row backfill (dump + drain via the real `walkrie_worker`) completed in ~4.25 minutes for an estimated ~$0.005, with per-batch drain throughput essentially flat between 1,000 and 10,000 rows (51.9 vs. 56.7 rows/sec) — unlike the local Llama provider, a network-bound backfill doesn't inherit this VM's CPU-contention variance, so a small-scale bench run is a trustworthy predictor of larger-scale OpenAI backfill throughput on this pipeline.
