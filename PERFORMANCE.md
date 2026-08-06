# Walkrie Performance

This document tracks benchmark results for Walkrie's replication and sink pipeline, measured with the `walkrie_bench`, `embedding_bench`, `embed_backfill_batched_bench`, and `backfill_bench` binaries included in this repo. Numbers here are reproducible on the hardware/config noted per section — they are not universal claims about performance on other hardware.

All benchmarks were run locally, source and sink on the same machine (`localhost`), so lag figures are not affected by network latency or clock skew between hosts.

## Methodology

* **`walkrie_bench`** wraps the configured sink in a lag-recording decorator and samples process CPU time (`getrusage`) and RSS (`/proc/self/status`) every 200ms on a background thread, independent of the pipeline's own threads.
* **Lag** is measured as `(time the sink finished processing the row) - (commit_timestamp of the transaction that produced it)`, in milliseconds.
* **CPU%** is a percentage of one core's wall-clock time and can exceed 100% when multiple threads (replication read, dispatcher, embedding inference) run concurrently — not capped at 100%.
* Load is generated one of two ways: **single-transaction** (`INSERT ... SELECT generate_series`, one commit for all N rows — fast to generate, but all rows share one `commit_timestamp`) or **batched** (N rows split across separate committed transactions, batch size noted per result — a realistic spread of commit timestamps).
* **Two wall-time figures appear per run**: `total wall time` (includes one-time connection/slot setup) and `pure processing time` (first row processed → last row processed). Trust `pure processing time` / `processing throughput` for steady-state comparisons — `total wall time` is diluted by fixed setup cost that doesn't scale with row count.
* **Burst/backlog lag vs. steady-state lag**: every load pattern used in this document commits input faster than Walkrie can process it, so lag figures measure *queue-drain time*, not real per-transaction replication lag. This is confirmed rather than assumed — in every run below, `max lag ≈ row count ÷ processing throughput` holds. A paced load generator (commits spaced slower than per-row processing time) would be needed for a genuine steady-state lag number; not yet built.

## Environment

All benchmarks in this document were run inside a VirtualBox VM (host: Windows, Intel Core Ultra 7 155H) with AVX2 passed through to the guest (required for the local Llama numbers in Section 2 — see that section's AVX2 finding).

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H, 8 vCPUs assigned to guest, 1 thread/core |
| RAM | 7.8 GiB (VM-assigned) |
| Disk | VirtualBox virtual disk, 250 GB (`VBOX HARDDISK`) |
| OS | Debian GNU/Linux 12 (bookworm), kernel 6.1.0-10-amd64 |
| PostgreSQL | 15.18 (Debian 15.18-0+deb12u1) |
| Walkrie build | `RelWithDebInfo` (CMakeLists.txt default) |

## Summary

* **Pipeline mechanics are cheap.** With no embedding call and no database write (Section 1), Walkrie decodes and dispatches WAL events at sub-10ms median lag and under 25 MB RSS under realistic (batched-commit) load.
* **Embedding latency dominates end-to-end cost.** Local Llama (BGE-M3, Q4_K_M, AVX2-enabled) averages 63.32 ms/call; OpenAI's `text-embedding-3-small` averages 306.66 ms/call — ~4.8× slower, plus network dependency and per-call cost (Section 2).
* **End-to-end steady-state throughput is ~7–10 events/sec** with the local Llama provider, serial embed + pgvector upsert on a single dispatcher thread (Section 3) — the number to use for local-model deployment sizing on comparable hardware.
* **CPU feature exposure is a critical, easy-to-miss deployment variable.** A VM with AVX2 not passed through measured 32× slower local-embedding latency with no error message — check `grep avx2 /proc/cpuinfo` before concluding the model itself is slow (Section 2).
* **Batching helps OpenAI (~7.3× lower per-row latency at batch size 10), but currently hurts local Llama (~20% higher)** — batching collapses N HTTP round-trips into one for OpenAI, but for llama.cpp's non-causal encode, dense attention over the combined ubatch scales worse than the GEMM efficiency gained (Section 5, working hypothesis, not yet profiled).
* **Backfill worker count must be sized against `n_threads`, not set independently** (Section 6). At the shipped default (`n_threads=4`), 2+ `walkrie_worker` processes oversubscribe this 8-vCPU VM and throughput *drops* below single-worker. `n_threads=2` + `--workers 2` (within budget) turned multi-worker drain into a real net speedup. Rule of thumb: `--workers ≈ nproc / n_threads`.
* **OpenAI-backed backfill is cheap and scales predictably** (Section 7): a real 10,000-row backfill (dump + drain) completed in ~4.25 min for ~$0.005, with drain throughput essentially flat between 1k and 10k rows — a network-bound backfill doesn't inherit the local-CPU variance the Llama path does, so a small bench run is a trustworthy predictor of larger-scale OpenAI throughput here.

## 1. Pipeline baseline (json-output sink, `discard` target)

Purpose: measure the cost of WAL decode + dispatch + serialization alone, with no database write and no embedding call — the floor the rest of the pipeline is added on top of.

**Config**: `sink.type = "json-output"`, `output_target = "discard"`

### Single-transaction load (1,000,000 rows, one commit)

| Metric | Value |
|---|---|
| Events processed | 1,000,000 |
| Wall time | 44.29 s |
| Throughput | 22,579.7 events/sec |
| Lag min / avg / p50 / p95 / p99 / max | 588.2 / 2,926.8 / 3,083.7 / 4,598.0 / 4,677.7 / 4,697.7 ms |
| Avg / Peak CPU | 12.6% / 160.1% |
| Avg / Peak RSS | 180.2 / 219.2 MB |

All 1,000,000 rows share one `commit_timestamp` here (one transaction), so lag reflects internal queue drain order rather than real per-row lag — throughput and resource figures are the meaningful takeaways from this run; see the batched-load result below for a realistic lag figure.

### Batched load (1,000,000 rows, 1,000 transactions of 1,000 rows each)

| Metric | Value |
|---|---|
| Events processed | 1,000,000 |
| Wall time | 107.20 s |
| Throughput | 9,328.6 events/sec |
| Lag min / avg / p50 / p95 / p99 / max | 2.1 / 8.6 / 7.9 / 12.6 / 42.8 / 93.1 ms |
| Avg / Peak CPU | 8.3% / 40.8% |
| Avg / Peak RSS | 20.8 / 25.3 MB |

This is the trustworthy lag figure: sub-10ms at the median, under 100ms at the tail. Throughput is lower than the single-transaction run (9.3K vs 22.6K events/sec) because Postgres itself becomes the bottleneck — 1,000 separate commits carry real per-commit overhead a single 1M-row transaction doesn't pay. RSS dropped substantially too (20.8 MB vs 180.2 MB avg), consistent with never needing to queue a large backlog when input arrives at a realistic pace.

## 2. Embedding provider latency (isolated, `embedding_bench`)

Purpose: measure raw `embed()` call latency independent of the replication/dispatch pipeline, to establish whether the embedding provider or the pipeline mechanics dominate end-to-end cost.

### Local Llama (BGE-M3, Q4_K_M)

**Config**: `provider = "llama"`, `model_path = ".../bge-m3-Q4_K_M.gguf"`, `dimensions = 1024`, `n_threads = 4`, `n_ctx = 512`

| Metric | Value (200 calls) |
|---|---|
| Model init (load) time | 0.93 s |
| Latency min / avg / p50 / p95 / max | 58.30 / 63.32 / 62.51 / 70.44 / 109.75 ms |
| Projected serial throughput | 15.8 rows/sec |

Consistent across two independent runs (100 calls: avg 64.72 ms; 200 calls: avg 63.32 ms).

**Finding — CPU feature exposure matters enormously for local inference.** An earlier measurement on this same hardware/model/config showed avg latency of **2,027.59 ms**, ~32× slower. Cause: the hypervisor CPU profile wasn't exposing AVX2 to the guest (a VirtualBox/WHP interaction, not a Walkrie or llama.cpp defect) — `llama.cpp`'s GGML backend silently falls back to a much slower scalar kernel path when AVX2 isn't detected, with no runtime error or warning; the only symptom is latency itself. Once AVX2 was exposed correctly (confirmed via `q4_K_8x8` tensor repacking messages in llama.cpp's load log) and the project rebuilt from a clean CMake reconfigure, latency dropped to the numbers above. **Anyone deploying the local provider inside a VM or container should check `grep avx2 /proc/cpuinfo` on the guest first** — a misconfigured hypervisor can produce a >30× slowdown that looks identical to "the model is just slow," and bare-metal deployments aren't affected by this specific issue but the check is still a cheap first step when local-model latency looks unexpectedly high.

**Run-to-run variance observed**: a repeat run under identical config measured avg 96.33 ms (p50 91.63 ms, max 209.74 ms) — ~52% higher than the 63.32 ms figure above, likely background load or CPU frequency scaling in the VM rather than a config difference. Treat single-run numbers in this section as indicative, not exact.

### OpenAI (`text-embedding-3-small`)

*(network-dependent; also subject to OpenAI rate limits, so treat as informational rather than a hard ceiling)*

| Metric | Value (200 calls) |
|---|---|
| Latency min / avg / p50 / p95 / max | 212.62 / 306.66 / 276.26 / 442.27 / 2,707.74 ms |
| Projected serial throughput | 3.3 rows/sec |

The single 2,707.74 ms max looks like an occasional network/API-side spike rather than a pattern — p95 (442.27 ms) is a more representative worst case. With AVX2 correctly exposed, local Llama (avg 63.32 ms) is ~4.8× faster than OpenAI (avg 306.66 ms) on this hardware, with no network dependency, no per-call cost, and no data leaving the host.

## 3. End-to-end pipeline (postgres-embedding sink, local Llama provider)

Purpose: full pipeline, real embedding calls, real pgvector upsert — the number that matters for deployment sizing.

**Config**: `sink.type = "postgres-embedding"`, `provider = "llama"`. Sink table (`test_embeddings`) and source table (`test_table`) truncated before each run — see the index-growth note below.

| | 500 rows, batched 10×100 | 200 rows, single-transaction burst | 200 rows, batched 10×20 |
|---|---|---|---|
| Total wall time | 57.2 s | 44.7 s | 30.8 s |
| Pure processing time | 50.2 s | 29.9 s | 24.1 s |
| Processing throughput | **10.0 events/sec** | **6.7 events/sec** | **8.3 events/sec** |
| Lag min / avg / p95 / max | 6.2 / 25,178.5 / 47,683.5 / 50,164.6 ms | 19.2 / 15,444.4 / 28,578.2 / 29,943.8 ms | 6.5 / 11,304.7 / 21,659.3 / 22,750.1 ms |
| Avg / Peak CPU | 232.2% / 532.2% | 266.8% / 405.5% | 94.3% / 613.4% |
| Avg / Peak RSS | 711.0 / 728.2 MB | 714.2 / 728.3 MB | 696.6 / 728.2 MB |

**Steady-state throughput: ~7–10 events/sec** (embed + pgvector upsert, serial, single dispatcher thread), for short text (~15 characters). The 6.7–10.0 events/sec spread across these three runs is normal run-to-run variance (Section 2), not a batch-size effect — per-row embed+upsert cost stayed consistently ~85–125 ms regardless of load pattern.

Lag here is backlog-bound, not steady-state (see Methodology) — the consistency check holds in all three: 500÷10.0=50.0s vs. 50.2s max; 200÷6.7=29.9s vs. 29.9s max; 200÷8.3=24.1s vs. 22.8s max.

**Methodology note — table growth affects results.** An earlier version of this test, run against tables that had accumulated several million rows from prior sessions without truncation, showed periodic upsert latency spikes (40–60 ms vs. a normal ~4–8 ms baseline), consistent with B-tree index page splits on a large `item_id` unique index. Truncating both tables before each run eliminated this. All results above are on freshly truncated tables — a large pre-existing table would show additional, size-dependent latency spikes not reflected here.

Resource usage (RSS ~700–730 MB, CPU 400–600%+ peak) matches the loaded BGE-M3 model (~540 MB) plus llama.cpp's 4 compute threads running concurrently with the replication and dispatcher threads — not a leak.

### Real Llama batching enabled (`batch_size = 8`, Q8_0)

Purpose: same end-to-end pipeline, but with `LlamaProvider::embed_batch()`'s real multi-sequence implementation active (the runs above predate that implementation — `batch_size` there only grouped DB writes, not `llama_encode()` calls). Model switched to `bge-m3-Q8_0.gguf`: `embed()`/`embed_batch()` diverge meaningfully for `Q4_K_M` but match within epsilon for `Q8_0` — see TECHNICAL.md's Known Limitations.

**Config**: `provider = "llama"`, `model_path = ".../bge-m3-Q8_0.gguf"`, `n_ctx = 512`, `[app] batch_size = 8`, `batch_timeout_ms = 50`.

| Metric | 200 rows, single-transaction burst | 200 rows, batched load | 500 rows, batched load |
|---|---|---|---|
| Total wall time | 22.2 s | 88.7 s | 64.5 s |
| Pure processing time | 22.0 s | 23.1 s | 61.2 s |
| Processing throughput | 9.1 events/sec | 8.6 events/sec | 8.2 events/sec |
| Lag min / avg / p95 / max | 34,359.8 / 43,736.2 / 55,075.7 / 56,327.9 ms | 11.2 / 9,507.5 / 20,929.8 / 22,352.5 ms | 5.0 / 29,371.9 / 56,056.9 / 59,033.7 ms |
| Avg / Peak CPU | 392.5% / 404.6% | 104.1% / 405.7% | 370.6% / 556.0% |
| Avg / Peak RSS | 786.2 / 787.3 MB | 760.1 / 787.5 MB | 787.1 / 789.7 MB |

Throughput across all three (8.2–9.1 events/sec) sits inside the same ~7–10 events/sec range measured *before* real Llama batching existed — consistent with §5's isolated finding that real batching doesn't reduce per-row cost on this hardware, not a regression.

The two batched-load columns are backlog-bound the same way as above (200÷8.6=23.3s vs. 22.4s max; 500÷8.2=61.0s vs. 59.0s max). The burst column does *not* satisfy that check (200÷9.1=22.0s processing vs. a 56.3s max lag) — its 200 rows share one `commit_timestamp`, so lag there also folds in the time between that commit and `walkrie_bench` starting to drain the slot, not just this run's own queue-drain time. Treat this column's throughput/resource figures as the meaningful takeaway, not its lag numbers.

## 4. End-to-end pipeline (postgres-embedding sink, OpenAI provider)

Not run as a separate end-to-end benchmark — Section 2 already isolates the per-call latency difference between providers (Llama avg 63.32 ms vs. OpenAI avg 306.66 ms), the dominant variable between an OpenAI- and Llama-backed run; pgvector upsert cost (Section 3, ~4–8 ms/row) and pipeline overhead (Section 1) are provider-independent. Left as a future addition if OpenAI-specific network variance becomes worth answering directly.

## 5. Batched vs. sequential embedding calls (OpenAI, `embedding_batch_bench`)

Purpose: isolate the effect of batching multiple texts into one `embed_batch()` call (one HTTP round-trip) vs. calling `embed()` sequentially N times (N round-trips).

**Config**: `provider = "openai"`, `model = "text-embedding-3-small"`, batch size 10, 20 rounds per method.

| Metric | Sequential (10× `embed()`) | Batched (1× `embed_batch()`) |
|---|---|---|
| Avg total/round | 1,983.14 ms | 271.33 ms |
| **Avg per-row** | **198.31 ms** | **27.13 ms** |

**~7.3× reduction in per-row latency.** Expected for a network-bound provider: N sequential HTTP round-trips each pay connection/queueing/network overhead independently, while one batched request pays it once. One data point (batch size 10), not necessarily optimal — a sweep across 5/10/20/50 would complete the picture, flagged as a follow-up.

### Local Llama (BGE-M3, Q8_0, `embedding_bench_batch`)

Purpose: the same measurement for `LlamaProvider::embed_batch()`, now that it does genuine multi-sequence computation rather than a per-`embed()` loop.

**Config**: `provider = "llama"`, `model_path = ".../bge-m3-Q8_0.gguf"`, `dimensions = 1024`, `n_threads = 4`, `n_ctx = 512` (reports `n_ctx=2560 n_batch=10240 n_ubatch=10240 n_seq_max=10` once sized for 10 parallel sequences), batch size 10, 20 rounds.

| Metric | Sequential (10× `embed()`) | Batched (1× `embed_batch()`) |
|---|---|---|
| Avg total/round | 502.29 ms | 602.37 ms |
| **Avg per-row** | **50.23 ms** | **60.24 ms** |

**~20% *higher* per-row latency when batched** — the opposite of the OpenAI result, and the opposite of the original theory that batching would help by shifting per-row GEMV into one larger GEMM.

**Working hypothesis (not yet confirmed by profiling):** `embed_batch()` packs all sequences in a chunk into one `llama_encode()` call as parallel sequences sharing one ubatch. For a non-causal (embedding) model, attention is a dense QK^T over the whole ubatch — cross-sequence entries are masked to zero *after* the matmul, not skipped — so attention compute scales with `(combined tokens)²` rather than `Σ(tokens per sequence)²`. Batching N short sequences multiplies attention cost roughly by N, while the GEMM efficiency gained in the QKV/FFN projections is a smaller effect at this model size and sequence length, netting a loss. Worth confirming via profiling (time in the attention sub-graph, or whether slowdown scales with batch size as predicted) before treating as settled.

**Caveats**: measured on one resource-constrained VM against a `llama.cpp` checkout still under active upstream development — treat as one data point on this hardware/version, not a final verdict.

## 6. Backfill drain: worker-count and thread-count scaling (`embed_backfill_batched_bench`)

Purpose: measure the standalone backfill-worker design (WLK-0001) under real multi-process contention — N `walkrie_worker` processes forked against the *same* `BackfillStore` SQLite file, racing on `claim_pending`, each with its own `LlamaProvider` and PG connection — against a single-process `embed_batch()` baseline over the same row count, to see whether more worker processes buys throughput or just adds contention.

**Config**: `config_samples/config_sample_backfill.toml` (`provider = "llama"`, `model_path = ".../bge-m3-Q4_K_M.gguf"`), sink table truncated between runs. `[app] batch_size` unset (defaults to `1`) → `n_seq_max=1`, so the "baseline" here is a sequential loop of single-sequence `llama_encode()` calls, not true multi-sequence batching — comparable to Section 2's serial throughput, not Section 5's batched figures. Same 8-vCPU VM as elsewhere in this document.

### Default config (`n_threads = 4`)

| rows | workers | baseline throughput | end-to-end throughput | overhead |
|---|---|---|---|---|
| 40 | 1 | 18.9 rows/s | 11.6 rows/s | +63.5% |
| 40 | 2 | 17.1 rows/s | 14.0 rows/s | +22.5% |
| 40 | 4 | 13.1 rows/s | 5.7 rows/s | +130.4% |
| 200 | 1 | 12.9 rows/s | 10.0 rows/s | +29.2% |
| 200 | 2 | 17.3 rows/s | 6.7 rows/s | +158.5% |

At `n_threads=4` (shipped default), each worker already claims 4 CPU threads. 2 workers already saturate all 8 vCPUs; 4 workers oversubscribe 2×. The 200-row `workers=2` run lands squarely on that oversubscription — worse than its own `workers=1` sibling — while baseline throughput itself swings 12.9–17.3 rows/sec run to run (Section 2's variance). **At this thread count, single-run comparisons between worker counts aren't reliable — oversubscription and background variance are the same order of magnitude.**

### Tuned config (`n_threads = 2`, `--workers 2`)

Dropping `n_threads` to 2 per worker keeps 2 workers inside the 8-core budget (2×2=4 threads, under 8) instead of at or past it. Three repeated runs, 200 rows each:

| run | baseline throughput | end-to-end throughput | overhead |
|---|---|---|---|
| 1 | 8.40 rows/s | 10.94 rows/s | -23.2% |
| 2 | 8.29 rows/s | 9.06 rows/s | -8.4% |
| 3 | 8.01 rows/s | 10.25 rows/s | -21.8% |

Two changes vs. the default-config runs: **variance dropped substantially** (baseline now 8.0–8.4 rows/sec, ~5% spread, vs. 12.9–17.3 rows/sec, ~34% spread, at `n_threads=4`); and **end-to-end throughput now consistently *beats* the single-process baseline** (avg ~10.1 vs. ~8.2 rows/sec) instead of trailing it — real multi-process parallelism finally paying off once neither worker is CPU-starved.

**Practical takeaway**: size `--workers` against `nproc / n_threads`, not independently. At this VM's shipped default (`n_threads=4`), there's no room for more than 1 worker before oversubscription erases any gain; `n_threads=2` + `--workers 2` is the first combination here where more workers is a net win.

## 7. Backfill: real end-to-end (OpenAI, `backfill_bench`)

Purpose: unlike Section 6 (hand-seeded store, N forked workers, stress-testing contention), `backfill_bench` drives the *real* production path with zero synthetic batching — genuine pre-existing rows inserted into Postgres, slot dropped/recreated so real `dump_all()` runs, real `walkrie_worker` spawned, real `BackfillWorker` → `PgEmbeddingSink` → `OpenAIProvider::embed_batch()` doing its own batching. Answers "what does a 10k-row OpenAI backfill actually cost, in time and money" with no bench-side approximation.

**Config**: `config_samples/config_sample_backfill_openai_batched.toml` (`provider = "openai"`, `model = "text-embedding-3-small"`, `dimensions = 1536`, `[app] batch_size = 10`). `walkrie_worker` used its own default 200-row claim increments (no `--batch-size` override); each round dispatched as one `sink->call_batch()` → one HTTP request (well under OpenAI's 2048-input cap). Note that `[app] batch_size = 10` above played no role in that — it only governs live-event batching via `EventDispatcher`, which backfill never goes through; see TECHNICAL.md's config section. CPU/RAM aren't the constraint here — this path is network-bound.

| Rows | Dump time | Drain time (worker batches) | Total wall time | Throughput | Est. cost |
|---|---|---|---|---|---|
| 1,000 | 7.41 s (135 rows/s) | 19.26 s (5× 200-row batches, avg 3.85 s/batch) | 28.01 s | 35.7 rows/s | ~$0.0005 |
| 10,000 | 77.25 s (130 rows/s) | 176.28 s (50× 200-row batches, avg 3.53 s/batch) | 254.85 s (~4.25 min) | 39.2 rows/s | ~$0.005 |

Zero upsert failures in either run. Cost estimated at ~27 tokens/row (char/4 heuristic on the benchmark's fixed sample sentence) × `text-embedding-3-small`'s $0.02/1M-token rate — order-of-magnitude, not exact billing.

**Dump rate is flat (~130 rows/s) between 1k and 10k rows** — expected, a single sequential `SELECT`, no per-row network round-trip. **Drain rate is also flat: 51.9 rows/s at 1k vs. 56.7 rows/s at 10k** (`= batch size ÷ avg batch time` — `200÷3.85` and `200÷3.53` respectively, equivalently `total rows ÷ total drain time` here since every batch was a uniform 200 rows). This is the headline finding: unlike Section 6's llama-based numbers (12.9–17.3 rows/sec run-to-run swings from this VM's CPU contention), the OpenAI drain rate barely moved between a 5-batch and a 50-batch run — 10× more rows produced worker-batch timings within ~10% of the 1k run's. A network-bound provider genuinely doesn't inherit this machine's local-CPU noise.

**Practical takeaway**: for OpenAI-provider deployment sizing, a small-scale backfill bench run (hundreds to low thousands of rows) reasonably predicts larger-scale throughput — per-batch cost doesn't measurably change with scale. This does *not* hold for the local Llama provider (Section 6) — worker/thread-count tuning there is scale- and hardware-contention-sensitive and needs re-validating at the row counts you actually intend to run.
