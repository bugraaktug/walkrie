# Walkrie Performance

This document tracks benchmark results for Walkrie's replication and sink pipeline, measured with the `walkrie_bench` and `embedding_bench` binaries included in this repo. Numbers here are reproducible on the hardware/config noted per section — they are not universal claims about performance on other hardware.

All benchmarks were run locally, source and sink on the same machine (`localhost`), so lag figures are not affected by network latency or clock skew between hosts.

## Methodology

* **`walkrie_bench`** wraps the configured sink in a lag-recording decorator and samples process CPU time (`getrusage`) and RSS (`/proc/self/status`) every 200ms on a background thread, independent of the pipeline's own threads.
* **Lag** is measured as `(time the sink finished processing the row) - (commit_timestamp of the transaction that produced it)`, in milliseconds. This isolates end-to-end pipeline lag, not just decode time.
* **CPU%** is expressed as a percentage of one core's wall-clock time, and can exceed 100% when multiple threads (replication read loop, dispatcher worker, embedding inference) are simultaneously active — it is not capped at 100%.
* Each run generates load via one of two SQL scripts (see `bench/` in the repo):
  * **Single-transaction load**: `INSERT ... SELECT generate_series(1, N)` as one transaction. Fast to generate, but produces one shared `commit_timestamp` for all N rows — the resulting "lag" mostly measures Walkrie's own internal queue drain time, not real replication lag, since Postgres streams all N row messages in one post-commit burst.
  * **Batched load**: N rows split into transactions of 1,000 rows each, committed separately. This produces a realistic spread of commit timestamps and is the methodology used for all lag numbers reported below unless stated otherwise.

## Environment

*(fill in once finalized — CPU model, core count, RAM, disk type, Postgres version, OS)*

| | |
|---|---|
| CPU | TBD |
| RAM | TBD |
| Disk | TBD |
| OS | Debian 12 |
| PostgreSQL | TBD |
| Walkrie build | `RelWithDebInfo`, commit TBD |

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

**Config**: `sink.type = "postgres-embedding"`, `provider = "llama"`

*(pending — recommend a small `--target-count` first, e.g. 100–1,000 rows, given single-threaded serial embed calls; a full 1M-row run at local-model speed could take a very long time and is not necessary to characterize steady-state throughput)*

| Metric | Value |
|---|---|
| Events processed | TBD |
| Wall time | TBD |
| Throughput | TBD |
| Lag min / avg / p50 / p95 / p99 / max | TBD |
| Avg / Peak CPU | TBD |
| Avg / Peak RSS | TBD |

**Expected relationship**: per-row pipeline time ≈ `embed()` latency (section 2) + Postgres upsert round-trip + negligible dispatch overhead (per section 1's baseline). If observed end-to-end latency is significantly higher than this sum, that gap points to pipeline overhead worth investigating rather than embedding cost.

## 4. End-to-end pipeline (postgres-embedding sink, OpenAI provider)

*(pending)*

| Metric | Value |
|---|---|
| Events processed | TBD |
| Wall time | TBD |
| Throughput | TBD |
| Lag min / avg / p50 / p95 / p99 / max | TBD |
| Avg / Peak CPU | TBD |
| Avg / Peak RSS | TBD |

## Summary (interim — sections 3 and 4 still pending)

With CPU features correctly exposed, local embedding (BGE-M3, Q4_K_M) averages **63.32 ms/call**, roughly 4.8× faster than OpenAI's `text-embedding-3-small` API (306.66 ms/call) on this hardware — the local model is both faster and free of per-call cost or data egress. Pipeline mechanics alone (WAL decode, dispatch, JSON serialization, no embedding or database write) impose negligible overhead in comparison: sub-10ms median lag and under 25 MB RSS at steady state under realistic (batched-commit) load. This suggests embedding provider latency, not pipeline overhead, will be the dominant factor in end-to-end throughput — sections 3 and 4 below will confirm whether that holds once a real Postgres upsert is added to the path.

A secondary but important finding from this benchmarking pass: local embedding performance is highly sensitive to whether the host CPU's AVX2 instruction set is actually exposed to the process — a hypervisor misconfiguration (common in VirtualBox VMs under active Windows Hyper-V) can silently produce a 30x+ slowdown with no error message, making a correctly-configured local model appear to be the slower option when it isn't. Anyone deploying Walkrie's local provider should verify `avx2` appears in `/proc/cpuinfo` as a first troubleshooting step if local-model latency looks unexpectedly high.
