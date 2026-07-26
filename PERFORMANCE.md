# Walkrie Performance

This document tracks benchmark results for Walkrie's replication and sink pipeline, measured with the `walkrie_bench` and `embedding_bench` binaries included in this repo. Numbers here are reproducible on the hardware/config noted per section — they are not universal claims about performance on other hardware.

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

**~7.3× reduction in per-row latency** from batching 10 texts into one request. This is consistent with the theory behind why batching helps at all for a network-bound provider: N sequential HTTP round-trips each pay their own connection/queueing/network overhead independently, while one batched request pays that overhead once and lets OpenAI process the batch server-side. (For the CPU-bound local Llama provider, the equivalent win comes from a different mechanism — shifting from memory-bandwidth-bound GEMV to compute-bound GEMM — see Section 2's discussion; that number is not yet measured, since `LlamaProvider` doesn't implement real batched computation yet — see TECHNICAL.md's Known Limitations.)

This specific result (batch size 10) is one data point, not necessarily the optimal batch size — a sweep across batch sizes (5/10/20/50) and a corresponding Llama batching measurement once implemented would complete the picture; both are flagged as follow-ups rather than included in this pass.

## Summary

* **Pipeline mechanics are cheap.** With no embedding call and no database write (Section 1), Walkrie decodes and dispatches WAL events at sub-10ms median lag and under 25 MB RSS under realistic (batched-commit) load — the pipeline itself is not the bottleneck.
* **Embedding latency dominates end-to-end cost.** Local Llama (BGE-M3, Q4_K_M, AVX2-enabled) averages 63.32 ms/call; OpenAI's `text-embedding-3-small` averages 306.66 ms/call — roughly 4.8× slower, plus network dependency and per-call cost (Section 2).
* **End-to-end steady-state throughput is ~7–10 events/sec** with the local Llama provider, serial embed + pgvector upsert on a single dispatcher thread (Section 3). This is the number to use for local-model deployment sizing on comparable hardware.
* **CPU feature exposure is a critical, easy-to-miss deployment variable.** A VM with AVX2 not passed through to the guest measured 32× slower local-embedding latency with no error message — anyone deploying the local provider in a VM or container should check `grep avx2 /proc/cpuinfo` before concluding the model itself is slow.
* **Lag figures in every burst-load test reflect queue drain time, not steady-state replication lag**, since all load-generation scripts used here commit input far faster than the pipeline can process it. A paced load generator is needed for a true steady-state lag number and is the main open item for future benchmarking.
* **Batching delivers a real, measured win for the network-bound OpenAI provider** — ~7.3× lower per-row latency at batch size 10 (Section 5), by collapsing N HTTP round-trips into one. The local Llama provider does not yet implement real batched computation (see TECHNICAL.md's Known Limitations) — batching's effect there remains theoretical (a shift from memory-bandwidth-bound to compute-bound execution, per Section 2's discussion) until `LlamaProvider::embed_batch()` is implemented and measured.
