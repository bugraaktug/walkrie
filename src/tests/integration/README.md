# Batching Integration Test

`test_sink_batch_mode.cpp` verifies that `PgEmbeddingSink::call()`
(the single-event path — what `batch_size=1` produces) and
`PgEmbeddingSink::call_batch()` (the batched path — what a real dispatcher
batch produces) leave the database in an **identical final state** for the
same sequence of events.

## Why this approach

- **No live WAL streaming, no `EventDispatcher`, no threads.** The test
  calls the sink's public API directly with hand-built `ChangeEvent`s.
  This makes it fast, deterministic, and trivially repeatable — a flaky
  integration test that depends on real replication timing would be far
  less useful as a regression guard. `EventDispatcher`'s own
  batching/timeout grouping logic is covered separately by
  `test_event_dispatcher_batching.cpp` (doctest, no DB required).
- **Two fixed, predefined tables** (`walkrie_it_single`, `walkrie_it_batch`)
  rather than dynamically-named/configurable ones — keeps the test fully
  self-contained and avoids any config-mismatch failure mode that isn't
  about the actual logic under test.
- **One shared embedding provider instance** feeds both sinks — avoids
  loading a local model twice, and ensures any provider-level state is
  identical across both paths.

## Scenarios

1. **`simple_inserts`** — baseline, no updates/deletes.
2. **`inserts_then_updates_including_unchanged_skip`** — includes an
   update with identical old/new text, which `PgEmbeddingSink` should
   skip entirely (no re-embed). Exercising this through both the single
   and batched paths is exactly the kind of logic that could silently
   diverge in a batch-rewrite bug.
3. **`insert_then_delete_same_id`** — **the regression case.** An insert
   and a delete for the same id land in one batch. This is the exact
   scenario that motivated the two-pass `call_batch()` rewrite: deletes
   used to execute immediately during validation, before same-batch
   inserts (deferred until after the batched embed call) had actually
   been upserted — silently resurrecting a row that should have ended up
   deleted. `expect_absent_ids` catches this directly, independent of
   whether single and batch happen to agree with each other.
4. **`delete_then_insert_same_id`** — the mirror case: a delete for a
   not-yet-existing row (a no-op) followed by the actual insert. Confirms
   original event order is preserved regardless of which op comes first.
5. **`mixed_realistic_5insert_1delete_4update`** — reproduces the exact
   "5 inserts, 1 delete, 4 updates" scenario from the original design
   discussion, at a size closer to a real `batch_size=10` batch.

Each scenario checks two independent things:
- **Spot checks** (`expect_present_ids`/`expect_absent_ids`) — is the
  result objectively correct, regardless of whether single and batch
  agree with each other? (Catches a case where both paths are wrong in
  the same way.)
- **Cross-comparison** — do the single-path and batch-path tables end up
  with the same row count, same `item_body` per id, and embeddings within
  `--epsilon` of each other?

## Running

```bash
./test_sink_batch_mode ../config_samples/config_sample.toml \
    --conninfo "host=localhost port=5432 dbname=walkrie_test user=walkrie_demo password=changeme"
```

The target database needs `CREATE EXTENSION vector` permission (the tool
runs this itself, idempotently) and `CREATE TABLE`/`TRUNCATE` on
`walkrie_it_single`/`walkrie_it_batch` — a dedicated test database is
recommended over pointing this at a real data database, same as any other
integration test that truncates tables between runs.

Exit code is `0` if all scenarios pass, `1` otherwise — safe to wire into
a CI step or pre-merge check.

## Important: the equality tolerance will need to change once Llama batching lands

As of this writing, `LlamaProvider` has **no real `embed_batch()`
override** — it inherits `EmbeddingProvider`'s default, which loops
calling `embed()` on each text individually. That means the "batched"
path in this test is, today, doing the exact same per-text `embed()`
calls as the "single" path — so their embeddings are expected to come
back **bit-identical**, and the default `--epsilon 1e-6` reflects that
(near-zero tolerance, just enough to absorb harmless floating-point
formatting round-trip through `::text` serialization).

**Once `LlamaProvider::embed_batch()` is implemented with real
multi-sequence `llama_encode()` (via `n_seq_max`), this assumption
breaks.** Genuine batched computation is not guaranteed to produce
bit-identical floating-point results to sequential per-row calls —
different summation order inside a batched matmul can produce tiny
numerical differences from computing the same row in isolation. At that
point:

- `--epsilon` will need to be loosened to a tolerance appropriate for
  floating-point drift (a small max-abs-diff threshold, or switch the
  comparison to cosine similarity with a threshold like `> 0.999999`
  rather than raw difference).
- This is worth treating as a deliberate, documented tolerance change at
  that time — not a silent "loosen it until the test passes" fix, since a
  tolerance that's too loose would stop catching a genuine correctness
  regression.

The same caveat does **not** apply to `OpenAIProvider`, which already has
a real batched `embed_batch()` today (one HTTP call with an array of
inputs) — if you run this test with `provider = "openai"`, any
floating-point drift between single and batched embeddings is already
live behavior, not a future concern. Worth running once with OpenAI to
see whether real-world drift is meaningfully above `1e-6` in practice,
and adjusting the default tolerance accordingly rather than assuming.

### References — is this actually documented anywhere?

Worth citing real sources rather than asserting this from first principles alone:

* OpenAI's own documentation states embeddings for a given input are
  deterministic — but this is a general "same call, same result" claim,
  not a specific guarantee that batched and sequential calls return
  bit-identical output for the same text.
* A user on OpenAI's developer community forum reported a cosine
  similarity of only ~0.968 between two runs of the same embedding
  request — a meaningfully large drift from 1.0, after ruling out an
  actual bug on their end. ([community.openai.com](https://community.openai.com/t/embedding-model-determinism-big-difference/1207498))
* OpenAI's own CLIP repository has an open GitHub issue confirming that
  the same prompt's embedding is measurably different depending on
  whether it was computed in a batch versus as a single inference call —
  the same underlying phenomenon (non-associative floating-point
  summation order inside a batched matmul), documented as a reproducible
  issue on a different OpenAI model. ([github.com/openai/CLIP#147](https://github.com/openai/CLIP/issues/147))
* A recent study of LLM inference reproducibility across backends found
  that batched generation numerically differs from single-batch
  generation within the *same* backend, confirming this isn't specific to
  any one framework or provider.

**No equivalent official statement or GitHub issue was found specifically
for `llama.cpp`/`ggml`'s embedding batching.** The underlying mechanism
(floating-point non-associativity in batched GEMM vs. sequential GEMV) is
architecture-agnostic and would be expected to apply to ggml's kernels the
same way it applies to CLIP's or any other transformer implementation —
but that is inference from a general hardware/numerics principle, not a
citation from the llama.cpp project itself. Treat the `LlamaProvider`
epsilon-loosening guidance above as a reasonable precaution grounded in
that general principle, not as something llama.cpp's own documentation
warns about — worth re-verifying empirically (run this test with real
batched `llama_encode()` output and measure the actual drift) once that
implementation exists, rather than assuming a specific tolerance value in
advance.
