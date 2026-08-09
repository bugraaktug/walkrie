# Integration Tests

The tests below need a live Postgres connection — that's what makes them
integration tests rather than part of the `walkrie_tests` doctest suite
(`src/tests/`), which needs neither a live database nor (for most cases) a
real embedding provider. Each uses its own dedicated tables and its own
dedicated config file, so running one doesn't disturb the other or whatever
tables/config you use for your own manual testing.

## Batching Integration Test (`test_sink_batch_mode`)

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

## Important: the equality tolerance is quantization- AND hardware-dependent

`LlamaProvider::embed_batch()` has a real override — a multi-sequence
`llama_encode()` (via `n_seq_max`), not the default per-`embed()` loop.
As predicted below, batched and sequential calls are **not** guaranteed
to produce bit-identical output, and this has now been confirmed
empirically on two different machines, with a twist: how far apart they
land depends on both the model's quantization and the specific
hardware/build running it — not quantization alone.

The comparison itself is gated on **cosine distance** (`1 - cosine_similarity`
between the single-path and batch-path embedding for the same row), not raw
per-dimension `max_abs_diff` — `--epsilon` is the max acceptable cosine
distance. This is deliberate: raw abs-diff bakes in whatever absolute scale
a provider happens to return — `LlamaProvider` returns raw, unnormalized
vectors (norm ~25 for the model used here); OpenAI's API returns
unit-normalized vectors (norm ~1) by default — so the same numeric
threshold isn't equivalently strict across providers. `max_abs_diff` is
still printed on a failure as diagnostic context, it just doesn't decide
pass/fail.

- **`bge-m3-Q4_K_M.gguf`** — `embed()` vs. `embed_batch()` diverge well
  past the default near-zero `--epsilon 1e-6` (cosine distance), on every
  machine tested so far. A test run against this model at a tight epsilon
  will (correctly) fail the cross-comparison check.
- **`bge-m3-Q8_0.gguf`** — machine-dependent, confirmed both ways:
  - On a VirtualBox VM (PERFORMANCE.md's Environment 1), the two match
    within the default `--epsilon 1e-6`. This is the machine/quantization
    combination used for the Llama numbers in PERFORMANCE.md's batching
    section for that reason.
  - On bare-metal, native-CPU-build hardware (Environment 2, Rocky Linux
    9), the **same model file** instead lands at cosine similarity
    ~0.9995-0.9997 (cosine distance ~0.0003-0.0005) for several rows in
    the `mixed_realistic_5insert_1delete_4update` scenario — roughly two
    to three orders of magnitude past `1e-6`. The vectors remain ~99.95%+
    similar, so this is not a correctness break, just proof that "Q8_0
    always passes at a tight epsilon" was a property of the first machine
    it was measured on, not of the quantization itself.

Root cause for why this is machine-dependent isn't established (see
TECHNICAL.md's Known Limitations) — treat it as an observed effect to
plan around, not a mechanism to design against. Practically: pick
`--epsilon` for the machine you're actually testing on (e.g.
`--epsilon 0.001` comfortably clears the Environment 2 drift above), and
when a row fails, look at the reported `cosine_similarity` — near `1.0`
is this floating-point-drift effect, not a regression; a genuinely low
`cosine_similarity` (or a row-count/item_body/spot-check failure) would
indicate something actually wrong.

- This is worth treating as a deliberate, documented tolerance choice per
  model *and* per deployment machine — not a silent "loosen it until the
  test passes" fix, since a tolerance that's too loose would stop
  catching a genuine correctness regression. Re-measure rather than
  reusing a number from a different machine.

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
that inference from a general hardware/numerics principle is now backed
by this project's own empirical data above (real batched `llama_encode()`
output, measured drift confirmed on two separate machines with the same
model file), not just a citation from elsewhere. Treat the `LlamaProvider`
epsilon-loosening guidance above as measured behavior of this project's
own batching implementation, not merely an inferred precaution — still
worth re-verifying on any new deployment hardware before trusting a
specific tolerance value, since the two machines measured so far already
disagree with each other.

## Sink Dimension-Check Integration Test (`test_sink_dims_check`)

`test_sink_dims_check.cpp` verifies `PgEmbeddingSink::verify_sink_column_dimensions()`
(`pgembedding_sink.cpp`) — the startup check that compares a sink table's
actual `vector(N)` column width (read from `pg_attribute.atttypmod`)
against the embedding provider's real output dimension, throwing before
any writes happen rather than letting every upsert fail at runtime with
pgvector's own `expected N dimensions, not M` error. This needs a real
`pg_attribute` lookup against a live connection, so — like the batching
test above — it can't be covered by the doctest unit suite.

### Cases

1. **`matching_dims_passes_init`** — sink column declared with exactly the
   provider's dimension; `init()` must not throw.
2. **`mismatched_dims_throws_naming_both_values`** — sink column declared
   `provider_dims + 7`; `init()` must throw, and the message must name
   both the column's declared width and the provider's actual output size.
3. **`unconstrained_vector_column_skips_check`** — sink column declared as
   a bare `vector` (no dimension, `atttypmod = -1`); nothing to compare
   against, so `init()` must not throw.
4. **`missing_sink_column_throws`** — table created with no embedding
   column at all; `init()` must throw mentioning the column wasn't found.

Each case creates its table fresh (`DROP TABLE IF EXISTS` then `CREATE
TABLE`), constructs a `PgEmbeddingSink` pointed at it, and asserts whether
`init()` throws (and, when it should, that the exception message contains
the expected substrings) — then drops the table again.

### Running

```bash
./test_sink_dims_check ../config_samples/config_sample_dims_check_test.toml \
    --conninfo "host=localhost port=5432 dbname=walkrie_test user=walkrie_demo password=changeme"
```

Uses its own dedicated config (`config_sample_dims_check_test.toml`) and
its own dedicated tables, all prefixed `walkrie_it_dims_*` — distinct from
the batching test's `walkrie_it_single`/`walkrie_it_batch` — so the two
integration tests don't interfere with each other. Only `[embedding]` in
that config is actually read (to construct the real embedding provider and
learn its true output dimension); `[source]`/`[sink]` exist only to make
it a complete, loadable config file.

Exit code is `0` if all cases pass, `1` otherwise — same convention as
`test_sink_batch_mode`, safe to wire into the same CI step.

## Backfill Dump Integration Test (`test_backfill_dump`)

`test_backfill_dump.cpp` verifies issue #1 / WLK-0001 slice 3 — the dump
phase and live-event reconciliation actually wired into
`PgReplicationSource` — against a live Postgres. `test_backfill_util.cpp`
(doctest suite) covers `BackfillUtil::absorb_event`'s logic in isolation
with hand-built `ChangeEvent`s; this test drives the real `connect()` /
`run_backfill_dump_if_required()` / `start_streaming()` sequence the way
`main.cpp` does, which is the only way to catch a wiring bug like a phase
never actually being called (this test caught exactly that during
development).

### Phases

1. **Fresh dump** — two rows inserted before the slot exists ("pre-existing
   rows"). After `connect()` (asserting `was_slot_freshly_created()`) and
   `run_backfill_dump_if_required()`, inspects the `BackfillStore` file
   directly (`is_table_dumped`, `get_row_data`) to confirm both rows landed
   with the right content.
2. **Live update merges + suppresses dispatch** — `start_streaming()`, then
   a real `UPDATE` (metadata column only) on one of the still-pending rows.
   Asserts the `Update` event never reaches the registered handler (would
   have gone to sinks in `main.cpp`) while the transaction's `Commit`
   marker still does, and that the backfill store's `row_data` reflects the
   merged value while retaining the previously-known embed text.
3. **Resume doesn't re-dump** — drops the first `PgReplicationSource` (releases
   the slot's client connection), inserts a third row directly, then
   connects a second source against the same (now-existing) slot. Asserts
   `was_slot_freshly_created()` is false and that the third row never made
   it into the backfill store — the "no effect on resume" invariant.

### Running

```bash
./test_backfill_dump ../config_samples/config_sample_backfill.toml
```

Uses `walkrie_it_backfill_table`/`_pub`/`_slot`, distinct from the other
integration tests' names, and its own SQLite file under the system temp
directory (removed on both start and successful completion). `[sink]`/
`[embedding]` in the config exist only to make it a loadable config file —
this test never constructs a sink or embedding provider, since suppressed
events are asserted by absence from the handler, not by inspecting a sink
table.

Exit code is `0` if all phases pass, `1` otherwise — same convention as the
other integration tests here.
