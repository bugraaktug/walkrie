# Walkrie CV / HR Semantic Search Demo

A worked example of walkrie's core pitch: candidate CVs land in Postgres,
walkrie streams every insert/update into local embeddings via `llama.cpp`,
and a small companion CLI lets you search them semantically — including
**hybrid search**, combining a plain SQL filter with vector similarity
ranking in a single query — entirely on your own infrastructure, with no
candidate data ever sent to a third-party API.

## 1. Database setup

`createdb`/`createuser` are shell utilities, not SQL — if you're working
entirely inside pgAdmin's Query Tool (rather than a terminal), use the SQL
equivalents below instead.

**While connected to `postgres` (or any existing database — `CREATE
DATABASE` cannot run inside a transaction block alongside other
statements, so run it on its own):**

```sql
CREATE DATABASE hr_demo;
CREATE ROLE walkrie_demo WITH LOGIN REPLICATION PASSWORD 'changeme';
```

**Then switch your Query Tool connection to `hr_demo`** (pgAdmin: right-click
`hr_demo` in the tree → Query Tool, or change the connection dropdown), and run:

```sql
GRANT ALL PRIVILEGES ON DATABASE hr_demo TO walkrie_demo;
```

(Terminal equivalent, if you prefer: `createdb hr_demo` and
`createuser --replication --pwprompt walkrie_demo`.)

## 2. Seed the source table

Run `insert_candidates.sql` against `hr_demo` — paste it into pgAdmin's Query
Tool, or from a terminal:

```bash
psql -d hr_demo -f demo/insert_candidates.sql
```

This creates a `candidates` table and inserts 300 synthetic (fake, randomly
generated) CVs spanning a dozen engineering/product roles, with a
`location` field (Berlin, Amsterdam, Austin, Remote, etc.) that we'll use
for structured filtering later.

If you re-run this after an earlier failed/partial attempt, clear old data
first so you don't end up with stale or duplicate rows:
```sql
TRUNCATE TABLE candidates RESTART IDENTITY;
```

## 3. Enable logical replication on the source table

```sql
ALTER TABLE candidates REPLICA IDENTITY DEFAULT;
CREATE PUBLICATION cv_pub FOR TABLE candidates;
```

## 4. Create the sink table

Run `candidate_search.sql` against `hr_demo`:

```bash
psql -d hr_demo -f demo/candidate_search.sql
```

This creates `candidate_search` — with `location` mapped as a plain,
queryable metadata column alongside the embedding — and an HNSW index on
the embedding column.

**Grant `walkrie_demo` ownership of the sink table.** `GRANT ALL PRIVILEGES
ON DATABASE ...` (step 1) only grants database-level `CONNECT`/`CREATE` —
it does not cascade to tables created afterward, especially by a different
role (e.g. whoever ran the SQL above in pgAdmin). Making `walkrie_demo` the
owner is the simplest fix for a demo/dev setup — ownership implies all
privileges automatically, including on the backing `bigserial` sequence
(a separate, easy-to-miss grant otherwise):

```sql
ALTER TABLE candidate_search OWNER TO walkrie_demo;
```

If you skip this, walkrie will connect fine but every upsert will fail
with `permission denied for table candidate_search` in its logs.

## 5. Place the model and configure walkrie

Requires the local Llama provider — see TECHNICAL.md's
[Model Installation](../TECHNICAL.md#model-installation) section if you
haven't already placed a GGUF model.

Edit `demo/config_cv_demo.toml` if your DB credentials or model path
differ from the defaults, then run walkrie in the foreground so you can
watch it work:

```bash
walkrie -f -c demo/config_cv_demo.toml
```

You should see 300 `insert event received` / `upserted` log lines stream
by as walkrie catches up on the seeded data.

## 6. Build and run the search CLI

If not already built as part of the main project:
```bash
# from your build directory, after adding candidate_search to CMakeLists.txt:
make candidate_search
```

**Pure semantic search** — no structured filter, ranked purely by meaning:
```bash
./candidate_search demo/config_cv_demo.toml \
    "senior backend engineer with postgres and docker experience"
```

You should get back a ranked list of the most semantically similar
synthetic candidates — including ones that don't share exact keywords
with the query, which is the point: this is meaning-based retrieval, not
keyword search.

**Hybrid search** — the same semantic ranking, narrowed first by an exact
SQL filter on `location`:
```bash
./candidate_search demo/config_cv_demo.toml \
    "senior backend engineer with postgres and docker experience" \
    --location Berlin --limit 5
```

This is the important part to notice: `location` is not a pgvector
concept at all — it's an ordinary `WHERE location = $2` clause sitting
right next to the `ORDER BY embedding <-> $1::vector`. Structured filters
(location, years of experience, department, anything else mapped as
`metadata` in your `table_mapping`) and semantic ranking compose in plain
SQL, in the same query, with no separate search engine or index to keep
in sync. This is the practical shape of "hybrid search" for most real
deployments — not a blend of two search engines, just SQL that happens to
have a vector column in it.

Note that `cv_text` already includes each candidate's location (`"...based
in Warsaw."`), so a plain semantic query like `"senior backend engineer
with postgres and docker experience in Warsaw"` — with no `--location`
flag at all — will already tend to surface Warsaw-based candidates higher,
purely because the embedding picked up on that phrase; `--location` turns
that soft, probabilistic preference into a guaranteed exact filter.

Try a few more:
```bash
./candidate_search demo/config_cv_demo.toml "machine learning and NLP background"
./candidate_search demo/config_cv_demo.toml "someone who can run production Kubernetes clusters" --location Remote
```

## 7. Watch it update live

With walkrie still running, insert a new candidate directly via `psql`
or pgAdmin's Query Tool:

```sql
INSERT INTO candidates (full_name, years_experience, location, cv_text)
VALUES (
    'Sasha Okafor',
    9,
    'Remote',
    'Sasha Okafor — Staff Backend Engineer with 9 years of experience, based in Remote. Core skills: PostgreSQL, distributed systems, Go, gRPC, Kubernetes. Led migration of a monolith to microservices at scale.'
);
```

Within moments (embedding + upsert latency — see PERFORMANCE.md for real
numbers), rerun a relevant search (with or without `--location Remote`)
and the new candidate should appear — with no batch job, no re-index
step, no manual sync trigger.

## Notes

* `candidate_search.cpp` currently hardcodes its own Postgres connection
  string rather than reading it from the walkrie config — adjust it to
  match your actual `[[sink]]` block, or add a `--conninfo` flag if you'd
  rather not hardcode it at all.
* `--location` currently does an exact string match (`WHERE location = $2`).
  Extending this to other metadata columns (years of experience ranges,
  department, seniority) is a straightforward extension of the same
  pattern — add the column to `candidate_search`'s `table_mapping` as
  `role = "metadata"`, then add a corresponding CLI flag and `WHERE`
  clause in `candidate_search.cpp`.
* This dataset is entirely synthetic — no real candidate/personal data of
  any kind. Do not substitute real CVs into a public demo without
  appropriate consent and data handling review.
* If you hit `permission denied for table candidate_search` in walkrie's
  logs after everything above looks right, double-check step 4's
  `ALTER TABLE ... OWNER TO` actually ran — this is the single most common
  setup mistake in this walkthrough.
