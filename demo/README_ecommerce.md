# Walkrie Multilingual E-Commerce Search Demo

A worked example of walkrie streaming CDC into embeddings for languages
other than English: a synthetic product catalog (titles/descriptions in
Japanese and Turkish) lands in Postgres, walkrie streams every insert/update
into local embeddings via `llama.cpp` using **jina-embeddings-v3** — a
multilingual model with **LoRA task adapters** (see
[TECHNICAL.md](../TECHNICAL.md) and the `lora_path`/`lora_scale` fields in
`config.hpp`) — and a companion CLI lets you search the catalog semantically
in either language, including hybrid search combining plain SQL filters
(`category`, `language`) with vector similarity ranking.

This demo is also the concrete illustration of why jina-embeddings-v3 needs
**two separate embedding configs**, not one:

* `config_product_demo.toml` — used by walkrie itself, loads the
  `retrieval.passage` LoRA adapter (for embedding rows *being indexed*).
* `config_product_query.toml` — used by `product_search_tool.cpp`, loads the
  `retrieval.query` adapter instead (for embedding the user's *search text*).

Both adapters point at the same base model but are different tensors on
disk; walkrie's `LlamaProvider` loads and activates one adapter once at
startup and keeps it active for the process's whole lifetime, so one running
process = one fixed task. See `config_product_query.toml`'s header comment
for the full reasoning.

## 1. Database setup

**While connected to `postgres` (or any existing database):**

```sql
CREATE DATABASE ecommerce_demo;
CREATE ROLE walkrie_demo WITH LOGIN REPLICATION PASSWORD 'changeme';
```

If you already created `walkrie_demo` for the CV demo, skip the `CREATE
ROLE` — roles are cluster-wide, not per-database.

**Then switch your connection to `ecommerce_demo`** and run:

```sql
GRANT ALL PRIVILEGES ON DATABASE ecommerce_demo TO walkrie_demo;
GRANT ALL ON SCHEMA public TO walkrie_demo;
```

(The second grant matters on Postgres 15+ — see the CV demo's README for
why.)

## 2. Seed the source table

```bash
psql -d ecommerce_demo -f demo/insert_products.sql
```

This creates a `products` table and inserts 240 synthetic (fake, randomly
generated) products across 8 categories (Electronics, Home & Kitchen,
Sports, Fashion, Beauty, Toys & Games), each row randomly Japanese or
Turkish — with `category` and `language` columns for structured filtering.

Re-running after a partial attempt: `TRUNCATE TABLE products RESTART IDENTITY;`

## 3. Enable logical replication on the source table

```sql
ALTER TABLE products REPLICA IDENTITY DEFAULT;
CREATE PUBLICATION product_pub FOR TABLE products;
```

## 4. Create the sink table

```bash
psql -d ecommerce_demo -f demo/product_search.sql
```

Then grant `walkrie_demo` ownership (same reasoning as the CV demo — a
database-level grant doesn't cascade to tables created afterward):

```sql
ALTER TABLE product_search OWNER TO walkrie_demo;
```

## 5. Place the model + adapters, then configure walkrie

Download the base model and both LoRA adapters (only `retrieval.passage`
and `retrieval.query` are needed for this demo, not all five task
adapters):

```bash
mkdir -p ~/models/jina-embeddings-v3
cd ~/models/jina-embeddings-v3
BASE="https://huggingface.co/gaianet/jina-embeddings-v3-GGUF/resolve/main"
curl -sL -o jina-embeddings-v3-Q4_K_M.gguf                        "$BASE/jina-embeddings-v3-Q4_K_M.gguf"
curl -sL -o lora-retrieval.passage-jina-embeddings-v3-f16.gguf     "$BASE/lora-retrieval.passage-jina-embeddings-v3-f16.gguf"
curl -sL -o lora-retrieval.query-jina-embeddings-v3-f16.gguf       "$BASE/lora-retrieval.query-jina-embeddings-v3-f16.gguf"
```

Edit `demo/config_product_demo.toml` and `demo/config_product_query.toml`
if your DB credentials or model paths differ from the defaults (both
assume `~/models/jina-embeddings-v3/`, i.e. `/home/<you>/models/...`).

`config_product_demo.toml`'s `[[source]]` sets `backfill = true` — this
picks up the 240 products already inserted in step 2, the same way the CV
demo's backfill works (see [TECHNICAL.md](../TECHNICAL.md#5-initial-backfill-scan)).

```bash
mkdir -p /tmp/logs /tmp/walkrie_backfill_ecommerce
walkrie -f -c demo/config_product_demo.toml
```

```bash
tail -f /tmp/logs/walkrie_ecommerce.log
```

Wait for `[BackfillManager] backfill drain complete for source
'product_cdc_slot'` before querying — otherwise you'll only see a partial
catalog.

## 6. Build and run the search CLI

```bash
# from your build directory
make product_search
```

**Pure semantic search, Japanese query:**
```bash
./product_search demo/config_product_query.toml "防水のバックパック"
```

**Pure semantic search, Turkish query:**
```bash
./product_search demo/config_product_query.toml "su geçirmez sırt çantası"
```

Note the config argument is `config_product_query.toml`, **not**
`config_product_demo.toml` — the query-side LoRA adapter is what makes
these searches score correctly against the passage-adapter embeddings
walkrie already wrote (see the intro above).

**Hybrid search** — semantic ranking narrowed by an exact `language` filter:
```bash
./product_search demo/config_product_query.toml "yüz bakım ürünleri" --language tr --limit 5
```

**Hybrid search on category:**
```bash
./product_search demo/config_product_query.toml "軽くて丈夫な靴" --category Sports
```

`category` and `language` compose the same way `location` does in the CV
demo: plain `WHERE` clauses sitting next to `ORDER BY embedding <=>
$1::vector`, in one query, no separate search engine to keep in sync.

Try a cross-category, cross-language query to see the semantic ranking at
work:
```bash
./product_search demo/config_product_query.toml "hediye önerisi güzellik ürünü"
./product_search demo/config_product_query.toml "オフィスで使えるおしゃれな照明"
```

## 7. Watch it update live

With walkrie still running, insert a new product directly via `psql`:

```sql
INSERT INTO products (sku, category, language, price, currency, title, description)
VALUES (
    'SKU-999999',
    'Electronics',
    'ja',
    18000,
    'JPY',
    'プレミアムワイヤレスイヤホン',
    'プレミアムワイヤレスイヤホン。ノイズキャンセリング機能を搭載し、ホワイトカラーが人気です。Bluetooth 5.3対応で通勤や運動時にも快適にお使いいただけます。'
);
```

Rerun a relevant search and the new product should appear within moments —
no batch job, no re-index step.

## Notes

* `product_search_tool.cpp` hardcodes its own connection string, same as
  `candidate_search_tool.cpp` — pass `--conninfo "host=... dbname=...
  user=... password=..."` to override without editing the binary.
* This dataset is entirely synthetic — no real product/vendor data.
* If searches score poorly despite matching content, double-check you're
  running `product_search` against `config_product_query.toml`, not
  `config_product_demo.toml` — using the passage adapter to embed a search
  query is the most likely mistake with this setup, and it fails silently
  (no error, just degraded ranking) rather than loudly.
* If you hit `permission denied for table product_search` in walkrie's
  logs, double-check step 4's `ALTER TABLE ... OWNER TO` actually ran.
