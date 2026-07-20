#!/bin/bash
# gen_load_batched.sh — commits every 1000 rows instead of one giant transaction
CONN="host=localhost port=5432 dbname=qdb user=quser password=quser1234"
BATCH=10
TOTAL=200

for ((i=0; i<TOTAL; i+=BATCH)); do
    psql "$CONN" -c "
        INSERT INTO test_table (name)
        SELECT 'This is a representative sample sentence used to benchmark embedding latency for the walkrie CDC pipeline.' || s
        FROM generate_series($i, $((i+BATCH-1))) AS s;
    " -q
done
