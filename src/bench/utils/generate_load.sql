-- generate_load.sql — single-transaction bulk insert (fast, but produces one giant WAL transaction)
INSERT INTO test_table (name)
SELECT 'This is a representative sample sentence used to benchmark embedding latency for the walkrie CDC pipeline.' || i
FROM generate_series(1, 200) AS i;
