-- generate_load.sql — single-transaction bulk insert (fast, but produces one giant WAL transaction)
INSERT INTO test_table (name)
SELECT 'Bench Entry ' || i
FROM generate_series(1, 1000) AS i;
