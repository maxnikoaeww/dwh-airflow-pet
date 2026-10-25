MERGE INTO dds.fct_key_rate AS t
USING (
    SELECT DISTINCT ON (rate_date) rate_date, rate, md5(rate::text) AS row_hash
    FROM stg.key_rate
    WHERE load_id = %(load_id)s
    ORDER BY rate_date
) AS s
ON t.rate_date = s.rate_date
WHEN MATCHED AND t.row_hash <> s.row_hash THEN
    UPDATE SET rate = s.rate, row_hash = s.row_hash, load_id = %(load_id)s, updated_at = now()
WHEN NOT MATCHED THEN
    INSERT (rate_date, rate, row_hash, load_id)
    VALUES (s.rate_date, s.rate, s.row_hash, %(load_id)s);
