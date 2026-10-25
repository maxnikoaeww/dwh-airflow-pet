-- Incremental upsert: only new or changed rows (by hash) touch the fact table
MERGE INTO dds.fct_currency_rate AS t
USING (
    SELECT rate_date, currency_id, char_code, num_code, name, nominal, value, vunit_rate,
           md5(concat_ws('|', char_code, num_code, name, nominal, value, vunit_rate)) AS row_hash
    FROM stg.currency_rate
    WHERE load_id = %(load_id)s
) AS s
ON t.rate_date = s.rate_date AND t.currency_id = s.currency_id
WHEN MATCHED AND t.row_hash <> s.row_hash THEN
    UPDATE SET char_code  = s.char_code,
               num_code   = s.num_code,
               name       = s.name,
               nominal    = s.nominal,
               value      = s.value,
               vunit_rate = s.vunit_rate,
               row_hash   = s.row_hash,
               load_id    = %(load_id)s,
               updated_at = now()
WHEN NOT MATCHED THEN
    INSERT (rate_date, currency_id, char_code, num_code, name, nominal, value, vunit_rate, row_hash, load_id)
    VALUES (s.rate_date, s.currency_id, s.char_code, s.num_code, s.name, s.nominal, s.value, s.vunit_rate,
            s.row_hash, %(load_id)s);
