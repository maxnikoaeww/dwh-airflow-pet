MERGE INTO dds.fct_metal_price AS t
USING (
    SELECT price_date, metal_code, buy, sell, md5(concat_ws('|', buy, sell)) AS row_hash
    FROM stg.metal_price
    WHERE load_id = %(load_id)s
) AS s
ON t.price_date = s.price_date AND t.metal_code = s.metal_code
WHEN MATCHED AND t.row_hash <> s.row_hash THEN
    UPDATE SET buy = s.buy, sell = s.sell, row_hash = s.row_hash, load_id = %(load_id)s, updated_at = now()
WHEN NOT MATCHED THEN
    INSERT (price_date, metal_code, buy, sell, row_hash, load_id)
    VALUES (s.price_date, s.metal_code, s.buy, s.sell, s.row_hash, %(load_id)s);
