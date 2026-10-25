-- SCD Type 2 for the currency reference.
-- A changed attribute closes the current version and opens a new one from %(valid_from)s.
-- The very first version of a currency is valid from 1900-01-01, so history joins work.
CREATE TEMP TABLE src ON COMMIT DROP AS
SELECT currency_id, name, eng_name, nominal, parent_code, iso_num_code, iso_char_code AS char_code,
       md5(concat_ws('|', name, eng_name, nominal, parent_code, iso_num_code, iso_char_code)) AS row_hash
FROM stg.currency_ref
WHERE load_id = %(load_id)s;

UPDATE dds.dim_currency AS d
SET valid_to   = %(valid_from)s::date,
    is_current = false
FROM src AS s
WHERE d.currency_id = s.currency_id
  AND d.is_current
  AND d.row_hash <> s.row_hash;

INSERT INTO dds.dim_currency
    (currency_id, char_code, name, eng_name, nominal, parent_code, iso_num_code, row_hash, valid_from)
SELECT s.currency_id, s.char_code, s.name, s.eng_name, s.nominal, s.parent_code, s.iso_num_code, s.row_hash,
       CASE WHEN EXISTS (SELECT 1 FROM dds.dim_currency x WHERE x.currency_id = s.currency_id)
            THEN %(valid_from)s::date
            ELSE DATE '1900-01-01'
       END
FROM src AS s
WHERE NOT EXISTS (SELECT 1 FROM dds.dim_currency d WHERE d.currency_id = s.currency_id AND d.is_current);
