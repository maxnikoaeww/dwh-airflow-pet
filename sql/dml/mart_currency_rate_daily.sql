-- Rebuilds the mart from %(date_from)s on. Window functions look 60 days back,
-- so moving averages are correct for the first rebuilt dates too.
DELETE FROM mart.currency_rate_daily WHERE rate_date >= %(date_from)s::date;

INSERT INTO mart.currency_rate_daily
    (rate_date, currency_id, char_code, name, eng_name, nominal, value, vunit_rate,
     prev_vunit_rate, change_abs, change_pct, ma_7, ma_30, key_rate)
WITH base AS (
    SELECT f.rate_date,
           f.currency_id,
           coalesce(d.char_code, f.char_code) AS char_code,
           coalesce(d.name, f.name)           AS name,
           d.eng_name,
           f.nominal,
           f.value,
           f.vunit_rate
    FROM dds.fct_currency_rate AS f
    LEFT JOIN dds.dim_currency AS d             -- as-of join to the SCD2 version
           ON d.currency_id = f.currency_id
          AND f.rate_date >= d.valid_from
          AND f.rate_date <  d.valid_to
    WHERE f.rate_date >= %(date_from)s::date - 60
), calc AS (
    SELECT b.*,
           lag(vunit_rate) OVER w                                 AS prev_vunit_rate,
           avg(vunit_rate) OVER (w ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS ma_7,
           avg(vunit_rate) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30
    FROM base AS b
    WINDOW w AS (PARTITION BY currency_id ORDER BY rate_date)
)
SELECT c.rate_date, c.currency_id, c.char_code, c.name, c.eng_name, c.nominal, c.value, c.vunit_rate,
       c.prev_vunit_rate,
       c.vunit_rate - c.prev_vunit_rate,
       round(100 * (c.vunit_rate / nullif(c.prev_vunit_rate, 0) - 1), 4),
       round(c.ma_7, 8),
       round(c.ma_30, 8),
       k.rate
FROM calc AS c
LEFT JOIN LATERAL (
    SELECT kr.rate
    FROM dds.fct_key_rate AS kr
    WHERE kr.rate_date <= c.rate_date
    ORDER BY kr.rate_date DESC
    LIMIT 1
) AS k ON true
WHERE c.rate_date >= %(date_from)s::date;
