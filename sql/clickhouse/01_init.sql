CREATE DATABASE IF NOT EXISTS cbr;

-- Re-exported rows for the same key are collapsed by the latest updated_at
CREATE TABLE IF NOT EXISTS cbr.currency_rate_daily
(
    rate_date       Date,
    currency_id     LowCardinality(String),
    char_code       LowCardinality(String),
    name            String,
    nominal         UInt32,
    value           Decimal(18, 4),
    vunit_rate      Decimal(18, 8),
    prev_vunit_rate Nullable(Decimal(18, 8)),
    change_pct      Nullable(Decimal(12, 4)),
    ma_7            Nullable(Decimal(18, 8)),
    ma_30           Nullable(Decimal(18, 8)),
    key_rate        Nullable(Decimal(6, 2)),
    updated_at      DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(rate_date)
ORDER BY (char_code, rate_date);
