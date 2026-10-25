-- Precious metal prices of the Bank of Russia (RUB per gram)
CREATE TABLE IF NOT EXISTS stg.metal_price (
    load_id    bigint        NOT NULL REFERENCES raw.cbr_response,
    price_date date          NOT NULL,
    metal_code smallint      NOT NULL,   -- 1 gold, 2 silver, 3 platinum, 4 palladium
    buy        numeric(12,2),
    sell       numeric(12,2)
);
CREATE INDEX IF NOT EXISTS metal_price_load_id_idx ON stg.metal_price (load_id);

CREATE TABLE IF NOT EXISTS dds.fct_metal_price (
    price_date date          NOT NULL,
    metal_code smallint      NOT NULL,
    buy        numeric(12,2) NOT NULL,
    sell       numeric(12,2) NOT NULL,
    row_hash   text          NOT NULL,
    load_id    bigint        NOT NULL,
    updated_at timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (price_date, metal_code)
);
