-- Runs once on the first start of the postgres container (as superuser, db "airflow").
CREATE USER dwh WITH PASSWORD 'dwh';
CREATE DATABASE dwh OWNER dwh;

\connect dwh
SET ROLE dwh;

CREATE SCHEMA raw;   -- source responses as is
CREATE SCHEMA stg;   -- parsed, typed rows of each load
CREATE SCHEMA dds;   -- detail layer: facts and SCD2 dimensions
CREATE SCHEMA mart;  -- analytical marts (dbt)
CREATE SCHEMA meta;  -- data quality results

-- RAW ---------------------------------------------------------------------
CREATE TABLE raw.cbr_response (
    load_id        bigserial PRIMARY KEY,
    source         text        NOT NULL,          -- cbr_daily | cbr_valfull | cbr_key_rate
    request_params jsonb       NOT NULL,
    payload        text        NOT NULL,
    dag_run_id     text,
    loaded_at      timestamptz NOT NULL DEFAULT now()
);

-- STG ---------------------------------------------------------------------
CREATE TABLE stg.currency_rate (
    load_id     bigint        NOT NULL REFERENCES raw.cbr_response,
    rate_date   date          NOT NULL,
    currency_id text          NOT NULL,
    num_code    text,
    char_code   text,
    nominal     integer,
    name        text,
    value       numeric(18,4),
    vunit_rate  numeric(18,8)
);
CREATE INDEX ON stg.currency_rate (load_id);

CREATE TABLE stg.currency_ref (
    load_id       bigint  NOT NULL REFERENCES raw.cbr_response,
    currency_id   text    NOT NULL,
    name          text,
    eng_name      text,
    nominal       integer,
    parent_code   text,
    iso_num_code  integer,
    iso_char_code text
);
CREATE INDEX ON stg.currency_ref (load_id);

CREATE TABLE stg.key_rate (
    load_id   bigint       NOT NULL REFERENCES raw.cbr_response,
    rate_date date         NOT NULL,
    rate      numeric(6,2)
);
CREATE INDEX ON stg.key_rate (load_id);

-- DDS ---------------------------------------------------------------------
-- SCD Type 2: valid_from inclusive, valid_to exclusive
CREATE TABLE dds.dim_currency (
    currency_sk   bigserial PRIMARY KEY,
    currency_id   text    NOT NULL,
    char_code     text,
    name          text,
    eng_name      text,
    nominal       integer,
    parent_code   text,
    iso_num_code  integer,
    row_hash      text    NOT NULL,
    valid_from    date    NOT NULL,
    valid_to      date    NOT NULL DEFAULT DATE '9999-12-31',
    is_current    boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX dim_currency_current_uq ON dds.dim_currency (currency_id) WHERE is_current;
CREATE INDEX ON dds.dim_currency (currency_id, valid_from, valid_to);

CREATE TABLE dds.fct_currency_rate (
    rate_date   date          NOT NULL,
    currency_id text          NOT NULL,
    char_code   text          NOT NULL,
    num_code    text,
    name        text,
    nominal     integer       NOT NULL,
    value       numeric(18,4) NOT NULL,
    vunit_rate  numeric(18,8) NOT NULL,
    row_hash    text          NOT NULL,
    load_id     bigint        NOT NULL,
    updated_at  timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency_id)
);
CREATE INDEX ON dds.fct_currency_rate (currency_id, rate_date);

CREATE TABLE dds.fct_key_rate (
    rate_date  date         PRIMARY KEY,
    rate       numeric(6,2) NOT NULL,
    row_hash   text         NOT NULL,
    load_id    bigint       NOT NULL,
    updated_at timestamptz  NOT NULL DEFAULT now()
);

-- MART: tables and views in schema mart are built by dbt (see dbt/models/marts)

-- META --------------------------------------------------------------------
CREATE TABLE meta.dq_result (
    id           bigserial PRIMARY KEY,
    dataset      text        NOT NULL,
    check_name   text        NOT NULL,
    severity     text        NOT NULL,
    failed_rows  bigint      NOT NULL,
    passed       boolean     NOT NULL,
    load_id      bigint,
    dag_run_id   text,
    checked_at   timestamptz NOT NULL DEFAULT now()
);
