-- Este script solo se ejecuta cuando el volumen postgres-data está vacío.
-- La base airflow (metadatos) se crea con POSTGRES_DB; warehouse está aislada.
CREATE DATABASE warehouse;
\connect warehouse

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.ingestion_batches (
    batch_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_sha256 char(64) NOT NULL UNIQUE,
    source_name text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE raw.customer_transactions (
    batch_id bigint NOT NULL REFERENCES raw.ingestion_batches(batch_id),
    source_row_number bigint NOT NULL CHECK (source_row_number >= 2),
    transaction_id text,
    customer_id text,
    transaction_date text,
    product_id text,
    product_name text,
    quantity text,
    price text,
    tax text,
    PRIMARY KEY (batch_id, source_row_number)
);

CREATE INDEX customer_transactions_batch_idx
    ON raw.customer_transactions (batch_id);