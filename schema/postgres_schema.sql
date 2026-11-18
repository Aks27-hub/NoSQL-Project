-- =====================================================================
-- NASA Log ETL - Reporting Database Schema (PostgreSQL)
-- Owner: Member 3 | Contract version: 2.0
--
--   runs       one row per tool execution (one pipeline, one batch size)
--   batches    one row per ingested batch (record / parsed / malformed counts)
--   q1/q2/q3   FINAL query results computed over ALL batches of a run
--
-- Each result row carries pipeline_name, run_id, batch_id and executed_at,
-- as the project statement requires. In the result tables, batch_id is the
-- last batch included in the final aggregation: the results cover batches
-- 1..batch_id, which is every batch of the run.
--
-- Usage:  psql -U <user> -d nasa_etl -f postgres_schema.sql
-- =====================================================================

DROP VIEW  IF EXISTS v_run_summary;
DROP TABLE IF EXISTS q3_hourly_errors;
DROP TABLE IF EXISTS q2_top_resources;
DROP TABLE IF EXISTS q1_daily_traffic;
DROP TABLE IF EXISTS batches;
DROP TABLE IF EXISTS runs;

-- ---------------------------------------------------------------------
-- runs: registered by start_run() BEFORE the runtime timer starts.
-- status lifecycle:  RUNNING --save_final_results--> FINALIZED --finish_run--> SUCCESS
--                    RUNNING / FINALIZED --finish_run(FAILED)--> FAILED
-- Batches can only be added while RUNNING; final results only once.
-- ---------------------------------------------------------------------
CREATE TABLE runs (
    run_id                     VARCHAR(64)   PRIMARY KEY,
    pipeline_name              VARCHAR(16)   NOT NULL
                               CHECK (pipeline_name IN ('mapreduce','pig','hive','mongodb')),
    batch_size                 INTEGER       NOT NULL CHECK (batch_size > 0),   -- configured size
    input_files                TEXT          NOT NULL,                          -- comma-separated
    status                     VARCHAR(16)   NOT NULL DEFAULT 'RUNNING'
                               CHECK (status IN ('RUNNING','FINALIZED','SUCCESS','FAILED')),
    started_at                 TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finalized_at               TIMESTAMP     NULL,  -- set when final results are committed
    finished_at                TIMESTAMP     NULL,
    runtime_seconds            NUMERIC(12,3) NULL CHECK (runtime_seconds >= 0),  -- input read start -> final results committed
    final_aggregation_seconds  NUMERIC(12,3) NULL CHECK (final_aggregation_seconds >= 0),  -- time of the global query step (informational)
    error_message              TEXT          NULL
);

-- ---------------------------------------------------------------------
-- batches: one row per ingested batch. record_count = raw lines in the batch.
-- The last batch may be smaller than batch_size and is still a valid batch.
-- ---------------------------------------------------------------------
CREATE TABLE batches (
    run_id                  VARCHAR(64)   NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    batch_id                INTEGER       NOT NULL CHECK (batch_id >= 1),
    pipeline_name           VARCHAR(16)   NOT NULL,
    record_count            INTEGER       NOT NULL CHECK (record_count >= 0),
    parsed_count            INTEGER       NOT NULL CHECK (parsed_count >= 0),
    malformed_count         INTEGER       NOT NULL CHECK (malformed_count >= 0),
    batch_runtime_seconds   NUMERIC(12,3) NULL CHECK (batch_runtime_seconds >= 0),
    executed_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, batch_id),
    CHECK (parsed_count + malformed_count = record_count)
);

-- ---------------------------------------------------------------------
-- Query 1: Daily Traffic Summary (global, over the whole run)
-- ---------------------------------------------------------------------
CREATE TABLE q1_daily_traffic (
    run_id          VARCHAR(64)  NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    batch_id        INTEGER      NOT NULL,      -- last batch covered by the final aggregation
    pipeline_name   VARCHAR(16)  NOT NULL,
    executed_at     TIMESTAMP    NOT NULL,
    log_date        DATE         NOT NULL,
    status_code     SMALLINT     NOT NULL,
    request_count   BIGINT       NOT NULL,
    total_bytes     BIGINT       NOT NULL,
    PRIMARY KEY (run_id, log_date, status_code)
);

-- ---------------------------------------------------------------------
-- Query 2: Top 20 Requested Resources (global)
-- rank_no 1..20 after ORDER BY request_count DESC, resource_path ASC
-- ---------------------------------------------------------------------
CREATE TABLE q2_top_resources (
    run_id               VARCHAR(64)  NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    batch_id             INTEGER      NOT NULL,
    pipeline_name        VARCHAR(16)  NOT NULL,
    executed_at          TIMESTAMP    NOT NULL,
    rank_no              SMALLINT     NOT NULL CHECK (rank_no BETWEEN 1 AND 20),
    resource_path        TEXT         NOT NULL,
    request_count        BIGINT       NOT NULL,
    total_bytes          BIGINT       NOT NULL,
    distinct_host_count  BIGINT       NOT NULL,
    PRIMARY KEY (run_id, rank_no)
);

-- ---------------------------------------------------------------------
-- Query 3: Hourly Error Analysis (global)
-- ---------------------------------------------------------------------
CREATE TABLE q3_hourly_errors (
    run_id                VARCHAR(64)   NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    batch_id              INTEGER       NOT NULL,
    pipeline_name         VARCHAR(16)   NOT NULL,
    executed_at           TIMESTAMP     NOT NULL,
    log_date              DATE          NOT NULL,
    log_hour              SMALLINT      NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
    error_request_count   BIGINT        NOT NULL,
    total_request_count   BIGINT        NOT NULL CHECK (total_request_count > 0),
    error_rate            NUMERIC(9,6)  NOT NULL CHECK (error_rate BETWEEN 0 AND 1),
    distinct_error_hosts  BIGINT        NOT NULL,
    PRIMARY KEY (run_id, log_date, log_hour)
);

CREATE INDEX idx_runs_pipeline ON runs(pipeline_name, started_at);

-- ---------------------------------------------------------------------
-- Run header for the Reporting Module.
-- avg_batch_size = total records processed / number of non-empty batches
-- ---------------------------------------------------------------------
CREATE VIEW v_run_summary AS
SELECT
    r.run_id,
    r.pipeline_name,
    r.status,
    r.batch_size                                   AS configured_batch_size,
    r.started_at,
    r.finalized_at,
    r.finished_at,
    r.runtime_seconds,
    r.final_aggregation_seconds,
    COUNT(b.batch_id)                              AS num_batches,
    COALESCE(SUM(b.record_count), 0)               AS total_records,
    COALESCE(SUM(b.parsed_count), 0)               AS total_parsed,
    COALESCE(SUM(b.malformed_count), 0)            AS total_malformed,
    ROUND(
        COALESCE(SUM(b.record_count), 0)::NUMERIC
        / NULLIF(COUNT(b.batch_id) FILTER (WHERE b.record_count > 0), 0),
        2)                                         AS avg_batch_size
FROM runs r
LEFT JOIN batches b ON b.run_id = r.run_id
GROUP BY r.run_id, r.pipeline_name, r.status, r.batch_size, r.started_at,
         r.finalized_at, r.finished_at, r.runtime_seconds, r.final_aggregation_seconds;
