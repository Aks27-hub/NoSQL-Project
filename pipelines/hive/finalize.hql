-- =====================================================================
-- Hive pipeline, FINALIZE step: Q1, Q2, Q3 over EVERY batch ingested in
-- this run (docs/PARSING_RULES.md section 7). Called once per run.
--
-- Placeholders:
--   __PARSED_TABLE__  parsed records of all batches (written by ingest.hql)
--   __STATS_TABLE__   one row of counts per batch  (written by ingest.hql)
--   __OUT__           local directory where the results are written as
--                     tab-separated text for runner.py
-- =====================================================================

USE __DB__;
-- always scan the data: never answer COUNT(*) from table statistics
SET hive.compute.query.using.stats=false;
-- Hive 4 query results cache off: it can capture an INSERT OVERWRITE LOCAL
-- DIRECTORY into its own cache folder (no output for runner.py), and a
-- cached answer would make a benchmark run look faster than it is.
SET hive.query.results.cache.enabled=false;

-- ---------------------------------------------------------------- totals
-- Counted by Hive from its own staging tables, not passed in from Python.
INSERT OVERWRITE LOCAL DIRECTORY '__OUT__/batch_totals'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    COUNT(*),
    SUM(record_count),
    SUM(parsed_count),
    SUM(malformed_count)
FROM __STATS_TABLE__;

-- The parsed records actually present in staging. runner.py checks that
-- this equals the sum of the per-batch parsed counts, so a lost batch
-- cannot go unnoticed.
INSERT OVERWRITE LOCAL DIRECTORY '__OUT__/staged_totals'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT COUNT(*) FROM __PARSED_TABLE__;

-- ---------------------------------------------------------------- Q1
-- Daily Traffic Summary: GROUP BY log_date, status_code. No filters.
INSERT OVERWRITE LOCAL DIRECTORY '__OUT__/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    status_code,
    COUNT(*)                AS request_count,
    SUM(bytes_transferred)  AS total_bytes
FROM __PARSED_TABLE__
GROUP BY log_date, status_code;

-- ---------------------------------------------------------------- Q2
-- Top 20 resources. Only records with a resource_path, any method/status.
-- Tie-break on resource_path: Hive compares STRINGs byte by byte in
-- UTF-8, which is code point order with no locale (section 6).
-- rank_no is computed in Hive, so the row order of the output file
-- does not matter.
-- The path is written base64-encoded (UTF-8 bytes): a path containing a
-- tab or a newline would otherwise break the tab-separated output.
INSERT OVERWRITE LOCAL DIRECTORY '__OUT__/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    ROW_NUMBER() OVER (ORDER BY request_count DESC, resource_path ASC) AS rank_no,
    base64(encode(resource_path, 'UTF-8'))                            AS resource_path_b64,
    request_count,
    total_bytes,
    distinct_host_count
FROM (
    SELECT
        resource_path,
        COUNT(*)              AS request_count,
        SUM(bytes_transferred) AS total_bytes,
        COUNT(DISTINCT host)  AS distinct_host_count
    FROM __PARSED_TABLE__
    WHERE resource_path IS NOT NULL
    GROUP BY resource_path
    ORDER BY request_count DESC, resource_path ASC
    LIMIT 20
) top20;

-- ---------------------------------------------------------------- Q3
-- Hourly Error Analysis over ALL parsed records. Error = 400..599.
-- Only (date, hour) pairs that have requests appear (no gap filling).
-- COUNT(DISTINCT ...) ignores NULLs, so only hosts of error records count.
INSERT OVERWRITE LOCAL DIRECTORY '__OUT__/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    log_date,
    log_hour,
    SUM(IF(status_code BETWEEN 400 AND 599, 1, 0))                        AS error_request_count,
    COUNT(*)                                                              AS total_request_count,
    SUM(IF(status_code BETWEEN 400 AND 599, 1, 0)) / COUNT(*)             AS error_rate,
    COUNT(DISTINCT IF(status_code BETWEEN 400 AND 599, host, NULL))       AS distinct_error_hosts
FROM __PARSED_TABLE__
GROUP BY log_date, log_hour;
