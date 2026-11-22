-- =====================================================================
-- Hive pipeline, INGEST step: parse ONE raw batch file inside Hive.
-- Rules: docs/PARSING_RULES.md. Called once per batch by runner.py.
--
-- Placeholders (runner.py fills them in):
--   __RAW_TABLE__      a temporary external table over this batch's file only
--   __INPUT_DIR__      directory holding the untouched batch file
--   __BATCH_ID__       1, 2, 3, ...
--   __STATS_OUT__      local directory where the batch's counts are written
--   __LINE_REGEX__     regex 1 (common/access_line.regex), escaped for HiveQL
--   __REQUEST_REGEX__  regex 2 (common/request.regex), escaped for HiveQL
-- The regexes are read from the shared files, never hand-copied.
-- =====================================================================

USE __DB__;
SET hive.compute.query.using.stats=false;
-- results cache off: see finalize.hql
SET hive.query.results.cache.enabled=false;

DROP TABLE IF EXISTS __RAW_TABLE__;

-- One line = one record. RegexSerDe applies regex 1 with Matcher.matches()
-- (a whole-line match). A line that does not match comes back as a row
-- whose 12 columns are all NULL. That is exactly "malformed" (section 5).
-- RegexSerDe has no field delimiter, so tabs or \u0001 inside a line are
-- just ordinary characters.
CREATE EXTERNAL TABLE __RAW_TABLE__ (
    host      STRING,
    ts_raw    STRING,
    dd        STRING,
    mon       STRING,
    yyyy      STRING,
    hh        STRING,
    mi        STRING,
    ss        STRING,
    tz        STRING,
    request   STRING,
    status    STRING,
    bytes     STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex' = '__LINE_REGEX__')
STORED AS TEXTFILE
LOCATION '__INPUT_DIR__';

-- Multi-insert: Hive reads the batch ONCE and writes two outputs.
--   1. parsed records into this batch's partition of the parsed table
--   2. the batch's counts into the stats table (used by finalize)
-- COUNT(*) counts every line. COUNT(host) counts only matched lines,
-- because host is NULL exactly when the line is malformed.
FROM (
    SELECT
        host, dd, yyyy, hh, mi, ss, request, status, bytes,
        -- month table by string lookup. No unix_timestamp / to_date: they
        -- apply the JVM timezone and locale (section 3). The regex only lets
        -- the 12 real abbreviations through, so ELSE can only be Dec.
        CASE mon
            WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
            WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
            WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
            WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' ELSE '12'
        END AS mm
    FROM __RAW_TABLE__
) r

INSERT INTO TABLE __PARSED_TABLE__ PARTITION (batch_id = __BATCH_ID__)
SELECT
    host,
    concat(yyyy, '-', mm, '-', dd, ' ', hh, ':', mi, ':', ss),
    concat(yyyy, '-', mm, '-', dd),
    CAST(hh AS INT),
    -- regex 2 on the request field. regexp_extract returns '' when the
    -- regex does not match or the group is empty, so NULLIF turns that
    -- into NULL (section 4).
    NULLIF(regexp_extract(request, '__REQUEST_REGEX__', 1), ''),
    NULLIF(regexp_extract(request, '__REQUEST_REGEX__', 2), ''),
    NULLIF(regexp_extract(request, '__REQUEST_REGEX__', 3), ''),
    CAST(status AS INT),
    CAST(IF(bytes = '-', '0', bytes) AS BIGINT)
WHERE host IS NOT NULL

INSERT INTO TABLE __STATS_TABLE__
SELECT
    __BATCH_ID__,
    COUNT(*),
    COUNT(host),
    COUNT(*) - COUNT(host);

-- The batch's counts as a text file for runner.py. A separate statement:
-- Hive 4 silently skips a LOCAL DIRECTORY target inside a multi-insert.
-- It reads one tiny row from the stats table, not the batch again.
INSERT OVERWRITE LOCAL DIRECTORY '__STATS_OUT__'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT batch_id, record_count, parsed_count, malformed_count
FROM __STATS_TABLE__
WHERE batch_id = __BATCH_ID__;

-- external table: dropping it only removes the definition. runner.py
-- deletes the batch file afterwards.
DROP TABLE __RAW_TABLE__;
