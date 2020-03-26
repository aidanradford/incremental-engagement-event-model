
-- 3. SELECT ENGAGEMENT DIMENSIONS (PART 1)

-- 3a. create the table if it doesn't exist

CREATE TABLE IF NOT EXISTS {{.scratch_schema}}.engagement_temp (

  -- event fields
  event_id VARCHAR(255) ENCODE ZSTD,
  event_name VARCHAR(255) ENCODe ZSTD, 

  -- user fields
  user_id VARCHAR(255) ENCODE ZSTD,
  domain_userid VARCHAR(128) ENCODE ZSTD,
  network_userid VARCHAR(128) ENCODE ZSTD,

  -- session fields
  session_id CHAR(128) ENCODE ZSTD,
  session_index INT ENCODE ZSTD,

  -- timestamp fields
  dvce_created_tstamp TIMESTAMP ENCODE ZSTD,
  collector_tstamp TIMESTAMP ENCODE ZSTD,
  derived_tstamp TIMESTAMP ENCODE ZSTD,

  -- application fields
  app_id VARCHAR(255) ENCODE ZSTD,

  -- page fields
  page_title VARCHAR(2000) ENCODE ZSTD,
  page_url VARCHAR(4096) ENCODE ZSTD,
  page_urlhost VARCHAR(255) ENCODE ZSTD,
  page_urlpath VARCHAR(3000) ENCODE ZSTD,
  page_urlquery VARCHAR(6000) ENCODE ZSTD,

  -- referrer fields
  page_referrer VARCHAR(4096) ENCODE ZSTD,

  refr_urlscheme VARCHAR(16) ENCODE ZSTD,
  refr_urlhost VARCHAR(255) ENCODE ZSTD,
  refr_urlpath VARCHAR(6000) ENCODE ZSTD,
  refr_urlquery VARCHAR(6000) ENCODE ZSTD,

  refr_medium VARCHAR(25) ENCODE ZSTD,
  refr_source VARCHAR(50) ENCODE ZSTD,
  refr_term VARCHAR(255) ENCODE ZSTD,

  -- marketing fields
  mkt_source VARCHAR(255) ENCODE ZSTD,
  mkt_medium VARCHAR(255) ENCODE ZSTD,
  mkt_term VARCHAR(255) ENCODE ZSTD,
  mkt_content VARCHAR(500) ENCODE ZSTD,
  mkt_campaign VARCHAR(255) ENCODE ZSTD,
  mkt_clickid VARCHAR(128) ENCODE ZSTD,
  mkt_network VARCHAR(64) ENCODE ZSTD,

  -- geo fields
  geo_country CHAR(2) ENCODE ZSTD,
  geo_region CHAR(2) ENCODE ZSTD,
  geo_city VARCHAR(75) ENCODE ZSTD,
  geo_zipcode VARCHAR(15) ENCODE ZSTD,

  -- device fields
  dvce_type VARCHAR(50) ENCODE ZSTD,
  dvce_ismobile BOOLEAN ENCODE ZSTD,

  -- OS fields
  os_family VARCHAR(50) ENCODE ZSTD,
  os_timezone VARCHAR(255) ENCODE ZSTD,

  -- create "conversion-by" parameter
  conversion_flag BOOLEAN ENCODE ZSTD,
  rolling_sum_conversion_flag INT ENCODE ZSTD,
  by_conversion VARCHAR(255) ENCODE ZSTD
  )

DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (derived_tstamp);

-- 3b. truncate in case the previous run failed

TRUNCATE {{.scratch_schema}}.engagement_temp;

-- 3c. insert the dimensions for engagement events that have not been processed

INSERT INTO {{.scratch_schema}}.engagement_temp (

  WITH prep AS (

    SELECT

    -- event fields
      ev.event_id,
      ev.evemt_name, 

      CASE
        -- append product interactions here 
        WHEN ev.app_id='XXX' AND ev.event_name = 'XXX' THEN 'XXX'
        WHEN ev.app_id='YYY' AND  ev.event_name = 'YYY' THEN 'YYY'
      END event_type,

    -- user fields
      ev.user_id,
      ev.domain_userid,
      ev.network_userid,

    -- session fields 
      ev.domain_sessionid AS session_id,
      ev.domain_sessionidx AS session_index,

    -- timestamp fields
      ev.dvce_created_tstamp,
      ev.collector_tstamp,
      ev.derived_tstamp,

    -- application fields
      ev.app_id,

    -- page fields
      ev.page_title,
      ev.page_url,
      ev.page_urlhost,
      ev.page_urlpath,
      ev.page_urlquery,

    -- referral fields
      ev.page_referrer,

      ev.refr_urlscheme,
      ev.refr_urlhost,
      ev.refr_urlpath,
      ev.refr_urlquery,

      ev.refr_medium,
      ev.refr_source,
      ev.refr_term,

    -- marketing fields
      ev.mkt_source,
      ev.mkt_medium,
      ev.mkt_term,
      ev.mkt_content,
      ev.mkt_campaign,
      ev.mkt_clickid,
      ev.mkt_network,

    -- geographic fields
      ev.geo_country,
      ev.geo_region,
      ev.geo_city,
      ev.geo_zipcode,

    -- device fields
      ev.dvce_type,
      ev.dvce_ismobile,

    -- OS fields
      ev.os_family,
      ev.os_timezone,

      -- create "by_conversion" parameter
      CASE WHEN event_type = 'generic_conversion_event' THEN 1 ELSE 0 END conversion_flag,
      SUM(conversion_flag) OVER (PARTITION BY ev.domain_sessionid ORDER BY ev.derived_tstamp DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rolling_sum_conversion_flag,
      CONCAT(ev.user_id,rolling_sum_conversion_flag) by_conversion,

    FROM {{.input_schema}}.events AS ev 

    INNER JOIN {{.scratch_schema}}.event_ids AS ids
       ON ev.event_id = ids.event_id
      AND ev.collector_tstamp = ids.collector_tstamp

    WHERE event_name NOT IN('page_ping')

  ),

  -- dedupe step to help with multiple conversion events - select first of duplicated events
  SELECT * FROM (SELECT *, ROW_NUMBER () OVER (PARTITION BY event_id ORDER BY derived_tstamp) AS n FROM prep WHERE n = 1
  ;
