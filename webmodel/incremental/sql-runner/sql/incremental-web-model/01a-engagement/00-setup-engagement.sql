CREATE TABLE IF NOT EXISTS {{.output_schema}}.engagement (
  
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
  by_conversion VARCHAR(255) ENCODE ZSTD,

  CONSTRAINT event_id_pk PRIMARY KEY(event_id)
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (derived_tstamp);