TRUNCATE {{.scratch_schema}}.etl_tstamps; -- step 1
TRUNCATE {{.scratch_schema}}.event_ids; -- step 2
TRUNCATE {{.scratch_schema}}.engagement_temp; -- step 3 