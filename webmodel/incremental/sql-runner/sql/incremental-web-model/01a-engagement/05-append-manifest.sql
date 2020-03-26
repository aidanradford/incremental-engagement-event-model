-- 05. APPEND MANIFEST

ALTER TABLE {{.output_schema}}.engagement_manifest APPEND FROM {{.scratch_schema}}.etl_tstamps;
