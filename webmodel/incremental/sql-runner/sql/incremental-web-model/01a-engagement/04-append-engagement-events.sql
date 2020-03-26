-- 04. APPEND ENGAGEMENT EVENTS

ALTER TABLE {{.output_schema}}.engagement APPEND FROM {{.scratch_schema}}.engagement_temp;