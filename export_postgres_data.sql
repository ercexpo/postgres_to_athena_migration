CREATE OR REPLACE TEMP VIEW v1 AS
select json_build_object(
    'id', id,
    'source', source,
    'generator', generator,
    'created_utc', to_char(created at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US000'),
    'created_str', to_char(created at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'),   --useless. kept for legacy reasons
    'generated_at', generated_at,
    'recorded_utc', to_char(recorded at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US000'),
    'recorded_str', to_char(recorded at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), --useless. kept for legacy reasons
    'properties', properties,
    'generator_identifier', generator_identifier,
    'secondary_identifier', secondary_identifier,
    'user_agent', user_agent,
    'server_generated', server_generated,
    'generator_definition_id', generator_definition_id,
    'source_reference_id', source_reference_id
)
from public.passive_data_kit_datapoint;
\copy (select * from v1) to './passive_data_kit_datapoint.json';