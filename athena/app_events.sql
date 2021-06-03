CREATE EXTERNAL TABLE IF NOT EXISTS web_historian.app_events
(
    id int,
    source string,
    generator string,
    created_utc timestamp,
    created_str string,
    generated_at string,
    recorded_utc timestamp,
    recorded_str string,
    properties struct<
        event_name: string,
        event_details: struct<
            step: int,
            session_id: string,
            count: int,
            domain_count: int,
            study: string,
            search_term_count: int
        >,
        properties_date: decimal(16,4),
        passive_data_metadata: struct<
            source: string,
            generator: string,
            generator_id: string,
            pdk_timestamp: decimal(16,7),
            encrypted_transmission: boolean
        >
    >,
    generator_identifier string,
    secondary_identifier string,
    user_agent string,
    server_generated boolean,
    generator_definition_id int,
    source_reference_id int
)
STORED AS ORC
LOCATION 's3://web-historian-eu/pdk_app_event/'
tblproperties ("orc.compress"="ZLIB");