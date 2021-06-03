CREATE EXTERNAL TABLE IF NOT EXISTS web_historian.web_historian_behavior_data
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
        passive_data_metadata: struct<
            source: string,
            generator: string,
            generator_id: string,
            pdk_timestamp: decimal(16,7),
            encrypted_transmission: boolean
        >,
        web_historian_server: struct<
            visits: int,
            domains: int,
            searches: int
        >,
        source_info: struct<
            visits: int,
            domains: int,
            searches: int,
            source: string
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
LOCATION 's3://web-historian-eu/web_historian_behavior_data/'
tblproperties ("orc.compress"="ZLIB");