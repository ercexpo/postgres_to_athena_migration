CREATE EXTERNAL TABLE IF NOT EXISTS web_historian.visits_orig
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
        id: string,
        url: string,
        properties_date: decimal(16,4),
        title: string,
        domain: string,
        visitId: string,
        transType: string,
        refVisitId: string,
        searchTerms: string,
        protocol: string,
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
PARTITIONED BY (created_week String)
STORED AS ORC
LOCATION 's3://web-historian-eu/web_historian/'
tblproperties ("orc.compress"="ZLIB");