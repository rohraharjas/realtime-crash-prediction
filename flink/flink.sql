-- ==============================
-- Strict 16-frame tumbling window per key (non-overlapping)
-- ==============================

-- Source Table
CREATE TABLE image_frames (
    key STRING,
    frame_base64 STRING,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts
) WITH (
    'connector' = 'kafka',
    'topic' = '${input.topic}',
    'properties.bootstrap.servers' = '${bootstrap.servers}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${sasl.username}" password="${sasl.password}";',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

-- Sink Table
CREATE TABLE frame_batches (
    key STRING,
    batch_id BIGINT,
    batch_frames ARRAY<STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = '${flink.output.topic}',
    'properties.bootstrap.servers' = '${bootstrap.servers}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${sasl.username}" password="${sasl.password}";',
    'format' = 'json'
);

-- Strict 16-row tumbling batch using ROW_NUMBER and FLOOR
WITH numbered AS (
    SELECT
        key,
        frame_base64,
        ts,
        FLOOR((ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts)-1)/16) AS batch_id
    FROM image_frames
)
INSERT INTO frame_batches
SELECT
    key,
    batch_id,
    COLLECT_LIST(frame_base64) AS batch_frames
FROM numbered
GROUP BY key, batch_id;
