CREATE TABLE IF NOT EXISTS kafka_views
(
    post_id Int64,
    author_id Int64,
    user_id Int64,
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'view_topic',
    kafka_group_name = 'ch_view_group',
    kafka_format = 'CSV',
    kafka_num_consumers = 3;

CREATE TABLE IF NOT EXISTS views
(
    post_id Int64,
    author_id Int64,
    user_id Int64,
    timestamp DateTime,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (post_id, user_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_to_views TO views
AS SELECT
    post_id,
    author_id,
    user_id,
    now() AS timestamp
FROM kafka_views;
