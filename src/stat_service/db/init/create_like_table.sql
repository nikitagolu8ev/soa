CREATE TABLE IF NOT EXISTS likes
(
    post_id Int32,
    user Int32,
    timestamp DateTime,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (post_id, user)
