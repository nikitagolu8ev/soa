CREATE TABLE IF NOT EXISTS likes
(
    post_id Int64,
    author_id Int64,
    user_id Int64,
    timestamp DateTime,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (post_id, user_id)
