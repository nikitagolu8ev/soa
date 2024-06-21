CREATE TABLE IF NOT EXISTS views
(
    post_id Int32,
    author_id Int32,
    user_id Int32,
    timestamp DateTime,
) ENGINE = ReplacingMergeTree
PRIMARY KEY (post_id, user_id)
