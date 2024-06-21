CREATE TABLE IF NOT EXISTS posts (
    post_id BIGSERIAL PRIMARY KEY,
    title TEXT,
    author_id INTEGER,
    content TEXT
)
