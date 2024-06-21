CREATE TABLE IF NOT EXISTS posts (
    post_id SERIAL PRIMARY KEY,
    title TEXT,
    author_id INTEGER,
    content TEXT
)
