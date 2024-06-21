CREATE TABLE IF NOT EXISTS user_data (
    user_id SERIAL PRIMARY KEY,
    login TEXT,
    hashed_password TEXT,
    data TEXT
)
