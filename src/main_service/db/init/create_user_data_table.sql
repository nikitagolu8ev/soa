CREATE TABLE IF NOT EXISTS user_data (
    user_id BIGSERIAL PRIMARY KEY,
    login TEXT,
    hashed_password TEXT,
    data TEXT
)
