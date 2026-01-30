-- 1. Enable the AI extension
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Create the MATCHES table
CREATE TABLE IF NOT EXISTS matches (
    id SERIAL PRIMARY KEY,
    fixture_id INT UNIQUE NOT NULL,
    date TIMESTAMP NOT NULL,
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    home_score INT,
    away_score INT,
    status VARCHAR(50),
    raw_json JSONB
);

-- 3. Create the NEWS table
CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    url VARCHAR(255) UNIQUE NOT NULL,
    title TEXT,
    published_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    content_json JSONB,
    embedding VECTOR(1536)
);