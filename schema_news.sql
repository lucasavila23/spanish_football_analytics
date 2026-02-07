-- Table to store news headlines linked to matches
CREATE TABLE IF NOT EXISTS news_headlines (
    id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(id) ON DELETE CASCADE,
    url VARCHAR(500) UNIQUE NOT NULL,
    headline TEXT,
    subheader TEXT,
    source VARCHAR(50) DEFAULT 'Marca',
    scraped_at TIMESTAMP DEFAULT NOW()
);

-- Index for fast lookups by match
CREATE INDEX IF NOT EXISTS idx_news_match_id ON news_headlines(match_id);