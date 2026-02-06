-- =============================================================================
-- SPANISH FOOTBALL ANALYTICS - DATABASE SCHEMA (MULTI-SEASON)
-- =============================================================================
-- This file defines the structure for the PostgreSQL Data Warehouse.
-- It supports longitudinal analysis across multiple seasons.
-- =============================================================================

-- 1. CLEAN SLATE (Drop tables if they exist to prevent conflicts)
DROP TABLE IF EXISTS lineups CASCADE;
DROP TABLE IF EXISTS player_stats CASCADE;
DROP TABLE IF EXISTS matches CASCADE;

-- 2. FACT TABLE: MATCHES
-- Stores the core metadata for every game.
-- The composite unique key includes 'season' to allow multi-year storage.
CREATE TABLE matches (
    id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL,             -- e.g., '2023', '2022'
    date DATE NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_score INT,
    away_score INT,
    home_xg NUMERIC,                         -- Expected Goals (xG)
    away_xg NUMERIC,
    
    -- Constraint: A team cannot play another team twice on the same day in the same season
    UNIQUE(season, date, home_team, away_team)
);

-- 3. DIMENSION TABLE: PLAYER STATS
-- Stores granular offensive performance metrics for every player in every match.
CREATE TABLE player_stats (
    id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(id) ON DELETE CASCADE,
    team TEXT NOT NULL,
    player_name TEXT NOT NULL,
    minutes INT,
    goals INT,
    assists INT,
    shots INT,
    xg NUMERIC,
    xa NUMERIC,                             -- Expected Assists (xA)
    xg_chain NUMERIC,                       -- xG Chain (Build-up contribution)
    xg_buildup NUMERIC,
    key_passes INT,
    yellow_card INT,
    red_card INT
);

-- 4. DIMENSION TABLE: LINEUPS
-- Stores tactical lineup data (Starting XI, Substitutes, Defensive Actions).
CREATE TABLE lineups (
    id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(id) ON DELETE CASCADE,
    team TEXT NOT NULL,
    player_name TEXT NOT NULL,
    position TEXT,                          -- e.g., 'DC' (Center Back), 'FW' (Forward)
    is_starter BOOLEAN,
    shots_on_target INT,
    fouls_committed INT,
    fouls_suffered INT,
    offsides INT,
    saves INT,
    goals_conceded INT
);

-- INDEXES (Optional but recommended for speed)
CREATE INDEX idx_matches_season ON matches(season);
CREATE INDEX idx_matches_date ON matches(date);
CREATE INDEX idx_player_stats_name ON player_stats(player_name);