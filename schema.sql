-- 1. Reset: Drop tables if they already exist (Clean Slate)
DROP TABLE IF EXISTS lineups CASCADE;
DROP TABLE IF EXISTS player_stats CASCADE;
DROP TABLE IF EXISTS matches CASCADE;

-- 2. Create Matches Table (The Central Hub)
CREATE TABLE matches (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_score INT,
    away_score INT,
    home_xg NUMERIC,
    away_xg NUMERIC,
    -- Constraint: Prevent duplicate match entries
    CONSTRAINT unique_match_entry UNIQUE (date, home_team, away_team)
);

-- 3. Create Player Stats Table (The Engine)
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
    xa NUMERIC,
    xg_chain NUMERIC,
    xg_buildup NUMERIC,
    key_passes INT,
    yellow_card INT,
    red_card INT
);

-- 4. Create Lineups Table (Tactical Context)
CREATE TABLE lineups (
    id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(id) ON DELETE CASCADE,
    team TEXT NOT NULL,
    player_name TEXT NOT NULL,
    position TEXT,
    is_starter BOOLEAN,
    shots_on_target INT,
    fouls_committed INT,
    fouls_suffered INT,
    offsides INT,
    saves INT,
    goals_conceded INT
);