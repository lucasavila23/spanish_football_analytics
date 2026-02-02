# ==============================================================================
# SPANISH FOOTBALL ANALYTICS - DATA INGESTION PIPELINE
# ==============================================================================
# This script performs the ETL (Extract, Transform, Load) process for the project.
# 
# 1. EXTRACT: Scrapes data from Understat (Metrics) and ESPN (Tactics) using 'soccerdata'.
# 2. TRANSFORM: Normalizes column names, cleans dates, and resolves entity links.
# 3. LOAD: Inserts the processed data into the PostgreSQL relational database.
# ==============================================================================

import soccerdata as sd       # Library for scraping football data from various sources
import pandas as pd           # Library for data manipulation (DataFrames)
import psycopg2               # Library for connecting to PostgreSQL database
import numpy as np            # Math library, used here for handling NaN (empty) values
from datetime import datetime # Library for handling date objects

# --- CONFIGURATION CONSTANTS ---
# Defines which league and season we are extracting.
LEAGUE = "ESP-La Liga"
SEASON = "2023"

# Database connection parameters (Credentials)
DB_CONFIG = {
    "dbname": "spanish_football",
    "user": "runner",
    "password": "football_password",
    "host": "localhost"
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_db_connection():
    """
    Establishes a connection to the local PostgreSQL database.
    Returns:
        conn: A psycopg2 connection object used to execute SQL commands.
    """
    return psycopg2.connect(**DB_CONFIG)

def parse_espn_date(game_str):
    """
    Parses the raw date string from ESPN.
    ESPN often returns dates like '2023-08-12 21:00:00'. 
    We only need the 'YYYY-MM-DD' part to link it with Understat data.
    """
    try:
        # Split by space and take the first part (the date)
        return game_str.split(' ')[0]
    except:
        # If the date is missing or malformed, return None to avoid crashing
        return None

def standardize_columns(df):
    """
    CRITICAL FUNCTION: Normalizes column names across different data sources.
    Different scrapers use different names for the same concept (e.g., 'xG' vs 'xg').
    This function converts everything to lowercase snake_case for consistency.
    """
    # 1. Convert all existing columns to lowercase (e.g., 'xG' -> 'xg')
    df.columns = [c.lower() for c in df.columns]
    
    # 2. Define a dictionary of {Target_Name: [List of possible source names]}
    mappings = {
        'team': ['team', 'team_title', 'team_name'],
        'player_name': ['player', 'player_name', 'name'],
        'minutes': ['time', 'minutes', 'min'],
        'xg': ['xg', 'expected_goals'],
        'xa': ['xa', 'expected_assists'],
        'xg_chain': ['xg_chain', 'xgchain', 'xgchain_value'],
        'xg_buildup': ['xg_buildup', 'xgbuildup', 'xgbuildup_value'],
        'key_passes': ['key_passes', 'kp'],
        'yellow_card': ['yellow_card', 'yellow_cards', 'yellow'],
        'red_card': ['red_card', 'red_cards', 'red']
    }

    # 3. Iterate through mappings and rename columns if variations are found
    for target, variations in mappings.items():
        if target in df.columns:
            continue # If the correct name exists, skip
            
        for v in variations:
            if v in df.columns:
                print(f"   [INFO] Renaming column '{v}' -> '{target}'")
                df.rename(columns={v: target}, inplace=True)
                break
    
    return df

# ==============================================================================
# MAIN EXECUTION PIPELINE
# ==============================================================================

def run_ingestion():
    print(f"\n--- STARTING INGESTION PIPELINE: {LEAGUE} {SEASON} ---")
    print("=" * 60)

    # ------------------------------------------------------------------
    # STEP 1: INITIALIZE SCRAPERS AND EXTRACT DATA
    # ------------------------------------------------------------------
    print("[1/4] Scraping Data (This may take a moment)...")
    
    # Initialize the scraper classes for the specific league/season
    understat = sd.Understat(leagues=LEAGUE, seasons=SEASON)
    espn = sd.ESPN(leagues=LEAGUE, seasons=SEASON)

    # Fetch Match Data (Scores, Dates, Teams)
    print("   -> Fetching Understat Matches...")
    # .reset_index() moves the MultiIndex (common in pandas) into regular columns
    ud_matches = understat.read_team_match_stats().reset_index()
    
    # Fetch Player Data (xG, xA, Shots, etc.)
    print("   -> Fetching Understat Player Stats...")
    ud_players = understat.read_player_match_stats().reset_index()
    # Apply our standardization helper to fix column name issues immediately
    ud_players = standardize_columns(ud_players)

    # Fetch Tactical Data (Lineups, Positions)
    print("   -> Fetching ESPN Lineups...")
    espn_lineups = espn.read_lineup().reset_index()

    # Create a clean 'date_str' column in ESPN data for joining later
    espn_lineups['date_str'] = espn_lineups['game'].apply(parse_espn_date)

    # ------------------------------------------------------------------
    # STEP 2: LOAD MATCHES INTO DATABASE
    # ------------------------------------------------------------------
    print("\n[2/4] Inserting Matches...")
    conn = get_db_connection()
    cur = conn.cursor()

    # Understat returns 2 rows per match (one for Home, one for Away).
    # We group by 'game_id' and take the first one to get unique matches.
    unique_games = ud_matches.groupby('game_id').first().reset_index()
    
    matches_to_insert = []
    # Iterate through every unique match and prepare the tuple for SQL
    for _, row in unique_games.iterrows():
        match_tuple = (
            row['date'].strftime('%Y-%m-%d'), # Convert Timestamp to String
            row['home_team'],
            row['away_team'],
            int(row['home_goals']),
            int(row['away_goals']),
            float(row['home_xg']),
            float(row['away_xg'])
        )
        matches_to_insert.append(match_tuple)

    # SQL Query: Uses 'ON CONFLICT DO NOTHING' to prevent duplicate errors 
    # if we run the script multiple times.
    insert_match_sql = """
        INSERT INTO matches (date, home_team, away_team, home_score, away_score, home_xg, away_xg)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, home_team, away_team) DO NOTHING;
    """
    
    # executemany() is much faster than running INSERT for each row individually
    cur.executemany(insert_match_sql, matches_to_insert)
    conn.commit() # Save changes to the database
    print(f"   [OK] Inserted {len(matches_to_insert)} matches.")

    # ------------------------------------------------------------------
    # STEP 3: CREATE ID MAPPING (The Bridge)
    # ------------------------------------------------------------------
    # The Database generates unique IDs (1, 2, 3...) for each match.
    # We need to fetch these IDs back into Python to link Players to Matches.
    cur.execute("SELECT id, date, home_team FROM matches")
    db_matches = cur.fetchall()
    
    # Create a dictionary map: 
    # Key: "2023-08-12|Athletic Club" -> Value: 15 (The SQL ID)
    match_map = {f"{str(m[1])}|{m[2]}": m[0] for m in db_matches}

    # ------------------------------------------------------------------
    # STEP 4: LOAD PLAYER STATS (The Engine)
    # ------------------------------------------------------------------
    print("\n[3/4] Linking & Inserting Player Stats...")
    players_to_insert = []

    # Iterate through the raw player data
    for _, row in ud_players.iterrows():
        # 1. Identify which match this player row belongs to
        # We find the match details in 'unique_games' using the 'game_id'
        match_meta = unique_games[unique_games['game_id'] == row['game_id']].iloc[0]
        
        # 2. Create the Lookup Key (Date + HomeTeam)
        match_key = f"{match_meta['date'].strftime('%Y-%m-%d')}|{match_meta['home_team']}"
        
        # 3. Retrieve the SQL ID from our map
        match_id = match_map.get(match_key)
        
        # Only proceed if we found a valid match ID
        if match_id:
            p_tuple = (
                match_id,
                row['team'],
                row['player_name'],
                int(row['minutes']),
                int(row['goals']),
                int(row['assists']),
                int(row['shots']),
                float(row['xg']),
                float(row['xa']),
                float(row['xg_chain']),
                float(row['xg_buildup']),
                int(row['key_passes']),
                int(row['yellow_card']),
                int(row['red_card'])
            )
            players_to_insert.append(p_tuple)

    insert_player_sql = """
        INSERT INTO player_stats 
        (match_id, team, player_name, minutes, goals, assists, shots, xg, xa, xg_chain, xg_buildup, key_passes, yellow_card, red_card)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_player_sql, players_to_insert)
    conn.commit()
    print(f"   [OK] Inserted {len(players_to_insert)} player performance records.")

    # ------------------------------------------------------------------
    # STEP 5: LOAD LINEUPS (The Tactics)
    # ------------------------------------------------------------------
    print("\n[4/4] Linking & Inserting Lineups...")
    lineups_to_insert = []

    # Iterate through matches to find corresponding lineups
    for _, game in unique_games.iterrows():
        g_date = game['date'].strftime('%Y-%m-%d')
        h_team = game['home_team']
        a_team = game['away_team']
        
        # Retrieve SQL ID
        m_id = match_map.get(f"{g_date}|{h_team}")
        if not m_id: continue

        # ESPN data is not linked by ID, but by Date and Team Name.
        # We fetch rows where Date matches AND Team is either Home or Away.
        
        # 1. Get Home Team Players
        h_rows = espn_lineups[(espn_lineups['date_str'] == g_date) & (espn_lineups['team'] == h_team)]
        # 2. Get Away Team Players
        a_rows = espn_lineups[(espn_lineups['date_str'] == g_date) & (espn_lineups['team'] == a_team)]
        
        # Combine them into one list for this match
        all_rows = pd.concat([h_rows, a_rows])
        
        for _, p in all_rows.iterrows():
            # Prepare the tuple. 
            # Note: We use checks like 'if not pd.isna' to handle missing data (NaN) 
            # by defaulting to 0.
            l_tuple = (
                m_id,
                p['team'],
                p['player'],
                p['position'],
                (p['position'] != 'Substitute'), # True if starter, False if sub
                int(p['shots_on_target'] if not pd.isna(p['shots_on_target']) else 0),
                int(p['fouls_committed'] if not pd.isna(p['fouls_committed']) else 0),
                int(p['fouls_suffered'] if not pd.isna(p['fouls_suffered']) else 0),
                int(p['offsides'] if not pd.isna(p['offsides']) else 0),
                int(p['saves'] if not pd.isna(p['saves']) else 0),
                int(p['goals_conceded'] if not pd.isna(p['goals_conceded']) else 0)
            )
            lineups_to_insert.append(l_tuple)

    insert_lineup_sql = """
        INSERT INTO lineups 
        (match_id, team, player_name, position, is_starter, shots_on_target, fouls_committed, fouls_suffered, offsides, saves, goals_conceded)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_lineup_sql, lineups_to_insert)
    conn.commit()
    print(f"   [OK] Inserted {len(lineups_to_insert)} tactical lineup entries.")

    # ------------------------------------------------------------------
    # CLEANUP
    # ------------------------------------------------------------------
    cur.close()
    conn.close()
    print("\nPIPELINE COMPLETE.")

# Standard boilerplate to run the function if this file is executed directly
if __name__ == "__main__":
    run_ingestion()