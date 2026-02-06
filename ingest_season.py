# ==============================================================================
# SPANISH FOOTBALL ANALYTICS - DATA INGESTION PIPELINE
# ==============================================================================
# This script performs the ETL (Extract, Transform, Load) process.
# ==============================================================================

import os
import soccerdata as sd
import pandas as pd
import psycopg2
import unidecode
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

# --- HELPER FUNCTION: NAME NORMALIZATION ---
def normalize_name(name):
    """
    Standardizes team names between ESPN (Accents) and Understat (Plain text).
    """
    if not isinstance(name, str):
        return name
        
    corrections = {
        "Deportivo Alavés": "Alaves",
        "Alavés": "Alaves",
        "UD Almería": "Almeria",
        "Almería": "Almeria",
        "Cádiz": "Cadiz",
        "Atlético de Madrid": "Atletico Madrid",
        "Atlético Madrid": "Atletico Madrid",
        "Athletic Club": "Athletic Club",
        "Real Betis": "Real Betis",
        "Rayo Vallecano": "Rayo Vallecano",
        "Girona FC": "Girona",
        "Granada CF": "Granada"
    }
    return corrections.get(name, name)

# --- CONFIGURATION ---
LEAGUE = "ESP-La Liga"
SEASON = "2023"

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "spanish_football"),
    "user": os.getenv("DB_USER", "runner"),
    "password": os.getenv("DB_PASSWORD", "football_password"),
    "host": os.getenv("DB_HOST", "localhost")
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def parse_espn_date(game_str):
    try:
        return game_str.split(' ')[0]
    except:
        return None

def standardize_columns(df):
    df.columns = [c.lower() for c in df.columns]
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
    for target, variations in mappings.items():
        if target in df.columns: continue
        for v in variations:
            if v in df.columns:
                df.rename(columns={v: target}, inplace=True)
                break
    return df

# ==============================================================================
# MAIN EXECUTION PIPELINE
# ==============================================================================

def run_ingestion():
    print(f"\n--- STARTING INGESTION PIPELINE: {LEAGUE} {SEASON} ---")
    
    # 1. INITIALIZE SCRAPERS
    print("[1/4] Scraping Data...")
    understat = sd.Understat(leagues=LEAGUE, seasons=SEASON)
    espn = sd.ESPN(leagues=LEAGUE, seasons=SEASON)

    print("   -> Fetching Understat Matches...")
    ud_matches = understat.read_team_match_stats().reset_index()
    
    print("   -> Fetching Understat Player Stats...")
    ud_players = understat.read_player_match_stats().reset_index()
    ud_players = standardize_columns(ud_players)

    print("   -> Fetching ESPN Lineups...")
    espn_lineups = espn.read_lineup().reset_index()
    espn_lineups['date_str'] = espn_lineups['game'].apply(parse_espn_date)
    
    print("   -> Normalizing Team Names...")
    espn_lineups['team'] = espn_lineups['team'].apply(normalize_name)

    # 2. INSERT MATCHES
    print("\n[2/4] Inserting Matches...")
    conn = get_db_connection()
    cur = conn.cursor()

    unique_games = ud_matches.groupby('game_id').first().reset_index()
    
    matches_to_insert = []
    for _, row in unique_games.iterrows():
        match_tuple = (
            row['date'].strftime('%Y-%m-%d'),
            row['home_team'],
            row['away_team'],
            int(row['home_goals']),
            int(row['away_goals']),
            float(row['home_xg']),
            float(row['away_xg'])
        )
        matches_to_insert.append(match_tuple)

    insert_match_sql = """
        INSERT INTO matches (date, home_team, away_team, home_score, away_score, home_xg, away_xg)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, home_team, away_team) DO NOTHING;
    """
    cur.executemany(insert_match_sql, matches_to_insert)
    conn.commit()
    print(f"   [OK] Inserted/Checked {len(matches_to_insert)} matches.")

    # 3. CREATE ID MAPPING
    cur.execute("SELECT id, home_team, away_team FROM matches")
    db_matches = cur.fetchall()
    
    match_map = {}
    for m in db_matches:
        h_team = normalize_name(m[1])
        a_team = normalize_name(m[2])
        key = f"{h_team}|{a_team}"
        match_map[key] = m[0]

    # 4. INSERT PLAYER STATS
    print("\n[3/4] Linking & Inserting Player Stats...")
    players_to_insert = []

    for _, row in ud_players.iterrows():
        match_meta = unique_games[unique_games['game_id'] == row['game_id']].iloc[0]
        h_key = normalize_name(match_meta['home_team'])
        a_key = normalize_name(match_meta['away_team'])
        match_id = match_map.get(f"{h_key}|{a_key}")
        
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

    # 5. INSERT LINEUPS (With Suspended Match Fix)
    print("\n[4/4] Linking & Inserting Lineups...")
    lineups_to_insert = []

    for _, game in unique_games.iterrows():
        g_date = game['date'].strftime('%Y-%m-%d')
        h_team = normalize_name(game['home_team'])
        a_team = normalize_name(game['away_team'])
        
        m_id = match_map.get(f"{h_team}|{a_team}")
        if not m_id: continue

        # --- FIX FOR SUSPENDED MATCH (Granada vs Athletic) ---
        target_dates = [g_date]
        # If it's the specific Granada/Athletic match in Dec 2023, check all 3 possible dates
        if h_team == "Granada" and a_team == "Athletic Club" and "2023-12" in g_date:
             # print(f"   [FIX] Applying date patch for {h_team} vs {a_team}")
             target_dates = ["2023-12-10", "2023-12-11", "2023-12-12"]
        # -----------------------------------------------------

        # Filter ESPN Data using the list of possible dates
        rows = espn_lineups[
            (espn_lineups['date_str'].isin(target_dates)) & 
            (espn_lineups['team'].isin([h_team, a_team]))
        ]
        
        for _, p in rows.iterrows():
            l_tuple = (
                m_id,
                p['team'],
                p['player'],
                p['position'],
                (p['position'] != 'Substitute'),
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

    cur.close()
    conn.close()
    print("\nPIPELINE COMPLETE.")

if __name__ == "__main__":
    run_ingestion()