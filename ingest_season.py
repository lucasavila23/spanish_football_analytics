import os
import time
import soccerdata as sd
import pandas as pd
import psycopg2
import unidecode
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIGURATION ---
LEAGUE = "ESP-La Liga"
SEASONS_TO_PROCESS = ["2023", "2022"]  # Added 2022 for your scaling test

load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "spanish_football"),
    "user": os.getenv("DB_USER", "runner"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost")
}

# --- HELPER FUNCTIONS ---
def normalize_name(name):
    if not isinstance(name, str): return name
    corrections = {
        "Deportivo Alavés": "Alaves", "Alavés": "Alaves",
        "UD Almería": "Almeria", "Almería": "Almeria",
        "Cádiz": "Cadiz", "Atlético de Madrid": "Atletico Madrid",
        "Atlético Madrid": "Atletico Madrid", "Athletic Club": "Athletic Club",
        "Girona FC": "Girona", "Granada CF": "Granada"
    }
    return corrections.get(name, name)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

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

def parse_espn_date(game_str):
    try: return game_str.split(' ')[0]
    except: return None

# New helper to safely convert NaN to 0
def safe_int(val):
    if pd.isna(val) or val == "":
        return 0
    try:
        return int(float(val))
    except:
        return 0

# --- MAIN ENGINE ---
def run_ingestion():
    total_start_time = time.time()
    print(f"\n--- STARTING MULTI-SEASON INGESTION: {SEASONS_TO_PROCESS} ---")

    conn = get_db_connection()
    cur = conn.cursor()

    for season in SEASONS_TO_PROCESS:
        season_start_time = time.time()
        print(f"\n>> PROCESSING SEASON: {season}")

        # 1. SCRAPE
        print("   [1/4] Scraping Data (This may take time)...")
        understat = sd.Understat(leagues=LEAGUE, seasons=season)
        espn = sd.ESPN(leagues=LEAGUE, seasons=season)

        ud_matches = understat.read_team_match_stats().reset_index()
        ud_players = standardize_columns(understat.read_player_match_stats().reset_index())
        
        espn_lineups = espn.read_lineup().reset_index()
        espn_lineups['date_str'] = espn_lineups['game'].apply(parse_espn_date)
        espn_lineups['team'] = espn_lineups['team'].apply(normalize_name)

        # 2. INSERT MATCHES
        print("   [2/4] Inserting Matches...")
        unique_games = ud_matches.groupby('game_id').first().reset_index()
        matches_to_insert = []
        
        for _, row in unique_games.iterrows():
            matches_to_insert.append((
                season, 
                row['date'].strftime('%Y-%m-%d'),
                row['home_team'],
                row['away_team'],
                int(row['home_goals']),
                int(row['away_goals']),
                float(row['home_xg']),
                float(row['away_xg'])
            ))

        match_sql = """
            INSERT INTO matches (season, date, home_team, away_team, home_score, away_score, home_xg, away_xg)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (season, date, home_team, away_team) DO NOTHING;
        """
        cur.executemany(match_sql, matches_to_insert)
        conn.commit()

        # 3. BUILD ID MAP
        cur.execute("SELECT id, date, home_team, away_team FROM matches WHERE season = %s", (season,))
        db_matches = cur.fetchall()
        match_map = {f"{str(m[1])}|{normalize_name(m[2])}|{normalize_name(m[3])}": m[0] for m in db_matches}

        # 4. INSERT PLAYER STATS
        print("   [3/4] Inserting Player Stats...")
        players_to_insert = []
        for _, row in ud_players.iterrows():
            game_meta = unique_games[unique_games['game_id'] == row['game_id']].iloc[0]
            g_date = game_meta['date'].strftime('%Y-%m-%d')
            h_team = normalize_name(game_meta['home_team'])
            a_team = normalize_name(game_meta['away_team'])
            
            m_id = match_map.get(f"{g_date}|{h_team}|{a_team}")
            
            if m_id:
                players_to_insert.append((
                    m_id, row['team'], row['player_name'], safe_int(row['minutes']),
                    safe_int(row['goals']), safe_int(row['assists']), safe_int(row['shots']),
                    float(row['xg']), float(row['xa']), float(row['xg_chain']),
                    float(row['xg_buildup']), safe_int(row['key_passes']),
                    safe_int(row['yellow_card']), safe_int(row['red_card'])
                ))
        
        p_sql = """
            INSERT INTO player_stats (match_id, team, player_name, minutes, goals, assists, shots, xg, xa, xg_chain, xg_buildup, key_passes, yellow_card, red_card)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(p_sql, players_to_insert)
        conn.commit()

        # 5. INSERT LINEUPS (With Suspended Match Fix)
        print("   [4/4] Inserting Lineups...")
        lineups_to_insert = []
        for _, game in unique_games.iterrows():
            g_date = game['date'].strftime('%Y-%m-%d')
            h_team = normalize_name(game['home_team'])
            a_team = normalize_name(game['away_team'])
            
            m_id = match_map.get(f"{g_date}|{h_team}|{a_team}")
            if not m_id: continue

            target_dates = [g_date]
            if h_team == "Granada" and a_team == "Athletic Club" and "2023-12" in g_date:
                target_dates = ["2023-12-10", "2023-12-11", "2023-12-12"]

            rows = espn_lineups[
                (espn_lineups['date_str'].isin(target_dates)) & 
                (espn_lineups['team'].isin([h_team, a_team]))
            ]

            for _, p in rows.iterrows():
                # Uses safe_int to handle NaNs/Nulls
                lineups_to_insert.append((
                    m_id, p['team'], p['player'], p['position'], (p['position'] != 'Substitute'),
                    safe_int(p['shots_on_target']), safe_int(p['fouls_committed']),
                    safe_int(p['fouls_suffered']), safe_int(p['offsides']),
                    safe_int(p['saves']), safe_int(p['goals_conceded'])
                ))

        l_sql = """
            INSERT INTO lineups (match_id, team, player_name, position, is_starter, shots_on_target, fouls_committed, fouls_suffered, offsides, saves, goals_conceded)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(l_sql, lineups_to_insert)
        conn.commit()
        
        print(f"   >> Season {season} done in {time.time() - season_start_time:.2f} seconds.")

    cur.close()
    conn.close()
    print(f"\nTOTAL TIME: {time.time() - total_start_time:.2f} seconds.")

if __name__ == "__main__":
    run_ingestion()