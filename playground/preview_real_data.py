import soccerdata as sd
import pandas as pd

# --- CONFIG ---
LEAGUE = "ESP-La Liga"
SEASON = "2023"

def parse_espn_date(game_str):
    try:
        return game_str.split(' ')[0]
    except:
        return None

print(f" STARTING LIVE DATA PREVIEW for {LEAGUE} {SEASON}...")

# 1. Initialize Scrapers (Real connection)
print(" Connecting to APIs (this may take a moment)...")
understat = sd.Understat(leagues=LEAGUE, seasons=SEASON)
espn = sd.ESPN(leagues=LEAGUE, seasons=SEASON)

# 2. Fetch Match Data
print(" Fetching Match Stats...")
matches_df = understat.read_team_match_stats().reset_index()
players_df = understat.read_player_match_stats().reset_index()

# --- SELECT THE FIRST MATCH FOUND ---
test_game_id = matches_df['game_id'].unique()[0]
match_row = matches_df[matches_df['game_id'] == test_game_id].iloc[0]
game_players = players_df[players_df['game_id'] == test_game_id]

# 3. Fetch Lineup Data
print(" Fetching Lineups...")
lineups_df = espn.read_lineup().reset_index()
lineups_df['date_str'] = lineups_df['game'].apply(parse_espn_date)
lineups_df['is_starter'] = lineups_df['position'] != 'Substitute'

# 4. Filter Lineups for this specific match
match_date_str = match_row['date'].strftime('%Y-%m-%d')
home_team = match_row['home_team']
away_team = match_row['away_team']

game_lineups = lineups_df[
    (lineups_df['date_str'] == match_date_str) & 
    (lineups_df['team'].isin([home_team, away_team]))
]

# --- 5. VISUALIZE THE DATABASE ROWS ---
print("\n" + "="*60)
print(" VISUAL CHECK: HOW IT WILL LOOK IN SQL")
print("="*60)

print(f"\n TABLE: matches (Note: Scores are separate columns)")
print("-" * 50)
print(f"Date:       {match_date_str}")
print(f"Home Team:  {home_team}")
print(f"Away Team:  {away_team}")
print(f"Home Score: {match_row['home_goals']}  <-- (Integer)")
print(f"Away Score: {match_row['away_goals']}  <-- (Integer)")
print(f"Home xG:    {match_row['home_xg']}")
print(f"Away xG:    {match_row['away_xg']}")

print(f"\n TABLE: player_stats (Sample of 3 players)")
print("-" * 50)
# Showing just the columns that matter
cols = ['team', 'player', 'minutes', 'goals', 'xg', 'shots']
print(game_players[cols].head(3).to_string(index=False))

print(f"\n TABLE: lineups (Sample of 3 players)")
print("-" * 50)
if not game_lineups.empty:
    print(game_lineups[['team', 'player', 'position', 'is_starter']].head(3).to_string(index=False))
else:
    print("  No matching lineup found in ESPN for this date.")

print("\n" + "="*60)
print("✅ Does this structure look good to you?")