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

print(f"🚀 STARTING SUPER-STATS PREVIEW for {LEAGUE} {SEASON}...")

# 1. Initialize Scrapers
print("⏳ Loading Scrapers...")
understat = sd.Understat(leagues=LEAGUE, seasons=SEASON)
espn = sd.ESPN(leagues=LEAGUE, seasons=SEASON)

# 2. Fetch Understat Data
print("📥 Fetching Advanced Match & Player Stats...")
matches_df = understat.read_team_match_stats().reset_index()
players_df = understat.read_player_match_stats().reset_index()

# PICK JUST ONE MATCH
test_game_id = matches_df['game_id'].unique()[0]
match_row = matches_df[matches_df['game_id'] == test_game_id].iloc[0]
game_players = players_df[players_df['game_id'] == test_game_id]

# 3. Fetch ESPN Data
print("📥 Fetching Lineups & Action Stats...")
lineups_df = espn.read_lineup().reset_index()
lineups_df['date_str'] = lineups_df['game'].apply(parse_espn_date)

# 4. Filter for specific match
match_date_str = match_row['date'].strftime('%Y-%m-%d')
home_team = match_row['home_team']
away_team = match_row['away_team']

game_lineups = lineups_df[
    (lineups_df['date_str'] == match_date_str) & 
    (lineups_df['team'].isin([home_team, away_team]))
]

# --- 5. VISUALIZE THE SUPER-TABLES ---
print("\n" + "="*60)
print("📊 PREVIEW: THE NEW 'SUPER' DATABASE STRUCTURE")
print("="*60)

print(f"\n1️⃣  TABLE: matches (Separate Scores)")
print("-" * 50)
print(f"Date:       {match_date_str}")
print(f"Home Team:  {home_team}")
print(f"Away Team:  {away_team}")
print(f"Home Score: {match_row['home_goals']}  <-- (Column: home_score)")
print(f"Away Score: {match_row['away_goals']}  <-- (Column: away_score)")
print(f"Home xG:    {match_row['home_xg']}")
print(f"Away xG:    {match_row['away_xg']}")

print(f"\n2️⃣  TABLE: player_stats (Advanced Metrics)")
print("-" * 50)
# Showing 'xg_chain' and 'xg_buildup'
stat_cols = ['player', 'xg', 'xg_chain', 'xg_buildup', 'shots', 'key_passes']
print(game_players[stat_cols].head(5).to_string(index=False))

print(f"\n3️⃣  TABLE: lineups (Action Stats)")
print("-" * 50)
# Showing 'shots_on_target', 'fouls', etc.
action_cols = ['player', 'position', 'shots_on_target', 'fouls_committed', 'fouls_suffered', 'saves']

if not game_lineups.empty:
    print(game_lineups[action_cols].head(5).to_string(index=False))
else:
    print("⚠️  No matching lineup found.")

print("\n✅ PREVIEW COMPLETE")