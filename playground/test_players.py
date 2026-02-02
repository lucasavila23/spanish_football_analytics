import soccerdata as sd
import pandas as pd

print("⏳ Initializing Understat for Player Stats...")
understat = sd.Understat(leagues="ESP-La Liga", seasons="2023")

print("📊 Fetching Player Match Stats (this might take a moment)...")
# This scrapes specific data for every player in every match
player_stats = understat.read_player_match_stats()

# Inspect the data
print("\n✅ Data Fetched!")
print("Columns available:", player_stats.columns.tolist())

print("\n🔍 Sample (Real Madrid Players):")
# Filter for a specific match to see readability
# Note: Understat often uses a MultiIndex, so we verify the structure first
if 'team' in player_stats.columns or 'team' in player_stats.index.names:
    print(player_stats.reset_index().head(5))
else:
    print(player_stats.head(5))