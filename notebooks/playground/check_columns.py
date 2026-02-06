import soccerdata as sd
import pandas as pd

print("INSPECTING AVAILABLE COLUMNS...")

# Fetch just a tiny bit of data
understat = sd.Understat(leagues="ESP-La Liga", seasons="2023")
players_df = understat.read_player_match_stats().reset_index()

print("\n AVAILABLE PLAYER STATS (Understat):")
print(players_df.columns.tolist())

espn = sd.ESPN(leagues="ESP-La Liga", seasons="2023")
lineups_df = espn.read_lineup().reset_index()

print("\n AVAILABLE LINEUP STATS (ESPN):")
print(lineups_df.columns.tolist())