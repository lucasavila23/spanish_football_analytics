import soccerdata as sd
import pandas as pd

print("⏳ Initializing ESPN...")
espn = sd.ESPN(leagues="ESP-La Liga", seasons="2023")

print("📊 Fetching Lineups...")
lineups = espn.read_lineup()
lineups = lineups.reset_index()

# Create 'is_starter' logic
lineups['is_starter'] = lineups['position'] != 'Substitute'

print("\n🔍 Real Madrid Starting XI (First Match Found):")
# Filter for Real Madrid starters
madrid_starters = lineups[
    (lineups['team'] == 'Real Madrid') & 
    (lineups['is_starter'] == True)
]

# Print only columns we KNOW exist
print(madrid_starters[['game', 'player', 'position']].head(11))