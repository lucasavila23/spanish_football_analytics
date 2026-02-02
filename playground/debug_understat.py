import soccerdata as sd
import pandas as pd

# 1. Initialize
print("⏳ Initializing Understat...")
understat = sd.Understat(leagues="ESP-La Liga", seasons="2023")

# 2. Fetch Data
print("📊 Fetching data...")
xg_stats = understat.read_team_match_stats()

# 3. DEBUG: Print the Index Names (what are the rows labeled?)
print("\n🕵️ DIAGNOSTIC 1: Index Names")
print(xg_stats.index.names)

# 4. Reset Index
xg_stats = xg_stats.reset_index()

# 5. DEBUG: Print the Column Names (what can we select?)
print("\n🕵️ DIAGNOSTIC 2: Column Names")
print(xg_stats.columns.tolist())

# 6. Try to find the team column
print("\n🕵️ DIAGNOSTIC 3: Sample Data")
print(xg_stats.head(2))