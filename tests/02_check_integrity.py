from utils import run_test_query

# 1. DEFINE THE TEST
# Find matches that exist in the 'matches' table but have ZERO rows in 'player_stats'
sql_query = """
    SELECT m.id, m.date, m.home_team, m.away_team
    FROM matches m
    LEFT JOIN player_stats ps ON m.id = ps.match_id
    WHERE m.season = '2023' AND ps.match_id IS NULL;
"""

# 2. RUN THE TEST
df = run_test_query("Integrity Check (Orphan Matches)", sql_query)

# 3. ANALYZE RESULTS
if df.empty:
    print("[PASS] All matches are successfully linked to player data.")
else:
    print(f"[FAIL] Found {len(df)} matches with NO player stats.")
    print("   These matches exist in the schedule but have no performance data:")
    print(df.head().to_string(index=False))