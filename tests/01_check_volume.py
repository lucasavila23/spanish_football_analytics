from utils import run_test_query

# 1. DEFINE THE TEST
# La Liga has 20 teams. 38 Matchdays. Total games = (20 * 38) / 2 = 380.
sql_query = """
    SELECT 
        (SELECT COUNT(*) FROM matches) as total_matches,
        (SELECT COUNT(DISTINCT match_id) FROM player_stats) as matches_with_stats,
        (SELECT COUNT(DISTINCT match_id) FROM lineups) as matches_with_lineups;
"""

# 2. RUN THE TEST
df = run_test_query("Volume Check (Completeness)", sql_query)

# 3. ANALYZE RESULTS
if df.empty:
    print("[FAIL] Could not retrieve data.")
else:
    matches = df.iloc[0]['total_matches']
    stats = df.iloc[0]['matches_with_stats']
    
    print(df.to_string(index=False))
    print("-" * 50)

    if matches == 380:
        print(f"[PASS] Found exactly {matches} matches.")
    else:
        print(f"[FAIL] Expected 380 matches, found {matches}. (Did the scraper finish?)")

    if matches == stats:
        print(f"[PASS] All {matches} matches have player stats linked.")
    else:
        print(f"[FAIL] We have {matches} matches but only {stats} have player data.")