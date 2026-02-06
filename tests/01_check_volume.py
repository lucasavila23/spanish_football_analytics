from modules.utils import run_test_query

# 1. DEFINE THE TEST
# We must filter EACH subquery independently because 'season' 
# only exists in the 'matches' table.
sql_query = """
    SELECT 
        -- 1. Count Matches in 2023
        (SELECT COUNT(*) FROM matches WHERE season = '2023') as total_matches,
        
        -- 2. Count Player Stats (Linked to 2023 matches via JOIN)
        (SELECT COUNT(DISTINCT ps.match_id) 
         FROM player_stats ps
         JOIN matches m ON ps.match_id = m.id
         WHERE m.season = '2023') as matches_with_stats,
         
        -- 3. Count Lineups (Linked to 2023 matches via JOIN)
        (SELECT COUNT(DISTINCT l.match_id) 
         FROM lineups l
         JOIN matches m ON l.match_id = m.id
         WHERE m.season = '2023') as matches_with_lineups;
"""

# 2. RUN THE TEST
df = run_test_query("Volume Check (Season 2023)", sql_query)

# 3. ANALYZE RESULTS
if df.empty:
    print("[FAIL] Could not retrieve data.")
else:
    # Extract values from the dataframe
    matches = df.iloc[0]['total_matches']
    stats = df.iloc[0]['matches_with_stats']
    lineups = df.iloc[0]['matches_with_lineups']
    
    # Print the raw table
    print(df.to_string(index=False))
    print("-" * 50)

    # CHECK 1: Total Volume
    # La Liga has 380 matches per season
    if matches == 380:
        print(f"✅ [PASS] Found exactly {matches} matches for 2023.")
    else:
        print(f"⚠️ [WARN] Expected 380 matches, found {matches}. (Scraper might be incomplete)")

    # CHECK 2: Integrity (Matches vs Stats)
    if matches == stats:
        print(f"✅ [PASS] All matches have player stats linked.")
    else:
        print(f"❌ [FAIL] Integrity Error: {matches} matches vs {stats} with stats.")

    # CHECK 3: Integrity (Matches vs Lineups)
    if matches == lineups:
        print(f"✅ [PASS] All matches have lineups linked.")
    else:
        print(f"❌ [FAIL] Integrity Error: {matches} matches vs {lineups} with lineups.")