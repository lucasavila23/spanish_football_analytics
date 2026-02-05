from utils import run_test_query

# 1. GET ALL IDs FROM ALL TABLES
# We fetch the distinct Match IDs present in each table
query_ids = """
    SELECT 'matches' as source, id as match_id FROM matches
    UNION ALL
    SELECT 'player_stats' as source, match_id as match_id FROM player_stats
    UNION ALL
    SELECT 'lineups' as source, match_id as match_id FROM lineups;
"""

df = run_test_query("Consistency Check (ID Matching)", query_ids)

if df.empty:
    print("[FAIL] No data found in database.")
else:
    # 2. SEPARATE INTO SETS (Mathematical Groups)
    matches_ids = set(df[df['source'] == 'matches']['match_id'])
    stats_ids = set(df[df['source'] == 'player_stats']['match_id'])
    lineups_ids = set(df[df['source'] == 'lineups']['match_id'])

    print(f"   > IDs in Matches Table:      {len(matches_ids)}")
    print(f"   > IDs in Player Stats Table: {len(stats_ids)}")
    print(f"   > IDs in Lineups Table:      {len(lineups_ids)}")
    print("-" * 50)

    # 3. PERFORM THE CHECKS
    
    # CHECK A: Are there matches without stats? (Matches - Stats)
    missing_stats = matches_ids - stats_ids
    if len(missing_stats) == 0:
        print("[PASS] Every match has corresponding player stats.")
    else:
        print(f"[FAIL] {len(missing_stats)} matches are missing player stats.")
        print(f"       IDs: {list(missing_stats)[:5]}...")

    # CHECK B: Are there matches without lineups? (Matches - Lineups)
    missing_lineups = matches_ids - lineups_ids
    if len(missing_lineups) == 0:
        print("[PASS] Every match has corresponding lineups.")
    else:
        print(f"[FAIL] {len(missing_lineups)} matches are missing lineups.")
        print(f"       IDs: {list(missing_lineups)[:5]}...")

    # CHECK C: Do we have stats for non-existent matches? (Stats - Matches)
    # This detects "Ghost Data" (violates Foreign Keys)
    ghost_stats = stats_ids - matches_ids
    if len(ghost_stats) == 0:
        print("[PASS] No 'Ghost' stats found (Referential Integrity OK).")
    else:
        print(f"[CRITICAL FAIL] Found stats for matches that don't exist in the main table!")
        print(f"       IDs: {list(ghost_stats)[:5]}...")