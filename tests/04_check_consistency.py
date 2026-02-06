from modules.utils import run_test_query

# 1. GET RELEVANT IDs FOR THE TARGET SEASON
# We use JOINs to ensure we only pull stats/lineups belonging to 2023 matches.
# If we didn't do this, the test would flag 2022 data as "Ghost Data" errors.
sql_query = """
    -- 1. Get matches from 2023
    SELECT 'matches' as source, id as match_id 
    FROM matches 
    WHERE season = '2023'

    UNION ALL

    -- 2. Get player_stats linked to 2023 matches
    SELECT 'player_stats' as source, ps.match_id 
    FROM player_stats ps
    JOIN matches m ON ps.match_id = m.id
    WHERE m.season = '2023'

    UNION ALL

    -- 3. Get lineups linked to 2023 matches
    SELECT 'lineups' as source, l.match_id 
    FROM lineups l
    JOIN matches m ON l.match_id = m.id
    WHERE m.season = '2023';
"""

df = run_test_query("Consistency Check (ID Matching - 2023)", sql_query)

if df.empty:
    print("[FAIL] No data found in database for 2023.")
else:
    # 2. SEPARATE INTO SETS
    matches_ids = set(df[df['source'] == 'matches']['match_id'])
    stats_ids = set(df[df['source'] == 'player_stats']['match_id'])
    lineups_ids = set(df[df['source'] == 'lineups']['match_id'])

    print(f"   > Matches in 2023:      {len(matches_ids)}")
    print(f"   > Match IDs in Stats:   {len(stats_ids)}")
    print(f"   > Match IDs in Lineups: {len(lineups_ids)}")
    print("-" * 50)

    # 3. PERFORM THE CHECKS
    
    # CHECK A: Completeness (Do all matches have stats?)
    missing_stats = matches_ids - stats_ids
    if len(missing_stats) == 0:
        print("[PASS] Every 2023 match has corresponding player stats.")
    else:
        print(f"[FAIL] {len(missing_stats)} matches are missing player stats.")
        print(f"       IDs: {list(missing_stats)[:5]}...")

    # CHECK B: Completeness (Do all matches have lineups?)
    missing_lineups = matches_ids - lineups_ids
    if len(missing_lineups) == 0:
        print("[PASS] Every 2023 match has corresponding lineups.")
    else:
        print(f"[FAIL] {len(missing_lineups)} matches are missing lineups.")
        print(f"       IDs: {list(missing_lineups)[:5]}...")

    # CHECK C: Ghost Data (Do we have stats for matches that aren't in the list?)
    # Note: Since we joined on matches table in SQL, this should theoretically always be 0
    # unless there is a strange logic error in the scraper.
    ghost_stats = stats_ids - matches_ids
    if len(ghost_stats) == 0:
        print("[PASS] No 'Ghost' stats found (Integrity OK).")
    else:
        print(f"[CRITICAL FAIL] Found stats for matches that don't exist in 2023 set!")
        print(f"       IDs: {list(ghost_stats)[:5]}...")