from modules.utils import run_test_query

# 1. DEFINE THE TEST
# This SQL logic mimics exactly how a league table is calculated
sql_query = """
    WITH home_points AS (
        SELECT 
            home_team as team, 
            SUM(CASE WHEN home_score > away_score THEN 3 
                     WHEN home_score = away_score THEN 1 
                     ELSE 0 END) as pts,
            SUM(home_score) as gf,
            SUM(away_score) as ga
        FROM matches 
        WHERE season = '2023'  -- <--- FIXED: Moved here and added quotes
        GROUP BY home_team
    ),
    away_points AS (
        SELECT 
            away_team as team, 
            SUM(CASE WHEN away_score > home_score THEN 3 
                     WHEN away_score = home_score THEN 1 
                     ELSE 0 END) as pts,
            SUM(away_score) as gf,
            SUM(home_score) as ga
        FROM matches 
        WHERE season = '2023'  -- <--- FIXED: Moved here and added quotes
        GROUP BY away_team
    )
    SELECT 
        h.team,
        (h.pts + a.pts) as points, -- Changed to lowercase for consistency
        (h.gf + a.gf) as gf,
        (h.ga + a.ga) as ga,
        ((h.gf + a.gf) - (h.ga + a.ga)) as gd
    FROM home_points h
    JOIN away_points a ON h.team = a.team
    ORDER BY points DESC, gd DESC
    LIMIT 5;
"""

# 2. RUN THE TEST
df = run_test_query("Reality Check (League Standings)", sql_query)

# 3. ANALYZE RESULTS
if df.empty:
    print("[FAIL] No data returned.")
else:
    print(df.to_string(index=False))
    print("-" * 50)

    # Check Real Madrid (The 2023 Winner)
    # Note: Column names coming from SQL usually default to lowercase in pandas
    rm_row = df[df['team'] == 'Real Madrid']
    
    if not rm_row.empty:
        points = rm_row.iloc[0]['points']
        if points == 95:
            print(f"[PASS] Real Madrid has {points} Points (Matches Reality).")
        else:
            print(f"[WARNING] Real Madrid has {points} points (Expected 95). Check for missing games.")
    else:
        print("[FAIL] Real Madrid not found in top 5.")