from utils import run_test_query

# 1. DEFINE THE TEST
# This SQL logic mimics exactly how a league table is calculated
sql_query = """
    WITH home_points AS (
        SELECT home_team as team, 
               SUM(CASE WHEN home_score > away_score THEN 3 
                        WHEN home_score = away_score THEN 1 
                        ELSE 0 END) as pts,
               SUM(home_score) as gf,
               SUM(away_score) as ga
        FROM matches GROUP BY home_team
    ),
    away_points AS (
        SELECT away_team as team, 
               SUM(CASE WHEN away_score > home_score THEN 3 
                        WHEN away_score = home_score THEN 1 
                        ELSE 0 END) as pts,
               SUM(away_score) as gf,
               SUM(home_score) as ga
        FROM matches GROUP BY away_team
    )
    SELECT 
        h.team,
        (h.pts + a.pts) as Points,
        (h.gf + a.gf) as GF,
        (h.ga + a.ga) as GA,
        ((h.gf + a.gf) - (h.ga + a.ga)) as GD
    FROM home_points h
    JOIN away_points a ON h.team = a.team
    ORDER BY Points DESC, GD DESC
    LIMIT 5;
"""

# 2. RUN THE TEST
df = run_test_query("Reality Check (League Standings)", sql_query)

# 3. ANALYZE RESULTS
print(df.to_string(index=False))
print("-" * 50)

# Check Real Madrid (The Winner)
rm_row = df[df['team'] == 'Real Madrid']
if not rm_row.empty and rm_row.iloc[0]['points'] == 95:
    print("[PASS] Real Madrid has 95 Points (Matches Reality).")
else:
    print("[WARNING] Check the points above against Google. (Real Madrid should have 95).")