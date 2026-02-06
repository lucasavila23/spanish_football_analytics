from utils import run_test_query

# Find the specific matches that have NO lineups
sql_query = """
    SELECT m.id, m.date, m.home_team, m.away_team 
    FROM matches m
    LEFT JOIN lineups l ON m.id = l.match_id
    WHERE l.match_id IS NULL AND m.season = '2023'
    ORDER BY m.date;
"""

df = run_test_query("Investigating Missing Lineups", sql_query)

print(df.to_string(index=False))