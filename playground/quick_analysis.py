import pandas as pd
from sqlalchemy import create_engine, text

# --- DATABASE CONNECTION (SQLAlchemy) ---
# This eliminates the Pandas UserWarning
DB_URI = "postgresql+psycopg2://runner:football_password@localhost/spanish_football"
engine = create_engine(DB_URI)

def run_query(query_title, sql_query):
    """
    Helper function to run SQL and print a clean Pandas table.
    """
    print(f"\n📊 QUERY: {query_title}")
    print("-" * 60)
    
    try:
        # We use a connection context manager for safety
        with engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn)
        
        if df.empty:
            print("[Result] No data returned (Check your filters).")
        else:
            # Print the table neatly
            print(df.head(15).to_string(index=False))
            print(f"\n(Total Rows: {len(df)})")
            
    except Exception as e:
        print(f"[ERROR] {e}")

# ==============================================================================
# 🧠 CORRECTED QUERIES
# ==============================================================================

# 0. SANITY CHECK: Do we have data?
sql_check = """
    SELECT 
        (SELECT COUNT(*) FROM matches) as match_count,
        (SELECT COUNT(*) FROM player_stats) as player_stat_count,
        (SELECT COUNT(*) FROM lineups) as lineup_count;
"""

# 1. Who are the most "unlucky" finishers? 
# (High xG, Low Goals)
sql_unlucky = """
    SELECT 
        player_name, 
        team, 
        SUM(goals) as goals, 
        ROUND(SUM(xg), 2) as total_xg,
        ROUND(SUM(goals) - SUM(xg), 2) as performance_vs_xg
    FROM player_stats
    GROUP BY player_name, team
    HAVING SUM(xg) > 5  -- Only look at relevant players
    ORDER BY performance_vs_xg ASC
    LIMIT 10;
"""

# 2. Who is the "King of Build-up"? 
# FIX: Changed WHERE to HAVING so we filter by *Season Total*, not *Single Match*
sql_buildup = """
    SELECT 
        player_name, 
        team, 
        SUM(minutes) as minutes_played,
        ROUND(SUM(xg_buildup), 2) as total_buildup
    FROM player_stats
    GROUP BY player_name, team
    HAVING SUM(minutes) > 900 -- Filter AFTER summing minutes
    ORDER BY total_buildup DESC
    LIMIT 10;
"""

# 3. Tactical Aggression: Which teams commit the most fouls?
sql_fouls = """
    SELECT 
        team, 
        COUNT(DISTINCT match_id) as games_played,
        SUM(fouls_committed) as total_fouls,
        ROUND(SUM(fouls_committed)::numeric / NULLIF(COUNT(DISTINCT match_id), 0), 1) as avg_fouls_per_game
    FROM lineups
    GROUP BY team
    ORDER BY avg_fouls_per_game DESC;
"""

# --- RUN THEM ---
if __name__ == "__main__":
    print("🚀 RUNNING ANALYTICS PREVIEW...")
    
    run_query("Sanity Check (Row Counts)", sql_check)
    run_query("The 'Unlucky' Finishers (Underperforming xG)", sql_unlucky)
    run_query("The Architects (Best xG Buildup)", sql_buildup)
    run_query("The 'Bad Boys' (Fouls per Game)", sql_fouls)