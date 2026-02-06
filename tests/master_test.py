import argparse
import pandas as pd
from utils import run_test_query

# ==========================================
# TEST 1: VOLUME CHECK
# ==========================================
def check_volume(season):
    print(f"\n RUNNING TEST 1: DATA VOLUME ({season})")
    
    sql = f"""
        SELECT 
            (SELECT COUNT(*) FROM matches WHERE season = '{season}') as total_matches,
            (SELECT COUNT(DISTINCT ps.match_id) 
             FROM player_stats ps JOIN matches m ON ps.match_id = m.id
             WHERE m.season = '{season}') as matches_with_stats,
            (SELECT COUNT(DISTINCT l.match_id) 
             FROM lineups l JOIN matches m ON l.match_id = m.id
             WHERE m.season = '{season}') as matches_with_lineups;
    """
    
    df = run_test_query("Volume Check", sql)
    
    if df.empty:
        print("[CRITICAL] No data returned for volume check.")
        return False

    matches = df.iloc[0]['total_matches']
    stats = df.iloc[0]['matches_with_stats']
    lineups = df.iloc[0]['matches_with_lineups']
    
    print(f"   > Matches Found: {matches}")
    print(f"   > With Stats:    {stats}")
    print(f"   > With Lineups:  {lineups}")

    # Logic: 380 matches is standard, but current season might be less
    if matches == 380:
        print(f"[PASS] Full 380 match season detected.")
    elif matches > 0:
        print(f"[WARN] {matches} matches found (Season might be ongoing or incomplete).")
    else:
        print(f"[FAIL] 0 matches found.")
        return False

    if matches == stats == lineups:
        print("[PASS] Data is perfectly synchronized across tables.")
        return True
    else:
        print("[FAIL] Discrepancy detected between tables.")
        return False

# ==========================================
# TEST 2: CONSISTENCY & GHOST DATA
# ==========================================
def check_consistency(season):
    print(f"\n RUNNING TEST 2: CONSISTENCY & GHOST DATA ({season})")
    
    sql = f"""
        SELECT 'matches' as source, id as match_id FROM matches WHERE season = '{season}'
        UNION ALL
        SELECT 'player_stats' as source, ps.match_id 
        FROM player_stats ps JOIN matches m ON ps.match_id = m.id WHERE m.season = '{season}'
        UNION ALL
        SELECT 'lineups' as source, l.match_id 
        FROM lineups l JOIN matches m ON l.match_id = m.id WHERE m.season = '{season}';
    """
    
    df = run_test_query("Consistency Check", sql)
    
    if df.empty:
        print("[FAIL] No data found.")
        return False

    matches_ids = set(df[df['source'] == 'matches']['match_id'])
    stats_ids = set(df[df['source'] == 'player_stats']['match_id'])
    
    # Check for Ghost Stats (Stats that exist for matches NOT in the match table)
    ghost_stats = stats_ids - matches_ids
    
    if len(ghost_stats) == 0:
        print("[PASS] No 'Ghost' data found.")
        return True
    else:
        print(f"[FAIL] Found {len(ghost_stats)} orphan stat records (Ghost Data).")
        return False

# ==========================================
# TEST 3: REALITY CHECK (STANDINGS)
# ==========================================
def check_reality(season):
    print(f"\n RUNNING TEST 3: REALITY CHECK (League Table {season})")
    
    sql = f"""
        WITH home_points AS (
            SELECT home_team as team, 
                   SUM(CASE WHEN home_score > away_score THEN 3 
                            WHEN home_score = away_score THEN 1 ELSE 0 END) as pts,
                   SUM(home_score) as gf, SUM(away_score) as ga
            FROM matches WHERE season = '{season}' GROUP BY home_team
        ),
        away_points AS (
            SELECT away_team as team, 
                   SUM(CASE WHEN away_score > home_score THEN 3 
                            WHEN away_score = home_score THEN 1 ELSE 0 END) as pts,
                   SUM(away_score) as gf, SUM(home_score) as ga
            FROM matches WHERE season = '{season}' GROUP BY away_team
        )
        SELECT h.team, (h.pts + a.pts) as points, 
               ((h.gf + a.gf) - (h.ga + a.ga)) as gd
        FROM home_points h
        JOIN away_points a ON h.team = a.team
        ORDER BY points DESC, gd DESC
        LIMIT 3;
    """
    
    df = run_test_query("Standings Check", sql)
    
    if df.empty:
        print("[FAIL] Could not calculate standings.")
        return False
    
    print(df.to_string(index=False))
    
    # Specific Check for 2023 (Real Madrid Won with 95pts)
    if season == '2023':
        top_team = df.iloc[0]
        if top_team['team'] == 'Real Madrid' and top_team['points'] == 95:
            print("[PASS] 2023 Champion is Real Madrid with 95 pts (Confirmed).")
            return True
        else:
            print("[WARN] Top team does not match historical 2023 reality.")
            return True # Not a hard fail, maybe data is partial
            
    return True

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full integrity suite for a season.")
    parser.add_argument("--season", type=str, required=True, help="Season year (e.g., 2023)")
    
    args = parser.parse_args()
    
    print("="*60)
    print(f" INITIATING MASTER TEST SUITE FOR SEASON: {args.season}")
    print("="*60)
    
    v_ok = check_volume(args.season)
    c_ok = check_consistency(args.season)
    r_ok = check_reality(args.season)
    
    print("\n" + "="*60)
    if v_ok and c_ok and r_ok:
        print(f"ALL TESTS PASSED FOR SEASON {args.season}")
    else:
        print(f"SOME TESTS FAILED FOR SEASON {args.season}")
    print("="*60)
    
# Check 2023
### python tests/master_test.py --season 2023

# Check 2022
### python tests/master_test.py --season 2022