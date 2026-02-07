import psycopg2
import os
import argparse
import unidecode
from dotenv import load_dotenv
from modules.news_scraper import scrape_as_headlines

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "spanish_football"),
    "user": os.getenv("DB_USER", "runner"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost")
}

# Words to ignore during matching to avoid "Real Betis" matching "Real Madrid"
STOP_WORDS = {"real", "club", "deportivo", "sociedad", "athletic", "atletico", "fútbol", "futbol", "cf", "fc", "sad"}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def normalize(text):
    """Removes accents and lowers text for comparison"""
    if not text: return ""
    return unidecode.unidecode(text).lower()

def get_unique_name_parts(team_name):
    """
    Splits a team name into parts but removes generic football words.
    'Real Betis' -> ['betis']
    'Atletico Madrid' -> ['madrid']
    'Real Madrid' -> ['madrid']
    'FC Barcelona' -> ['barcelona']
    """
    clean_name = normalize(team_name)
    parts = clean_name.split()
    # Filter out stop words AND short words (len < 3)
    unique_parts = [p for p in parts if p not in STOP_WORDS and len(p) > 2]
    
    # If we filtered everything out (e.g. just "Real"), revert to original parts to be safe
    if not unique_parts:
        return [p for p in parts if len(p) > 2]
        
    return unique_parts

def get_season_matches(cursor, season_year):
    query = """
        SELECT id, date, home_team, away_team 
        FROM matches 
        WHERE season = %s 
        ORDER BY date ASC
    """
    cursor.execute(query, (str(season_year),))
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def find_match_id_by_names(matches, context_text, jornada):
    if not context_text: return None
    
    clean_context = normalize(context_text)
    potential_matches = []
    
    for m in matches:
        # Use the stricter name splitter
        home_parts = get_unique_name_parts(m['home_team'])
        away_parts = get_unique_name_parts(m['away_team'])
        
        # Check if at least one unique part of the Home team is in the text
        home_found = any(p in clean_context for p in home_parts)
        # Check if at least one unique part of the Away team is in the text
        away_found = any(p in clean_context for p in away_parts)
        
        if home_found and away_found:
            potential_matches.append(m)
            
    if not potential_matches:
        return None
        
    potential_matches.sort(key=lambda x: x['date'])
    
    if len(potential_matches) == 1:
        return potential_matches[0]['id']
    
    if len(potential_matches) >= 2:
        if jornada <= 19:
            return potential_matches[0]['id']
        else:
            return potential_matches[-1]['id']

    return None

def run_news_ingestion(season_year):
    # 1. Scrape
    news_items = scrape_as_headlines(season_year)
    if not news_items: return

    # 2. Connect
    conn = get_db_connection()
    cur = conn.cursor()
    
    print(f"\n Loading matches for season {season_year}...")
    try:
        all_matches = get_season_matches(cur, season_year)
    except Exception as e:
        print(f" Error loading matches: {e}")
        return

    matches_linked = 0
    print(f"   Processing {len(news_items)} articles...")

    for item in news_items:
        match_id = find_match_id_by_names(all_matches, item['context_teams'], item['jornada'])
        
        if match_id:
            try:
                cur.execute("""
                    INSERT INTO news_headlines (match_id, url, headline, subheader)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING;
                """, (match_id, item['url'], item['headline'], item['subheader']))
                matches_linked += 1
            except Exception as e:
                conn.rollback()
                print(f"Error inserting: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ FINISHED! Linked {matches_linked} articles to matches.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2023, help="Season year")
    args = parser.parse_args()
    
    run_news_ingestion(args.season)