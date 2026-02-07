import psycopg2
import os
import sys
from dotenv import load_dotenv

# Ensure we can find the .env file from the root directory
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "spanish_football"),
    "user": os.getenv("DB_USER", "runner"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost")
}

def inspect_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
    except Exception as e:
        print(f" Error connecting to DB: {e}")
        return
    
    print("\n Inspecting Scraped News (Sample of 10)...\n")
    
    # Query to join headlines with matches so we see the context
    query = """
        SELECT 
            m.date,
            m.home_team, 
            m.away_team, 
            n.headline, 
            n.subheader
        FROM news_headlines n
        JOIN matches m ON n.match_id = m.id
        ORDER BY RANDOM()
        LIMIT 10;
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    print(f"{'DATE':<12} | {'MATCH':<35} | {'HEADLINE'}")
    print("-" * 100)
    
    for row in rows:
        date, home, away, headline, subheader = row
        match_str = f"{home} vs {away}"
        date_str = str(date)
        
        # Truncate headline if too long for display
        display_headline = (headline[:50] + '..') if len(headline) > 50 else headline
        
        print(f"{date_str:<12} | {match_str:<35} | {display_headline}")
            
    # Count total
    cur.execute("SELECT COUNT(*) FROM news_headlines")
    total = cur.fetchone()[0]
    
    print("-" * 100)
    print(f"\n✅ Total Articles Scraped: {total}")
    
    conn.close()

if __name__ == "__main__":
    inspect_data()