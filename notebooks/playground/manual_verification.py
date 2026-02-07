import psycopg2
import os
import textwrap
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "spanish_football"),
    "user": os.getenv("DB_USER", "runner"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost")
}

def verify_manual():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Get 3 random matches that HAVE headlines
    cur.execute("""
        SELECT m.date, m.home_team, m.away_team, m.home_score, m.away_score, 
               n.headline, n.subheader, n.url
        FROM news_headlines n
        JOIN matches m ON n.match_id = m.id
        ORDER BY RANDOM()
        LIMIT 3
    """)
    
    rows = cur.fetchall()
    
    print("\n  MANUAL VERIFICATION LOG\n" + "="*60)
    
    for i, row in enumerate(rows, 1):
        date, home, away, hs, as_score, head, sub, url = row
        
        print(f"\nCASE #{i}: {home} vs {away}")
        print(f" Date:   {date}")
        print(f" Score:  {hs} - {as_score}")
        print("-" * 60)
        print(f" Headline: {head}")
        
        # Wrap subheader nicely
        if sub:
            print(f" Subheader:\n{textwrap.fill(sub, width=60, initial_indent='   ', subsequent_indent='   ')}")
            
        print(f"\n URL: {url}")
        print("=" * 60)

    conn.close()

if __name__ == "__main__":
    verify_manual()