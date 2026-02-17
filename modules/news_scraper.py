import requests
from bs4 import BeautifulSoup
import time
import random
import re

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def scrape_as_headlines(season_year):
    season_year = int(season_year)
    start_y = season_year
    end_y = season_year + 1
    
    base_url = "https://as.com"
    # Note: This URL pattern is standard for the archives
    pattern_url = f"https://as.com/resultados/futbol/primera/{start_y}_{end_y}/jornada/regular_a_"
    
    all_news = []
    seen_urls = set()

    print(f"  Starting Score-Validated Scraper ({start_y}-{end_y})...")

    for jornada in range(1, 39):
        target_url = f"{pattern_url}{jornada}/"
        print(f"     Jornada {jornada}/38...", end="", flush=True)
        
        try:
            resp = requests.get(target_url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                print(f" [Error {resp.status_code}]")
                continue
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 1. FIND ALL CANDIDATE LINKS
            # We look for any link containing "crónica" in its text (case insensitive)
            candidate_links = soup.find_all('a', string=lambda t: t and 'crónica' in t.lower())
            
            valid_articles = 0
            
            for link in candidate_links:
                # 2. VALIDATE THE CONTAINER (The "Anti-Leak" Check)
                # We climb up the HTML tree to find the row containing this link.
                # We check if that row contains a Score (e.g., "2 - 1" or "0-0").
                # If it has a score, it's a match. If not, it's likely a sidebar ad.
                
                is_valid_match_row = False
                context_text = ""
                container = link.parent
                
                # Climb up to 4 levels to find a container with a score
                for _ in range(4):
                    if not container: break
                    text = container.get_text(" ", strip=True)
                    
                    # REGEX: Look for "Number - Number" pattern (e.g. 2 - 1)
                    if re.search(r'\d+\s*-\s*\d+', text):
                        is_valid_match_row = True
                        context_text = text.lower()
                        break
                    container = container.parent
                
                if not is_valid_match_row:
                    continue # Skip this link, it's not a match result
                
                # 3. EXTRACT URL
                href = link.get('href')
                if not href: continue
                full_link = href if href.startswith('http') else base_url + href
                
                if full_link in seen_urls: continue
                seen_urls.add(full_link)

                # 4. VISIT ARTICLE
                try:
                    time.sleep(random.uniform(0.05, 0.1))
                    art_resp = requests.get(full_link, headers=HEADERS, timeout=5)
                    art_soup = BeautifulSoup(art_resp.text, 'html.parser')
                    
                    # Find Headline
                    h1 = art_soup.select_one('h1')
                    headline = h1.get_text(strip=True) if h1 else "No Headline"
                    
                    # Filter out generic pages
                    if "clasificación" in headline.lower() or "calendario" in headline.lower():
                        continue

                    sub = art_soup.find('h2')
                    subheader = sub.get_text(strip=True) if sub else ""

                    all_news.append({
                        'url': full_link,
                        'headline': headline,
                        'subheader': subheader,
                        'context_teams': context_text,
                        'jornada': jornada
                    })
                    valid_articles += 1
                    
                except Exception:
                    continue

            print(f" Found {valid_articles} articles.")

        except Exception as e:
            print(f" Error: {e}")

    return all_news