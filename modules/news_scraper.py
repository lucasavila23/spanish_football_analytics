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
    
    # Base URL for relative links
    base_url = "https://resultados.as.com"
    pattern_url = f"https://resultados.as.com/resultados/futbol/primera/{start_y}_{end_y}/jornada/regular_a_"
    
    all_news = []
    seen_urls = set()

    print(f"  Starting Context-Aware Scraper ({start_y}-{end_y})...")

    for jornada in range(1, 39):
        target_url = f"{pattern_url}{jornada}"
        print(f"     Jornada {jornada}/38...")
        
        try:
            resp = requests.get(target_url, headers=HEADERS, timeout=10)
            if resp.status_code != 200: continue
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # STRATEGY: Find links specifically with text "Ver crónica"
            cronica_links = soup.find_all(lambda tag: tag.name == 'a' and tag.string and 'ver crónica' in tag.string.lower())
            
            found_count = 0
            
            for link in cronica_links:
                href = link.get('href')
                if not href: continue
                
                # Fix relative URL
                full_link = base_url + href if href.startswith('/') else href
                
                if full_link in seen_urls: continue
                seen_urls.add(full_link)

                # --- CAPTURE CONTEXT (The Team Names) ---
                try:
                    # Try to find the Table Row (tr) first
                    container = link.find_parent('tr')
                    if not container:
                        # Fallback: go up 3 parents
                        container = link.parent.parent.parent
                    
                    context_text = container.get_text(" ", strip=True).lower()
                except:
                    context_text = ""

                # --- VISIT ARTICLE ---
                try:
                    time.sleep(random.uniform(0.1, 0.3))
                    art_resp = requests.get(full_link, headers=HEADERS, timeout=5)
                    art_soup = BeautifulSoup(art_resp.text, 'html.parser')
                    
                    h1 = art_soup.find('h1')
                    headline = h1.get_text(strip=True) if h1 else "No Headline"
                    
                    sub = art_soup.find('h2') or art_soup.find(class_='entradilla')
                    subheader = sub.get_text(strip=True) if sub else ""

                    # Extract Date for DB lookup (still useful if available)
                    date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', full_link)
                    date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}" if date_match else None

                    all_news.append({
                        'url': full_link,
                        'headline': headline,
                        'subheader': subheader,
                        'date_url': date_str,
                        'context_teams': context_text,
                        'jornada': jornada  # <--- CRITICAL FOR THE FIX
                    })
                    found_count += 1
                    
                except Exception:
                    continue

            print(f"      Found {found_count} chronicles.")

        except Exception as e:
            print(f"      Error J{jornada}: {e}")

    return all_news