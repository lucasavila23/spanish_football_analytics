# UPDATE modules/debug_links.py with this code
import requests
from bs4 import BeautifulSoup

def inspect_hemeroteca():
    # Let's check the archive for the date of El Clasico (Barcelona vs Real Madrid)
    # Note: We check the day AFTER the match (Oct 29) because chronicles often publish late or next morning.
    url = "https://www.marca.com/hemeroteca/2023/10/29/"
    print(f"🕵️ Inspecting Time Machine: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        resp = requests.get(url, headers=headers)
        print(f"   Status Code: {resp.status_code}")
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # In Hemeroteca, links are usually in a list
        links = soup.find_all('a', href=True)
        print(f"   Found {len(links)} links.")
        
        print("\n--- CHECKING FOR MATCH REPORTS (CRONICA) ---")
        found_count = 0
        for a in links:
            if '/futbol/' in a['href'] and 'cronica' in a['href']:
                print(f"   Found Chronicle: {a.get_text(strip=True)[:50]}...")
                print(f"      Link: {a['href']}")
                found_count += 1
                if found_count >= 5: break
                
        if found_count == 0:
            print("   No chronicles found explicitly. Dumping first 5 football links:")
            for a in links:
                if '/futbol/' in a['href']:
                    print(f"      - {a['href']}")
                    found_count += 1
                    if found_count >= 5: break

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_hemeroteca()