import requests
from bs4 import BeautifulSoup

def inspect_as_links():
    # Inspect Matchday 1 of 2023/24
    url = "https://resultados.as.com/resultados/futbol/primera/2023_2024/jornada/regular_a_1"
    print(f" Inspecting: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)'
    }
    
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    links = soup.find_all('a', href=True)
    
    print(f"   Found {len(links)} links total.")
    print("   Dumping links containing 'futbol' or 'cronica':\n")
    
    count = 0
    for a in links:
        href = a['href']
        text = a.get_text(strip=True)
        
        # Show us anything that looks like a match link
        if 'futbol' in href or 'cronica' in href or 'partido' in href:
            print(f"   [{count}] Text: '{text}' | Href: '{href}'")
            count += 1
            if count > 15: break

if __name__ == "__main__":
    inspect_as_links()