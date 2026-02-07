import requests
from bs4 import BeautifulSoup

def xray_matchday():
    # We look at Jornada 1 again
    url = "https://resultados.as.com/resultados/futbol/primera/2023_2024/jornada/regular_a_1"
    print(f" X-Raying: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)'
    }
    
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # 1. Search for elements containing the text "Crónica"
    # We use a lambda to find the text case-insensitively
    cronica_tags = soup.find_all(string=lambda text: text and 'crónica' in text.lower())
    
    print(f"\n Found {len(cronica_tags)} elements containing 'Crónica'.")
    
    print("\n--- DUMPING CONTEXT ---")
    for i, text_node in enumerate(cronica_tags[:5]): # Show first 5 matches
        parent = text_node.parent
        
        # If the parent is a span or formatting tag, go up one more level to find the link
        if parent.name not in ['a', 'div', 'td']:
            parent = parent.parent
            
        print(f"\n[{i}] Found text: '{text_node.strip()}'")
        print(f"    Parent Tag: <{parent.name} class='{parent.get('class')}'>")
        
        # Check if it's inside a link
        if parent.name == 'a':
            print(f"    TARGET HREF: {parent.get('href')}")
        else:
            # Look for a link nearby
            link = parent.find('a')
            if link:
                print(f"    Nearby Link: {link.get('href')}")
            else:
                print("    No link found nearby.")

if __name__ == "__main__":
    xray_matchday()