import requests

def probe_urls():
    # We test with "Jornada 11" of the 2023-24 season (El Clasico week)
    candidates = [
        # Pattern A: Statistical Subdomain (Most likely for Archives)
        "https://www.marca.com/estadisticas/futbol/primera/2023_24/jornada_11.html",
        
        # Pattern B: Standard Football Section
        "https://www.marca.com/futbol/primera-division/clasificacion/2023-24/jornada_11.html",
        
        # Pattern C: AS.com (Backup Source - often easier to scrape)
        "https://resultados.as.com/resultados/futbol/primera/2023_2024/jornada/regular_a_11"
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)'
    }

    print(" Probing URL patterns for Season 2023/24...\n")

    for url in candidates:
        try:
            response = requests.get(url, headers=headers, timeout=5)
            status = response.status_code
            
            icon = "✅" if status == 200 else "❌"
            print(f"{icon} [{status}] {url}")
            
            # If we find a winner, print a snippet of the content to ensure it's not a fake 200
            if status == 200:
                if "cronica" in response.text.lower() or "crónica" in response.text.lower():
                    print("    SUCCESS: Found 'cronica' keyword in HTML!")
                else:
                    print("    WARNING: Page loads, but 'cronica' keyword not found.")

        except Exception as e:
            print(f" Error connecting to {url}: {e}")

if __name__ == "__main__":
    probe_urls()