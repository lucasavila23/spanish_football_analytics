import argparse
import sys
import time
from modules.reset_db import reset_database
from modules.ingest_season import run_ingestion
from modules.ingest_news import run_news_ingestion  # Added

# ==============================================================================
# SPANISH FOOTBALL ANALYTICS - MASTER ORCHESTRATOR
# ==============================================================================
# Usage:
#   python main.py --reset --seasons 2022 2023   (Reset DB + Load specific years)
#   python main.py --seasons 2024                (Just append 2024)
#   python main.py --news 2023                   (Scrape news for 2023)
#   python main.py --reset                       (Just wipe DB)
# ==============================================================================

def main():
    # 1. SETUP ARGUMENT PARSER
    parser = argparse.ArgumentParser(description="Spanish Football Data Pipeline Orchestrator")
    
    # Argument: --reset (Flag)
    parser.add_argument(
        "--reset", 
        action="store_true", 
        help="WARNING: Wipes the entire database before processing."
    )
    
    # Argument: --seasons (List of strings)
    parser.add_argument(
        "--seasons", 
        nargs="+", 
        default=[], 
        help="List of seasons to ingest (e.g., 2022 2023). If empty, no data is loaded."
    )

    # Argument: --news (Integer)
    parser.add_argument(
        "--news", 
        type=int,
        help="Year of the season to scrape news for (e.g., 2023)."
    )

    args = parser.parse_args()

    # 2. EXECUTE LOGIC
    print("\nSPANISH FOOTBALL PIPELINE")
    print("=========================")

    # Step A: Reset Database (if requested)
    if args.reset:
        print("\n[ACTION] Resetting Database...")
        confirm = input("WARNING: Are you sure you want to drop all tables? (y/n): ")
        if confirm.lower() == 'y':
            reset_database()
            print("[OK] Database reset complete.")
        else:
            print("[CANCELLED] Reset cancelled.")
            if not args.seasons and not args.news:
                sys.exit(0)

    # Step B: Ingest Seasons (if requested)
    if args.seasons:
        print(f"\n[ACTION] Starting Ingestion for seasons: {args.seasons}")
        
        try:
            run_ingestion(seasons=args.seasons)
        except TypeError:
             print("[ERROR] Your ingest_season.py needs to accept a 'seasons' argument.")
             print("Please update ingest_season.py first.")
             sys.exit(1)
             
        print("\n[SUCCESS] Stats Ingestion Finished.")

    # Step C: Ingest News (if requested)
    if args.news:
        print(f"\n[ACTION] Starting News Scraper for season: {args.news}")
        
        try:
            run_news_ingestion(args.news)
        except Exception as e:
             print(f"[ERROR] News ingestion failed: {e}")
             sys.exit(1)
             
        print("\n[SUCCESS] News Ingestion Finished.")

    if not any([args.reset, args.seasons, args.news]):
        print("[INFO] No actions selected. Use --help to see options.")

if __name__ == "__main__":
    main()