import argparse
import sys
import time
from modules.reset_db import reset_database
from modules.ingest_season import run_ingestion

# ==============================================================================
# SPANISH FOOTBALL ANALYTICS - MASTER ORCHESTRATOR
# ==============================================================================
# Usage:
#   python main.py --reset --seasons 2022 2023   (Reset DB + Load specific years)
#   python main.py --seasons 2024                (Just append 2024)
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
            if not args.seasons:
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
             
        print("\n[SUCCESS] Pipeline Execution Finished.")
    else:
        if not args.reset:
            print("[INFO] No actions selected. Use --help to see options.")

if __name__ == "__main__":
    main()