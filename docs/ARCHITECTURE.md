# Project Architecture & File Structure

This document provides a technical overview of the **Spanish Football Analytics** repository. It explains the purpose of every directory and key file in the system.

---

## Root Directory (`/`)
The entry point and configuration center of the application.

* **`main.py`**
    * **Role:** The Command Line Interface (CLI) Orchestrator.
    * **Usage:** `python main.py --season 2024` or `python main.py --reset`.
    * **Logic:** It parses arguments and calls the appropriate modules (ETL or Database Reset).
* **`.env`**
    * **Role:** Security & Secrets.
    * **Content:** Stores sensitive credentials like `DB_PASSWORD` and `DB_USER`.
    * **Note:** This file is ignored by Git to prevent leaking secrets.
* **`.gitignore`**
    * **Role:** Version Control Rules.
    * **Content:** Lists files/folders that Git should ignore (e.g., `venv/`, `.env`, `__pycache__`).
* **`requirements.txt`**
    * **Role:** Dependency Management.
    * **Content:** Lists all Python libraries required to run the project (e.g., `pandas`, `psycopg2`, `soccerdata`).

---

## Modules (`/modules`)
The "Engine Room" containing the core business logic.

* **`ingest_season.py`**
    * **Role:** Extract, Transform, Load (ETL) Pipeline.
    * **Logic:**
        1.  **Scrape:** Fetches data from Understat and ESPN using `soccerdata`.
        2.  **Transform:** Normalizes team names, fixes missing dates, and maps player IDs.
        3.  **Load:** Inserts data into PostgreSQL (`matches`, `player_stats`, `lineups`).
* **`news_scraper.py`**
    * **Role:** Web Scraper (AS.com).
    * **Logic:** Navigates to match reports ("Crónicas") and extracts the headline, subheader, and URL context.
* **`ingest_news.py`**
    * **Role:** News Linker.
    * **Logic:** Matches scraped headlines to specific database matches using fuzzy name matching and date heuristics (Jornada logic).
* **`reset_db.py`**
    * **Role:** Database Schema Management.
    * **Logic:** Drops existing tables and rebuilds the schema from scratch. Used when the schema changes or for a "Clean Slate" run.
* **`utils.py`**
    * **Role:** Shared Utilities.
    * **Logic:** Contains helper functions used across the project (e.g., `run_test_query` for running SQL checks safely).
* **`__init__.py`**
    * **Role:** Package Marker.
    * **Logic:** An empty file that tells Python to treat this directory as a package, allowing imports like `from modules import utils`.

---

## Tests (`/tests`)
Automated Quality Assurance (QA) scripts.

* **`master_test.py`**
    * **Role:** Unified Integrity Suite.
    * **Usage:** `python tests/master_test.py --season 2024`
    * **Logic:** Runs three critical checks:
        1.  **Volume:** Are there 380 matches? Do stats/lineups counts match?
        2.  **Consistency:** Are there "Ghost Stats" (records with no matching game)?
        3.  **Reality:** Does the calculated league table match real life (e.g., correct Champion/Points)?

---

## Documentation (`/docs`)
The project's knowledge base.

* **`DEV_LOG.md`**
    * **Role:** History of Changes.
    * **Content:** A chronological journal of decisions, bug fixes, and milestones.
* **`SETUP.md`**
    * **Role:** Onboarding Guide.
    * **Content:** Step-by-step instructions to install Python, Postgres, and run the pipeline.
* **`ARCHITECTURE.md`**
    * **Role:** System Overview.
    * **Content:** The file you are reading right now.

---

## Notebooks (`/notebooks`)
Experimental laboratory for data analysis.

* **`playground/`**
    * **Role:** Sandbox.
    * **Content:** Contains `.ipynb` (Jupyter Notebooks) or loose scripts for quick experiments, visualization prototyping, or testing new ideas before they move to `modules`.
    * **`inspect_headlines.py`:** A tool to view scraped headlines and verify they look correct.
    * **`manual_verification.py`:** A deep-dive tool to verify specific database rows against reality.

---

## Data (`/data`) [Auto-Generated]
* **`soccerdata/`**
    * **Role:** Caching Layer.
    * **Content:** The `soccerdata` library automatically saves scraped files here (JSON/CSV) so you don't have to re-download them every time.