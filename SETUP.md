# Project Setup Guide (Mac/Linux)

Follow these steps to set up the Spanish Football Analytics project environment.

## 1. System Requirements
* **OS:** macOS (Apple Silicon recommended) or Linux.
* **Python:** Version **3.10** or higher (Tested on **3.13**).
* **Database:** PostgreSQL Version **15** or higher.

## 2. Install System Dependencies
Ensure you have **Homebrew** installed.
```bash
brew --version
```

If you don't have Python or PostgreSQL installed yet:

```bash
# Install Python 3.13
brew install python@3.13

# Install PostgreSQL (Latest)
brew install postgresql

# Start the PostgreSQL service
brew services start postgresql
```

## 3. Database Initialization

Run these commands in your terminal to create the user and database.

*Note: If you have already done this, you can skip to Step 4.*

```bash
# 1. Create the database
createdb spanish_football

# 2. Create the specific user for the scripts
psql postgres -c "CREATE USER runner WITH PASSWORD 'football_password';"

# 3. Grant permissions
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE spanish_football TO runner;"
```

## 4. Python Environment Setup

```bash
# 1. Create Virtual Environment
# We explicitly use python3.13 to match the tested environment
python3.13 -m venv venv

# 2. Activate Environment
source venv/bin/activate

# 3. Upgrade pip (good practice)
pip install --upgrade pip

# 4. Install Dependencies
pip install -r requirements.txt
```

## 5. Verification

To verify that the connections to Understat, ESPN, and your local PostgreSQL database are working correctly, run the preview script:

```bash
python playground/test_db_preview.py
```

*Expected Output:* A text preview of Match, Player, and Lineup tables without errors.

## 6. Project Architecture

* **Source 1:** `Understat`
* *Metrics:* Goals, xG, xA, xGChain, xGBuildup.


* **Source 2:** `ESPN`
* *Metrics:* Lineups, Positions, Fouls, Cards, Saves.


* **Database:** PostgreSQL
* **Core Libraries:** `soccerdata`, `pandas`, `psycopg2`