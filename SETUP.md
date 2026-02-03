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

## 5. Security Configuration (New)

This project uses environment variables to keep passwords safe.

1. **Create the secrets file:**
```bash
touch .env

```


2. **Add your credentials to `.env`:**
Open the file and paste the following (matching the user you created in Step 3):
```ini
DB_NAME=spanish_football
DB_USER=runner
DB_PASSWORD=football_password
DB_HOST=localhost

```



## 6. 🚀 Running the Pipeline

Now that the environment is set, follow this workflow to build the data warehouse.

### Step 1: Initialize Schema

Wipes the database and applies the correct table structure.

```bash
python reset_db.py

```

### Step 2: Data Ingestion (ETL)

Scrapes data from Understat/ESPN and populates the database.

```bash
python ingest_season.py

```

### Step 3: Analysis

Run the preview queries to verify the data.

```bash
python playground/quick_analysis.py

```

```

---

### One important check
The text you pasted as "how it looks right now" (with the table definitions like `matches`, `player_stats`) is actually **very valuable documentation**. It belongs in `DB_SCHEMA.md`.

If you don't have `DB_SCHEMA.md` right now, you should create it and paste that text there so you don't lose your data dictionary.

```