# Project Setup Guide (Mac/Linux)

Follow these steps to set up the project on a new machine.

## 1. Prerequisites
Ensure you have **Homebrew** installed (Mac only).
`brew --version`

## 2. Install Database (PostgreSQL)
We use PostgreSQL with the `pgvector` extension for AI capabilities.

```bash
# Install Postgres and Vector extension
brew install postgresql
brew install pgvector

# Start the service
brew services start postgresql

# Project Setup Guide

## Database Commands used:
1. createdb spanish_football
2. psql postgres -c "CREATE USER runner WITH PASSWORD 'football_password';"
3. psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE spanish_football TO runner;"

## Python Commands used:
1. python -m venv venv
2. source venv/bin/activate
3. pip install -r requirements.txt