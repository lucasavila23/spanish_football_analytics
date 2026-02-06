# Spanish Football Predictive Analytics

![Status](https://img.shields.io/badge/Status-Development-blue)
![Course](https://img.shields.io/badge/Course-Big_Data_Analytics-orange)
![University](https://img.shields.io/badge/Context-University_Project-green)

## Project Overview
This project is developed as part of the **Big Data Analytics** university course.

The goal is to build a high-performance predictive analytics engine for Spanish La Liga football matches. Unlike standard statistical models that rely on simple goals/assists, this project implements a robust **ETL (Extract, Transform, Load)** pipeline to fuse two distinct data layers:

1.  **The Engine (Statistical Layer):** Advanced metrics like Expected Goals (xG), xA, and xGChain from *Understat*.
2.  **The Context (Tactical Layer):** Starting lineups, player positions, and defensive actions from *ESPN*.

By merging these datasets into a normalized PostgreSQL warehouse, we aim to train predictive models that understand *context*—not just who scored, but who created the play and how tactical formations influence match outcomes.



## Architecture
The system follows a modern Data Warehousing architecture:

* **Ingestion:** Python-based scrapers (`soccerdata`) fetching raw data from multiple web sources.
* **Transformation:** Cleaning, normalization, and entity resolution (linking players across different sources).
* **Storage:** A relational **PostgreSQL** database with optimized schemas for analytical querying.

```mermaid
graph TD
    %% 1. The Source Layer
    subgraph Sources
        A[Understat] -->|Scraper| C(ingest_season.py)
        B[ESPN] -->|Scraper| C
    end

    %% 2. The Storage Layer (The Hub)
    C -->|Batch Insert| DB[(PostgreSQL Database)]
    
    subgraph Schema
        DB -.-> T1[Table: matches]
        DB -.-> T2[Table: player_stats]
        DB -.-> T3[Table: lineups]
    end

    %% 3. The Analytics Layer
    subgraph "Analytics & Visualization"
        T1 -->|Read Scores| G[quick_analysis.py]
        T2 -->|Read xG Metrics| G
        
        T2 -->|Read Performance| H[TBD]
        T3 -->|Read Tactics| H
    end
    
    %% Styling for the cool cylinder
    style DB fill:#336791,stroke:#333,stroke-width:2px,color:#fff
```

## Data Dictionary
The database consists of three core tables designed for granular analysis:

| Table | Description | Key Metrics | Source |
| :--- | :--- | :--- | :--- |
| `matches` | The central fact table for game metadata. | Date, Score, Aggregate xG, season | Understat |
| `player_stats` | Performance metrics for the "Engine". | xG, xA, xGChain, xGBuildup | Understat |
| `lineups` | Tactical context and defensive actions. | Positions, Fouls, Saves, Cards | ESPN |

## Tech Stack
* **Language:** Python 3.13
* **Database:** PostgreSQL 15+
* **ETL & Cleaning:** `soccerdata`, `pandas`, `unidecode`
* **Database Drivers:** `psycopg2`, `SQLAlchemy`
* **Orchestration:** `argparse` (CLI)
* **Environment:** Virtual Environment (venv) on macOS/Linux

## Installation & Setup
For detailed instructions on setting up the environment, installing dependencies, and initializing the database, please refer to the **[SETUP.md](SETUP.md)** file.

**Quick Start:**
1.  Install dependencies: `pip install -r requirements.txt`
2.  Run the Master Pipeline (Reset DB + Load 2022/2023)

## Disclaimer
This project is for educational and research purposes only. Data scraping adheres to the `robots.txt` policies of the respective sources where applicable.

---
*Created by Lucas Avila for Big Data Analytics, 2026*