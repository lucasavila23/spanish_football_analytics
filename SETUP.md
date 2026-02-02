# Spanish Football Database Schema (2023 Season)

## 1. Architecture Overview
This database combines deep analytics from **Understat** with tactical line-up data from **ESPN**.
* **Database Engine:** PostgreSQL
* **Primary Key Strategy:** `matches.id` is the central hub. All player data links back to this ID.
* **Linking Logic:** Understat and ESPN data are merged based on `Date` and `Team Name` using a Python-based dictionary map.

---

## 2. Table: `matches` (The Hub)
*Source: Understat*
*Description: The central record for every game played.*

| Column Name | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `id` | SERIAL (PK) | Unique ID for the match | `101` |
| `date` | DATE | Date of the match | `2023-08-12` |
| `home_team` | TEXT | Name of the home team | `Athletic Club` |
| `away_team` | TEXT | Name of the away team | `Real Madrid` |
| `home_score` | INT | **Goals scored by Home Team** | `0` |
| `away_score` | INT | **Goals scored by Away Team** | `2` |
| `home_xg` | NUMERIC | Total Expected Goals (Home) | `0.45` |
| `away_xg` | NUMERIC | Total Expected Goals (Away) | `1.89` |

---

## 3. Table: `player_stats` (The Engine)
*Source: Understat*
*Description: Advanced metric performance for every player in a specific match.*

| Column Name | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `id` | SERIAL (PK) | Unique ID for this performance record | `5001` |
| `match_id` | INT (FK) | Links to `matches.id` | `101` |
| `team` | TEXT | Player's Team | `Real Madrid` |
| `player_name` | TEXT | Player's Name | `Rodrygo` |
| `minutes` | INT | Minutes played | `79` |
| `goals` | INT | Actual goals scored | `1` |
| `assists` | INT | Actual assists | `0` |
| `shots` | INT | Total shots taken | `3` |
| `xg` | NUMERIC | Expected Goals (Quality of chances) | `0.28` |
| `xa` | NUMERIC | Expected Assists | `0.00` |
| `xg_chain` | NUMERIC | Involvement in xG sequences | `0.85` |
| `xg_buildup` | NUMERIC | Involvement in build-up play | `0.52` |
| `key_passes` | INT | Passes leading to a shot | `1` |
| `yellow_card` | INT | Yellow cards received | `0` |
| `red_card` | INT | Red cards received | `0` |

---

## 4. Table: `lineups` (The Tactics & Actions)
*Source: ESPN*
*Description: Tactical positions and defensive/action stats.*

| Column Name | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `id` | SERIAL (PK) | Unique ID for this lineup entry | `9001` |
| `match_id` | INT (FK) | Links to `matches.id` | `101` |
| `team` | TEXT | Player's Team | `Real Madrid` |
| `player_name` | TEXT | Player's Name | `Jude Bellingham` |
| `position` | TEXT | Tactical Position played | `Center Midfielder` |
| `is_starter` | BOOLEAN | True if started, False if sub | `TRUE` |
| `shots_on_target` | INT | Shots that were on goal | `1` |
| `fouls_committed` | INT | Fouls committed by player | `2` |
| `fouls_suffered` | INT | Fouls suffered by player | `3` |
| `offsides` | INT | Times caught offside | `0` |
| `saves` | INT | (GK Only) Saves made | `0` |
| `goals_conceded` | INT | (GK/Def) Goals allowed while on pitch | `0` |

---

## 5. ETL Implementation Strategy

### 1. Extract (Scraping)
* **Understat:** Fetches aggregated season data for Matches and Players.
* **ESPN:** Fetches match-by-match lineups.
* **Normalization:** All column names are standardized to `snake_case` (e.g., `xG` -> `xg`) via a helper function to resolve naming conflicts between sources.

### 2. Transform (The Bridge)
Since sources do not share a common ID, we implement a **Dictionary Mapping Strategy**:
1.  Insert all Matches into the database first.
2.  Query the database to retrieve the generated `id` for every match.
3.  Create a composite key map in Python: `{"YYYY-MM-DD|HomeTeam": match_id}`.
4.  When processing Players and Lineups, use this map to resolve the correct foreign key (`match_id`).

### 3. Load (Insertion)
* **Batch Processing:** Uses `executemany` for high-performance insertion.
* **Idempotency:** The `matches` table uses `ON CONFLICT DO NOTHING` to prevent duplicates if the pipeline is re-run.