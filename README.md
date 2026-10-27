# ⚽ Football Manager 24: Data Warehouse & Moneyball Analytics Engine

[![SQL Server](https://img.shields.io/badge/Database-SQL%20Server%202019%2B-red.svg)](https://www.microsoft.com/sql-server)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20%7C%20Silver%20%7C%20Gold)-blue.svg)](docs/ARCHITECTURE.md)
[![Methodology](https://img.shields.io/badge/Analytics-Moneyball%20%26%20SCD%20Type%202-success.svg)](docs/METRICS_AND_SIMILARITY.md)
[![Tests](https://img.shields.io/badge/Quality-Automated%20TDD%20%26%20Integrity%20Suites-brightgreen.svg)](docs/PIPELINE_RUNBOOK.md)
[![License](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)

Welcome to the **Football Manager Data Warehouse & Moneyball Recruitment Engine** (`DW`).

This platform bridges **modern enterprise Data Engineering** (Medallion Architecture, Kimball Star Schema, Slowly Changing Dimensions Type 2, transactional testing, and audit logging) with **cutting-edge football analytics** (scale-weighted Euclidean profile similarity matching, regression-shrunk finishing indices, and value-efficiency scouting marts) to run data-driven, Moneyball-style management saves in Football Manager 24.

---

## 📚 Complete Documentation Suite

Comprehensive technical, analytical, and operational documentation is available in the [`docs/`](docs/README.md) directory:

| Guide | Description |
| :--- | :--- |
| **[🔰 Zero-Coding Setup Guide](docs/BEGINNER_SETUP_GUIDE.md)** | Step-by-step setup guide for non-coders across **Windows**, **macOS**, and **Linux** with 1-click startup scripts. |
| **[🎯 The Scouting Playbook](docs/SCOUTING_PLAYBOOK.md)** | Step-by-step recruitment workflows, query recipes for 5 common transfer scenarios, and decision matrices. |
| **[🏗️ System Architecture & Design](docs/ARCHITECTURE.md)** | Medallion architecture (Bronze/Silver/Gold), Star Schema design, SCD Type 2 time-travel, audit logging, and design decisions. |
| **[📖 Data Dictionary & Catalog](docs/DATA_DICTIONARY.md)** | Full schema catalog covering all tables, views, columns, data types, business definitions, keys, and nullability constraints. |
| **[📐 Metrics & Similarity Mathematics](docs/METRICS_AND_SIMILARITY.md)** | Mathematical formulation of scale-weighted Euclidean distance, finishing shrinkage, tactical archetypes, and per-90 metrics. |
| **[🚀 Pipeline Runbook & Operations](docs/PIPELINE_RUNBOOK.md)** | Step-by-step setup, database creation, execution orchestration, ad-hoc scouting procedures, automated testing, and troubleshooting. |
| **[🎮 FM24 Data Export Guide](docs/FM_EXPORT_GUIDE.md)** | Practical walkthrough on importing pre-configured `.fmf` views in FM24, exporting CSV/HTML data, and zero-effort setup for any club. |
| **[🐍 Python Analytics & Dashboard](docs/PYTHON_ANALYTICS.md)** | CLI documentation for `scouting_dashboard.py`, generating radar charts, Moneyball scatter plots, and CSV recruitment shortlists. |
| **[🌐 Web Analytics Application](webapp/README.md)** | Full Next.js webapp documentation: Opta pizza charts, FBref scouting reports, TradingView quant screener, and 1-click startup. |

---

## 🏗️ Architecture & Pipeline Flow

The warehouse is built on Microsoft SQL Server following the **Medallion Architecture**, progressing from raw CSV landing zones to clean normalized facts/dimensions, and finally to analytical reporting marts and tactical scouting engines:

```mermaid
flowchart TD
    subgraph Data Sources ["📁 FM24 Data Exports (/documents)"]
        A1[team_players.csv]
        A2[team_gks.csv]
        A3[interested_out_players*.csv]
        A4[interested_gks.csv]
    end

    subgraph Bronze ["🥉 Bronze Layer (Landing Zone)"]
        B1[(bronze.fmdata_team_players)]
        B2[(bronze.fmdata_interested_out_players)]
        B_P[EXEC bronze.load_bronze]
    end

    subgraph Silver ["🥈 Silver Layer (Cleaning & Typing)"]
        S1[(silver.fmdata_team_players)]
        S2[(silver.fmdata_interested_out_players)]
        S3[(silver.fmdata_team_gks)]
        S_P[EXEC silver.load_silver]
    end

    subgraph Gold ["🥇 Gold Layer (Star Schema & SCD2)"]
        G_DIM[(Dimensions: Player, Team, Club, League)]
        G_FACT[(Facts: Stats, GK, Value with SCD2)]
        G_MART[Analytical Marts & Similarity Views]
        G_P[EXEC gold.load_gold @Season = '2023/24']
    end

    subgraph Analytics ["📊 Analytics & Decision Layer"]
        V1[8x Positional Similarity Engines]
        V2[Squad Succession Planning]
        V3[Player Growth Tracking]
        V4[Python Visual Dashboard]
    end

    Data Sources --> B_P --> Bronze
    Bronze --> S_P --> Silver
    Silver --> G_P --> Gold
    Gold --> Analytics
```

---

## 🥇 Gold Layer: Core Analytical Features

### 1. Unified Scouting Marts
* **[`gold.mart_outfield_scouting_summary`](sql%20scripts/gold/views-gold.sql#L42):** Wide summary mart combining demographics, financials, and tactical metrics across both your squad and scouted external targets.
* **[`gold.mart_gk_scouting_summary`](sql%20scripts/gold/views-gold.sql#L128):** Dedicated goalkeeper mart featuring shot-stopping delta, hold rate %, sweeper keeper activity, and distribution efficiency.
* **[`gold.vw_transfer_targets_outfield`](sql%20scripts/gold/views-gold.sql#L180):** Pre-filtered scouting target pool (`is_own_squad = 0`, `minutes >= 900`) enriched with `PERCENT_RANK()` percentiles across key performance indices.

### 2. Positional Squad Performance Reports
Deep positional evaluations of your own squad members and contracted players ($\text{Minutes} \ge 1,000$) to identify team weaknesses and benchmark starter profiles:
* **[`gold.vw_squad_central_defender_detailed_reports`](sql%20scripts/gold/views-gold.sql#L235):** In-depth tactical CD report with weighted defensive actions ($W_{\text{def}}$), line-breaking progression rate, and key passes per progression ($kP/\text{ProgP}$).
* **[`gold.vw_squad_central_defender_reports`](sql%20scripts/gold/views-gold.sql#L321):** Standard squad CD performance appraisal.
* **[`gold.vw_squad_flank_defender_reports`](sql%20scripts/gold/views-gold.sql#L351):** Fullback & wingback report covering crossing threat, ball retention, and high-intensity sprint volume.
* **[`gold.vw_squad_midfielder_reports`](sql%20scripts/gold/views-gold.sql#L385):** Central midfielder report analyzing passing tempo, dual-phase output, and duel win rates.
* **[`gold.vw_squad_attacking_midfielder_reports`](sql%20scripts/gold/views-gold.sql#L420):** Attacking midfielder and winger appraisal measuring chance creation danger ($xA/\text{OP-KP}$), take-ons, and goal threat.
* **[`gold.vw_squad_striker_reports`](sql%20scripts/gold/views-gold.sql#L454):** Forward evaluation tracking regression-shrunk finishing overperformance, shot quality ($xG/\text{shot}$), and conversion rates.

### 3. Positional Replacement Similarity Engines
Instead of subjective scouting, the warehouse features **scale-weighted Euclidean distance profile matchers** that compare external targets against your primary starting lineup (`is_own_squad = 1`, ordered by minutes played):

$$D(\mathbf{Target}, \mathbf{Incumbent}) = \sqrt{\sum_{i=1}^n w_i \cdot \left(\text{Target}_i - \text{Incumbent}_i\right)^2}$$

| Position | Engine View | Tactical Focus & Metrics |
| :--- | :--- | :--- |
| **Striker (ST)** | [`gold.vw_similar_strikers`](sql%20scripts/gold/views-gold.sql#L501) | $NPxG/90$, Shot on Target %, Aerials Won, Heading Acc, $xG/\text{shot}$, Goal Threat Share |
| **Central Defender (CD)** | [`gold.vw_similar_central_defenders`](sql%20scripts/gold/views-gold.sql#L563) | Aerial Duels Won, Heading Acc, Progressive Passes, Tackle %, Reactive vs Positional, Discipline Index |
| **Fullback / Wingback** | [`gold.vw_similar_flank_defenders`](sql%20scripts/gold/views-gold.sql#L629) | Completed Crosses/90, Open-play Cross Acc, Progressive Rate/Pass, Turnover %, Sprints/90 |
| **Defensive Midfielder (DM)** | [`gold.vw_similar_defensive_midfielders`](sql%20scripts/gold/views-gold.sql#L690) | Defensive Actions/90, Tackle Acc, Reactive vs Positional, Aerials Won, Pass Acc, Pressure Efficiency |
| **Central Midfielder (CM)** | [`gold.vw_similar_central_midfielders`](sql%20scripts/gold/views-gold.sql#L765) | Progressive Passes/90, Progression Share, Passes Attempted, Pass Acc, $NPxG+xA$, Press Efficiency |
| **Winger / Wide Attacker** | [`gold.vw_similar_wingers`](sql%20scripts/gold/views-gold.sql#L835) | $NPxG+xA$, Dribbles/90, Completed Crosses/90, $xA/\text{OP-KP}$, Sprints/90, Goal Threat Share |
| **Attacking Midfielder (CAM)**| [`gold.vw_similar_attacking_midfielders`](sql%20scripts/gold/views-gold.sql#L905) | $NPxG+xA$, $xA/\text{OP-KP}$, Open-Play Key Passes/90, Progression Share, Turnover %, Press Efficiency |
| **Goalkeeper (GK)** | [`gold.vw_similar_goalkeepers`](sql%20scripts/gold/views-gold.sql#L971) | Shot Stopping Delta ($\text{Save \%} - \text{xSave \%}$), $xG\text{ Prevented}$, Hold Rate %, Sweeper Index, Distribution |

### 4. Ad-Hoc Player Replacement Stored Procedure
Want to replace a specific aging star, an injured player, or find the closest budget alternative to an opposition superstar?
```sql
-- Search replacements for any specific player in the database:
EXEC gold.usp_scout_similar_players @TargetPlayerName = 'Edin Džeko', @TopN = 10;
EXEC gold.usp_scout_similar_players @TargetPlayerName = 'Rolando Mandragora', @TopN = 5;
```

### 5. Squad Succession Planning & Bargain Watch
* **[`gold.vw_squad_succession_plan`](sql%20scripts/gold/views-gold.sql#L1211):** Categorizes your squad into *Prospects (U23)*, *Peak (24–29)*, and *Aging Starters ($\ge 30$)*. For every aging starter, it automatically surfaces the top 3 U23 successors from the scouting pool based on positional profile match and cost efficiency.
* **[`gold.vw_bargain_watch`](sql%20scripts/gold/views-gold.sql#L1320):** Surfaces players with elite per-90 output whose transfer value or wages are drastically below market value using `attacking_bargain_index`, `defensive_bargain_index`, and:
  $$\text{Cost per xGI} = \frac{\text{Transfer Value (£M)} \times 1000 + (\text{Weekly Wage (£k)} \times 52.0)}{\text{NPxG} + \text{xA}}$$
* **[`gold.vw_tactical_archetypes`](sql%20scripts/gold/views-gold.sql#L1348):** Automatically classifies players into 21 FM tactical roles (*Ball-Playing Defender*, *Stopper*, *Deep-Lying Playmaker*, *Ball-Winning Midfielder*, *Inside Forward*, *Poacher*, *Sweeper Keeper*).

### 6. Multi-Season Player Growth & Development
With Slowly Changing Dimensions (SCD Type 2), historical seasons are never overwritten:
* **[`gold.mart_outfield_historical_snapshots`](sql%20scripts/gold/views-gold.sql#L1031):** Full chronological record of all player seasons.
* **[`gold.vw_player_growth_tracking`](sql%20scripts/gold/views-gold.sql#L1071):** Computes exact season-over-season deltas:
  $$\Delta (\text{NPxG} + \text{xA}), \quad \Delta \text{Progressive Passes}, \quad \Delta \text{Transfer Value}, \quad \Delta \text{Defensive Actions}$$
* **[`gold.vw_gk_growth_tracking`](sql%20scripts/gold/views-gold.sql#L1146):** Tracks goalkeeper development deltas ($\Delta$ Shot Stopping Delta, $\Delta$ Hold Rate %, $\Delta$ Sweeper Index).

> 💡 *For complete column schemas, grains, data types, and mathematical formulas of all 24 views, see [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md#7-analytical-views--derived-metrics-catalog).*

---

## ⚡ Quickstart & Pipeline Execution

> [!TIP]
> **🔰 Never coded before?** Follow the comprehensive **[Beginner's Zero-Coding Setup Guide](docs/BEGINNER_SETUP_GUIDE.md)** with step-by-step instructions, download links, and troubleshooting for Windows, macOS, and Linux.

The project comes pre-loaded with **over 11,350 scouted players** and a complete sample squad in `documents/`, so you can launch and explore immediately!

### 🪟 Microsoft Windows (1-Click with Docker Desktop)
1. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/) and start it.
2. In the project folder, open **PowerShell** and run:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\run_pipeline_docker.ps1
   ```
3. To start the browser scouting web app, run:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\run_webapp.ps1
   ```
   Open **[http://localhost:3000](http://localhost:3000)** in your browser!

---

### 🍏 Apple macOS (Apple Silicon M1/M2/M3/M4 & Intel)
1. Install [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/) and start it.
2. Open **Terminal**, `cd` into the project directory, and run:
   ```bash
   ./run_pipeline_docker.sh
   ```
3. To start the browser scouting web app, run:
   ```bash
   ./run_webapp.sh
   ```
   Open **[http://localhost:3000](http://localhost:3000)** in Safari or Chrome!

---

### 🐧 Linux (Ubuntu, Debian, Fedora, Arch)
1. Ensure Docker is running (`sudo systemctl start docker` or `sudo usermod -aG docker $USER`).
2. Run the automated Docker pipeline runner:
   ```bash
   chmod +x run_pipeline_docker.sh run_webapp.sh
   ./run_pipeline_docker.sh
   ```
3. Launch the web app:
   ```bash
   ./run_webapp.sh
   ```
   Open **[http://localhost:3000](http://localhost:3000)** in your browser!

---

### 🔄 Ingesting Subsequent Seasons (Multi-Season SCD2)
When you progress to a new season in FM24, ingest the new data with 1-click while preserving all historical growth:
```bash
./prepare_data.sh --season 2024/25
```
*(For detailed instructions and manual two-step workflows, see [docs/PIPELINE_RUNBOOK.md](docs/PIPELINE_RUNBOOK.md#34-ingesting-a-new-season-going-forward-multi-season-scd2)).*

---

### 🛠️ Native SQL Execution (Windows without Docker / SSMS)
If you are running native Microsoft SQL Server and SSMS:
```sql
USE DW;
GO
EXEC dbo.usp_run_warehouse_pipeline @Season = '2023/24', @RunTests = 1;
```

---

## 🔍 Sample Analytical Queries

### 2. Find Top 5 Striker Replacements for Your Starter
```sql
SELECT TOP 5
    target_player_name,
    age,
    club_name,
    league,
    transfer_value_m,
    wage_per_week_k,
    similarity_distance,
    profile_match_pct,
    target_npxg_per90,
    target_conversion_rate
FROM gold.vw_similar_strikers
ORDER BY similarity_distance ASC;
```

### 3. Find Undervalued Bargains (Cost per xGI)
```sql
SELECT TOP 10
    player_name,
    age,
    club_name,
    league,
    position,
    npxg_plus_xa_per90,
    transfer_value_m,
    wage_per_week_k,
    cost_per_xgi_k
FROM gold.vw_bargain_watch
ORDER BY cost_per_xgi_k ASC;
```

### 4. Query Succession Plan for Aging Squad Players
```sql
SELECT 
    starter_name,
    starter_age,
    position,
    u23_rank,
    successor_name,
    successor_age,
    successor_club,
    successor_transfer_value_m,
    similarity_distance
FROM gold.vw_squad_succession_plan
ORDER BY starter_name, u23_rank;
```

---

## 🐍 Python Visual Analytics & Export Tool

A standalone Python analytics engine is included in [`python scripts/scouting_dashboard.py`](python%20scripts/scouting_dashboard.py):

```bash
# Export the Top 50 Moneyball recruitment targets to CSV:
python "python scripts/scouting_dashboard.py" --mode export --top 50

# Generate a visual comparison radar chart against your squad starter:
python "python scripts/scouting_dashboard.py" --mode radar --target "Lucas Beltrán"

# Generate a Moneyball scatter plot (Output vs Valuation):
python "python scripts/scouting_dashboard.py" --mode scatter
```

---

## 🌐 Interactive Web Application (FBref & Opta Scouting Engine)

The warehouse includes a full-stack Next.js web application coupled directly to the SQL Server data warehouse. It provides an interactive scouting suite modeled after **FBref** and **Opta/StatsBomb**, complete with an interactive **TradingView-style Player Screener** for both outfield players and goalkeepers.

### ⚡ 1-Click Launch (Recommended)
From the root of the repository, execute:
```bash
./run_webapp.sh
```
The script automatically verifies your SQL Server Docker container is running, checks your Node.js runtime, installs missing dependencies, creates `webapp/.env.local` if needed, and starts the server at **[http://localhost:3000](http://localhost:3000)**.

### 🛠️ Manual Startup
```bash
cd webapp
npm install        # First time only
npm run dev        # Starts Next.js development server
```

### 🔍 Key Features & Routes
* **[Scouting Hub (Home)](http://localhost:3000/) (`/`):** Live database search across 11,359 players and clubs with position filters, valuation/minutes sliders, and quick sorting.
* **[TradingView Screener](http://localhost:3000/screener) (`/screener`):** Interactive quantitative screener featuring dynamic filter pill chips, preset strategies (*Wonderkids*, *Moneyball Gems*, *Elite Playmakers*, *Shot-Stoppers*), CSV export, and dual **Outfield** vs **🧤 Goalkeeper** modes.
* **[Player Dossier](http://localhost:3000/player/1) (`/player/[id]`):** Comprehensive player profile featuring:
  * **Opta / StatsBomb Pizza Radar Chart:** Radial slices and classic radar polygon views with category glow and hover tooltips.
  * **FBref Percentile Benchmark Table:** 0–99th percentile progress bars across Attacking, Passing, Defending, and Physical metrics.
  * **FBref Similar Players Widget:** Multi-metric Euclidean distance matching for both outfield positions and goalkeepers.
  * **Career Progression (SCD Type 2):** Longitudinal tracking of output and market valuation across historical snapshots.
* **[Head-to-Head Comparison](http://localhost:3000/compare) (`/compare`):** Side-by-side player search, overlaid dual radar chart, and statistical differential matrix.
* **[Tactical Archetypes](http://localhost:3000/archetypes) (`/archetypes`):** Browse players grouped by 22 distinct tactical profiles from `gold.vw_tactical_archetypes`.
* **[Metrics & Visual Guide](http://localhost:3000/metrics) (`/metrics`):** Dedicated reference hub defining all 30+ performance metrics, mathematical formulas, benchmarks, and interactive explanations of all charts (Pizza Radars, FBref bars, Scatter explorer).

For in-depth web application documentation, see [`webapp/README.md`](file:///home/mcmedal/Documents/Github/football-manager-warehouse-project/webapp/README.md).

---

## 📂 Repository Structure

```text
football-manager-warehouse-project/
├── docker-compose.yml                  # Docker Compose configuration for SQL Server 2022 (Linux/Mac/Windows)
├── .env.example                        # Environment variable template for Docker
├── run_pipeline_docker.sh              # 1-click Linux/Mac/Docker ETL orchestrator & test runner
├── run_webapp.sh                       # 1-click Linux/Mac launcher for Next.js scouting web application
├── run_pipeline_docker.ps1             # 1-click Windows PowerShell ETL orchestrator & test runner
├── run_webapp.ps1                      # 1-click Windows PowerShell launcher for Next.js scouting web application
├── webapp/                             # Full-stack Next.js FBref/Opta scouting web application
│   ├── app/                            # App router pages (Scouting Hub, Screener, Player Dossier, Compare, Archetypes)
│   ├── components/                     # PizzaRadarChart, PercentileTable, SimilarPlayers, Navbar
│   ├── lib/db.ts                       # MSSQL connection pool with cached connection management
│   └── README.md                       # Detailed webapp architecture & API reference
├── docs/                               # Comprehensive project documentation
│   ├── README.md                       # Documentation navigation hub
│   ├── BEGINNER_SETUP_GUIDE.md         # Zero-coding setup guide for Windows, Mac & Linux
│   ├── SCOUTING_PLAYBOOK.md            # Step-by-step practical recruitment workflows
│   ├── ARCHITECTURE.md                 # Medallion design, SCD2, Star Schema, ADRs
│   ├── DATA_DICTIONARY.md              # Complete catalog of tables, views & columns
│   ├── METRICS_AND_SIMILARITY.md       # Equations, Euclidean weights, archetypes
│   ├── PIPELINE_RUNBOOK.md             # Deployment, execution & troubleshooting
│   ├── FM_EXPORT_GUIDE.md              # In-game FM24 view setup & export guide
│   └── PYTHON_ANALYTICS.md             # Python CLI, radar charts & scatter plots
├── fm24 views/                         # Pre-configured in-game FM24 view files (.fmf) for 1-click import
│   ├── GK View.fmf                     # Scouting Goalkeeper view
│   ├── O.Player View.fmf               # Scouting Outfield Player view
│   ├── Team GK View.fmf                # Squad Goalkeeper view
│   └── Team O.Player View.fmf          # Squad Outfield Player view
├── documents/                          # Raw FM24 CSV exports (teams and targets)
├── python scripts/
│   ├── prepare_ingestion.py            # Steam Proton / Wine automated pre-processor & deduplicator
│   ├── html_to_csv.py                  # HTML export to standardized CSV converter
│   └── scouting_dashboard.py           # Python Moneyball radar charts, scatter plots & exporter
├── sql scripts/
│   ├── init_database.sql               # Database & schema creation script (UTF-8 collation)
│   ├── bronze/                         # Landing layer DDL & ingestion scripts
│   │   ├── ddl-bronze.sql
│   │   └── data-ingestion bronze.sql
│   ├── silver/                         # Data cleaning, formatting & SCD2 staging
│   │   ├── ddl-silver.sql
│   │   └── proc-silver.sql
│   ├── gold/                           # Normalized Star Schema, SCD2 & Analytical Marts
│   │   ├── ddl-gold.sql                # Star schema tables, league coefs & audit logs
│   │   ├── proc-gold.sql               # SCD Type 2 loader & ad-hoc similarity procedure
│   │   ├── views-gold.sql              # Summary marts, similarity engines & succession views
│   │   └── ERD.dbml                    # Database Markup Language diagram definition
│   ├── orchestration/
│   │   └── run_pipeline.sql            # Master orchestrator: dbo.usp_run_warehouse_pipeline
│   └── tests/                          # Automated TDD unit tests & data integrity suites
│       ├── test_data_integrity.sql
│       ├── test_vw_similar_positions.sql
│       └── test_vw_squad_central_defender_reports.sql
├── scouting_shortlist.csv              # Exported Top 50 recruitment shortlist
└── README.md                           # Main repository overview & quickstart
```

---

## 👤 Author
**Olufeoluwa (Medal)**  
Data Engineer & Football Analytics Enthusiast.
