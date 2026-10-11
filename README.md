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
| **[🎯 The Scouting Playbook](docs/SCOUTING_PLAYBOOK.md)** | Step-by-step recruitment workflows, query recipes for 5 common transfer scenarios, and decision matrices. |
| **[🏗️ System Architecture & Design](docs/ARCHITECTURE.md)** | Medallion architecture (Bronze/Silver/Gold), Star Schema design, SCD Type 2 time-travel, audit logging, and design decisions. |
| **[📖 Data Dictionary & Catalog](docs/DATA_DICTIONARY.md)** | Full schema catalog covering all tables, views, columns, data types, business definitions, keys, and nullability constraints. |
| **[📐 Metrics & Similarity Mathematics](docs/METRICS_AND_SIMILARITY.md)** | Mathematical formulation of scale-weighted Euclidean distance, finishing shrinkage, tactical archetypes, and per-90 metrics. |
| **[🚀 Pipeline Runbook & Operations](docs/PIPELINE_RUNBOOK.md)** | Step-by-step setup, database creation, execution orchestration, ad-hoc scouting procedures, automated testing, and troubleshooting. |
| **[🎮 FM24 Data Export Guide](docs/FM_EXPORT_GUIDE.md)** | Practical walkthrough on setting up custom views in FM24, exporting CSV/HTML data, file placements, and configuring for any club/save. |
| **[🐍 Python Analytics & Dashboard](docs/PYTHON_ANALYTICS.md)** | CLI documentation for `scouting_dashboard.py`, generating radar charts, Moneyball scatter plots, and CSV recruitment shortlists. |

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
        B2[(bronze.fmdata_interested_players)]
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
* **[`gold.mart_outfield_scouting_summary`](sql%20scripts/gold/views-gold.sql#L37):** Wide summary mart combining demographics, financials, and tactical metrics across both your squad and scouted external targets.
* **[`gold.mart_gk_scouting_summary`](sql%20scripts/gold/views-gold.sql#L128):** Dedicated goalkeeper mart featuring shot-stopping delta, hold rate %, sweeper keeper activity, and distribution efficiency.
* **[`gold.vw_transfer_targets_outfield`](sql%20scripts/gold/views-gold.sql#L198):** Pre-filtered scouting target pool (`is_own_squad = 0`, `minutes >= 900`) enriched with `PERCENT_RANK()` percentiles across key performance indices.

### 2. Positional Replacement Similarity Engines
Instead of subjective scouting, the warehouse features **scale-weighted Euclidean distance profile matchers** that compare external targets against your primary starting lineup (`is_own_squad = 1`, ordered by minutes played):

$$D(\mathbf{Target}, \mathbf{Incumbent}) = \sqrt{\sum_{i=1}^n w_i \cdot \left(\text{Target}_i - \text{Incumbent}_i\right)^2}$$

| Position | Engine View | Tactical Focus & Metrics |
| :--- | :--- | :--- |
| **Striker (ST)** | [`gold.vw_similar_strikers`](sql%20scripts/gold/views-gold.sql#L487) | $NPxG/90$, Shot on Target %, Aerials Won, Heading Acc, $xG/\text{shot}$, Goal Threat Share |
| **Central Defender (CD)** | [`gold.vw_similar_central_defenders`](sql%20scripts/gold/views-gold.sql#L549) | Aerial Duels Won, Heading Acc, Progressive Passes, Tackle %, Reactive vs Positional, Discipline Index |
| **Fullback / Wingback** | [`gold.vw_similar_flank_defenders`](sql%20scripts/gold/views-gold.sql#L614) | Completed Crosses/90, Open-play Cross Acc, Progressive Rate/Pass, Turnover %, Sprints/90 |
| **Defensive Midfielder (DM)** | [`gold.vw_similar_defensive_midfielders`](sql%20scripts/gold/views-gold.sql#L674) | Defensive Actions/90, Tackle Acc, Reactive vs Positional, Aerials Won, Pass Acc, Pressure Efficiency |
| **Central Midfielder (CM)** | [`gold.vw_similar_central_midfielders`](sql%20scripts/gold/views-gold.sql#L750) | Progressive Passes/90, Progression Share, Passes Attempted, Pass Acc, $NPxG+xA$, Press Efficiency |
| **Winger / Wide Attacker** | [`gold.vw_similar_wingers`](sql%20scripts/gold/views-gold.sql#L824) | $NPxG+xA$, Dribbles/90, Completed Crosses/90, $xA/\text{OP-KP}$, Sprints/90, Goal Threat Share |
| **Attacking Midfielder (CAM)**| [`gold.vw_similar_attacking_midfielders`](sql%20scripts/gold/views-gold.sql#L899) | $NPxG+xA$, $xA/\text{OP-KP}$, Open-Play Key Passes/90, Progression Share, Turnover %, Press Efficiency |
| **Goalkeeper (GK)** | [`gold.vw_similar_goalkeepers`](sql%20scripts/gold/views-gold.sql#L972) | Shot Stopping Delta ($\text{Save \%} - \text{xSave \%}$), $xG\text{ Prevented}$, Hold Rate %, Sweeper Index, Distribution |

### 3. Ad-Hoc Player Replacement Stored Procedure
Want to replace a specific aging star, an injured player, or find the closest budget alternative to an opposition superstar?
```sql
-- Search replacements for any specific player in the database:
EXEC gold.usp_scout_similar_players @TargetPlayerName = 'Edin Džeko', @TopN = 10;
EXEC gold.usp_scout_similar_players @TargetPlayerName = 'Rolando Mandragora', @TopN = 5;
```

### 4. Squad Succession Planning & Bargain Watch
* **[`gold.vw_squad_succession_plan`](sql%20scripts/gold/views-gold.sql#L1204):** Categorizes your squad into *Prospects (U23)*, *Peak (24–29)*, and *Aging Starters ($\ge 30$)*. For every aging starter, it automatically surfaces the top 3 U23 successors from the scouting pool based on positional profile match and cost efficiency.
* **[`gold.vw_bargain_watch`](sql%20scripts/gold/views-gold.sql#L1312):** Surfaces players with elite per-90 output whose transfer value or wages are drastically below market value:
  $$\text{Cost per xGI} = \frac{\text{Transfer Value (€M)} \times 1000 + \text{Annual Wage (€k)}}{\text{NPxG} + \text{xA}}$$
* **[`gold.vw_tactical_archetypes`](sql%20scripts/gold/views-gold.sql#L1338):** Automatically classifies players into FM tactical roles (*Ball-Playing Defender*, *Stopper*, *Deep-Lying Playmaker*, *Ball-Winning Midfielder*, *Inside Forward*, *Poacher*, *Sweeper Keeper*).

### 5. Multi-Season Player Growth & Development
With Slowly Changing Dimensions (SCD Type 2), historical seasons are never overwritten:
* **[`gold.mart_outfield_historical_snapshots`](sql%20scripts/gold/views-gold.sql#L1021):** Full chronological record of all player seasons.
* **[`gold.vw_player_growth_tracking`](sql%20scripts/gold/views-gold.sql#L1063):** Computes exact season-over-season deltas:
  $$\Delta (\text{NPxG} + \text{xA}), \quad \Delta \text{Progressive Passes}, \quad \Delta \text{Transfer Value}, \quad \Delta \text{Defensive Actions}$$

---

## ⚡ Quickstart & Sample Analytical Queries

### 1. Execute the Master Pipeline
```sql
USE DW;
GO
EXEC dbo.usp_run_warehouse_pipeline @Season = '2023/24', @RunTests = 1;
```

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

## 📂 Repository Structure

```text
football-manager-warehouse-project/
├── docs/                               # Comprehensive project documentation
│   ├── README.md                       # Documentation navigation hub
│   ├── SCOUTING_PLAYBOOK.md            # Step-by-step practical recruitment workflows
│   ├── ARCHITECTURE.md                 # Medallion design, SCD2, Star Schema, ADRs
│   ├── DATA_DICTIONARY.md              # Complete catalog of tables, views & columns
│   ├── METRICS_AND_SIMILARITY.md       # Equations, Euclidean weights, archetypes
│   ├── PIPELINE_RUNBOOK.md             # Deployment, execution & troubleshooting
│   ├── FM_EXPORT_GUIDE.md              # In-game FM24 view setup & export guide
│   └── PYTHON_ANALYTICS.md             # Python CLI, radar charts & scatter plots
├── documents/                          # Raw FM24 CSV exports (teams and targets)
├── python scripts/
│   ├── scouting_dashboard.py           # Python Moneyball radar charts, scatter plots & exporter
├── sql scripts/
│   ├── init_database.sql               # Database & schema creation script
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
│       └── test_vw_fiorentina_central_defender_reports.sql
├── scouting_shortlist.csv              # Exported Top 50 recruitment shortlist
└── README.md                           # Main repository overview & quickstart
```

---

## 👤 Author
**Olufeoluwa (Medal)**  
Data Engineer & Football Analytics Enthusiast.
