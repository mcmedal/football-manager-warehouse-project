# 📖 Data Dictionary & Schema Catalog

This catalog documents the tables, views, columns, data types, business definitions, constraints, and grains across all schemas in `DW`.

---

## 1. Schema Overview

The database is divided into four distinct logical schemas:
* **`bronze`**: Raw landing zone for untransformed Football Manager 24 data exports.
* **`silver`**: Cleansed, sanitized, standardized, and typed tables.
* **`gold`**: Dimensional models (Kimball Star Schema), Fact tables, Dimension tables, and Analytical Views.
* **`audit`**: Enterprise pipeline monitoring, execution timestamps, and error logging.

---

## 2. Bronze Schema Tables

### `bronze.fmdata_team_players`
* **Grain:** One row per player in the user's club squad export.
* **Description:** Raw landing table for user team outfield players and goalkeepers from `team_players.csv`.
* **Key Columns:**
  * `player_id` (`VARCHAR(50)`): Raw unique identifier assigned by FM24.
  * `player_name` (`NVARCHAR(100)`): Player full name (may contain UI strings like `' - Pick Player'`).
  * `club_name` (`NVARCHAR(100)`): Club name.
  * `league` (`NVARCHAR(100)`): League name.
  * `position` (`VARCHAR(50)`): Raw positional abbreviation (e.g. `'ST (C)'`, `'D (C)'`).
  * `age` (`VARCHAR(10)`): Age in years.
  * `minutes_played` (`VARCHAR(50)`): Season minutes (contains commas and dashes).
  * Tactical stats (`VARCHAR(50)`): All shooting, passing, defending, and athletic stats stored as raw strings.

### `bronze.fmdata_team_gks`
* **Grain:** One row per goalkeeper in the user's club squad export.
* **Description:** Raw landing table for user team goalkeepers from `team_gks.csv`.
* **Key Columns:** Goalkeeping metrics (`saves_percentage`, `xsave_rate`, `saves_held_per90`, `clearances_per90`, etc.) stored as raw text.

### `bronze.fmdata_interested_players`
* **Grain:** One row per scouted player within the club's scouting range.
* **Description:** Raw landing table for outfield targets scouted via recruitment filters (`interested_out_players*.csv`).
* **Key Columns:** Demographics, contracts, transfer valuations, and per-90 performance metrics.

### `bronze.fmdata_interested_gks`
* **Grain:** One row per scouted goalkeeper within the club's scouting range.
* **Description:** Raw landing table for scouted goalkeepers from `interested_gks.csv`.

---

## 3. Silver Schema Tables

### `silver.fmdata_team_players`
* **Grain:** One row per user squad member.
* **Cleaning Applied:**
  * Trimmed and sanitized strings; stripped `' - Pick Player'`.
  * Dashes (`'-'`) converted to `0` or appropriate nulls.
  * Standardized positions to descriptive titles (`'Striker'`, `'Central Defender'`, etc.).
  * Percentage strings (`'75%'`) converted to `DECIMAL(5, 2)` ratios (`0.75`).
  * Numbers with comma separators (`'1,450'`) parsed to `INT` (`1450`).
  * Financials parsed into weekly wage (£k) and market transfer value (£M).

### `silver.fmdata_interested_out_players`
* **Grain:** One row per scouted outfield target.
* **Cleaning Applied:**
  * Deduplicated records across dynamic multi-file partition exports (`interested_out_players1.csv` ... `interested_out_players8.csv`+ or single file).
  * Filtered out players currently contracted to the user's club (`WHERE contracted = 0`) to prevent duplicate entity representation.

### `silver.fmdata_team_gks` & `silver.fmdata_interested_gks`
* **Grain:** One row per goalkeeper.
* **Cleaning Applied:**
  * Cleansed shot-stopping metrics, sweep actions, and distribution metrics into standard `DECIMAL(5, 2)` representations.

---

## 4. Gold Schema: Dimension Tables

### `gold.dim_player_info`
* **Grain:** One row per player per season snapshot.
* **Temporal Lineage:** SCD Type 2 enabled (`dwh_current_validity`, `dwh_cd_valid_till`).

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `player_key` | `INT IDENTITY(1,1)` | **NO** (PK) | Surrogate primary key for the player record snapshot. |
| `player_id` | `INT` | **NO** | Natural key originating from Football Manager. |
| `player_name` | `VARCHAR(35)` | NO | Full name of the footballer. |
| `team_key` | `VARCHAR(10)` | YES | Foreign key linking to `gold.dim_team_info`. |
| `is_own_squad` | `BIT` | NO | `1` if player belongs to the user's managed club; `0` if scouted external target. |
| `contracted` | `BIT` | NO | `1` if under professional contract; `0` if free agent. |
| `age` | `INT` | YES | Age of the player during the season snapshot. |
| `position` | `VARCHAR(28)` | YES | Standardized primary position (e.g. `'Striker'`, `'Central Defender'`). |
| `season` | `VARCHAR(9)` | NO | Competitive season identifier (e.g. `'2023/24'`). |
| `dwh_create_date` | `DATETIME2` | NO | Timestamp when snapshot became active. |
| `dwh_cd_valid_till` | `DATETIME2` | NO | Snapshot expiration timestamp (`'9999-12-31'` if currently active). |
| `dwh_current_validity` | `BIT` | NO | `1` if current active version; `0` if historical snapshot. |

### `gold.dim_team_info`
* **Grain:** One row per team per season snapshot.

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `team_key` | `VARCHAR(10)` | **NO** (PK) | Surrogate key representing the team entity. |
| `league` | `NVARCHAR(35)` | YES | Competitive league name. |
| `club_name` | `NVARCHAR(35)` | YES | Club name. |
| `dwh_create_date` | `DATETIME2` | NO | Ingestion timestamp. |
| `dwh_cd_valid_till` | `DATETIME2` | NO | Expiration timestamp. |
| `dwh_current_validity` | `BIT` | NO | Active record flag. |

### `gold.dim_team_tactical_archetypes`
* **Grain:** One row per team entity per competitive season snapshot (`dwh_current_validity = 1`).
* **Source Tables:** `gold.fact_outfield_player_stats` (minutes-weighted aggregation), `gold.dim_team_info`.
* **Business Purpose:** Central dimension classifying each club into one of 7 tactical frameworks (Gegenpressing, Tiki-Taka, Vertical Tiki-Taka, Counter-Attack, Wing Play, Route One, Catenaccio) based on minutes-weighted event metrics per 90, computing archetype affinity scores ($0\text{--}100$).

| Column Name | Data Type | Nullable | Description / Metric Formula |
| :--- | :--- | :--- | :--- |
| `team_key` | `VARCHAR(10)` | **NO** (FK) | Reference to `gold.dim_team_info`. |
| `club_name` | `NVARCHAR(35)` | NO | Club name. |
| `league` | `NVARCHAR(35)` | YES | League name. |
| `active_players` | `INT` | NO | Number of outfield players in sample ($\ge 180$ mins). |
| `total_minutes` | `INT` | NO | Cumulative minutes played by squad outfield players. |
| `tactical_archetype`| `VARCHAR(35)` | NO | Primary classified tactical archetype. |
| `tactical_framework`| `VARCHAR(50)` | NO | Parent tactical framework category (1–7). |
| `tactical_description`| `NVARCHAR(500)`| YES | Tactical description of the system. |
| `pressures_attempted_per90` | `DECIMAL(5,2)` | YES | Minutes-weighted team pressures attempted per 90. |
| `passes_attempted_per90` | `DECIMAL(5,2)` | YES | Minutes-weighted team passes attempted per 90. |
| `pass_accuracy` | `DECIMAL(5,3)` | YES | Minutes-weighted team pass accuracy ($[0.00, 1.00]$). |
| `progressive_passes_per90` | `DECIMAL(5,2)` | YES | Minutes-weighted team progressive passes per 90. |
| `crosses_attempted_per90` | `DECIMAL(5,2)` | YES | Minutes-weighted team crosses attempted per 90. |
| `headers_attempted_per90` | `DECIMAL(5,2)` | YES | Minutes-weighted team headers attempted per 90. |
| `clearances_per90` | `DECIMAL(5,2)` | YES | Minutes-weighted team clearances per 90. |
| `blocks_per90` | `DECIMAL(5,2)` | YES | Minutes-weighted team blocks per 90. |
| `gegenpressing_score` | `INT` | YES | Affinity index ($0\text{--}100$) for high-intensity pressing. |
| `tiki_taka_score` | `INT` | YES | Affinity index ($0\text{--}100$) for control possession. |
| `vertical_tiki_taka_score`| `INT` | YES | Affinity index ($0\text{--}100$) for central penetrative possession. |
| `fluid_counter_score` | `INT` | YES | Affinity index ($0\text{--}100$) for fluid counter-attacking. |
| `direct_counter_score`| `INT` | YES | Affinity index ($0\text{--}100$) for direct transition play. |
| `wing_play_score` | `INT` | YES | Affinity index ($0\text{--}100$) for horizontal flank play. |
| `route_one_score` | `INT` | YES | Affinity index ($0\text{--}100$) for aerial attrition long ball. |
| `catenaccio_score` | `INT` | YES | Affinity index ($0\text{--}100$) for deep block penalty box defending. |

### `gold.dim_club`
* **Grain:** One row per unique club entity.

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `club_key` | `INT IDENTITY(1,1)` | **NO** (PK) | Surrogate key for the club. |
| `club` | `NVARCHAR(35)` | NO | Club name. |

### `gold.dim_league`
* **Grain:** One row per competitive league entity.

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `league_key` | `INT IDENTITY(1,1)` | **NO** (PK) | Surrogate key for the league. |
| `league` | `NVARCHAR(35)` | NO | League name (e.g. `'SERIE A'`, `'ENGLISH PREMIER LEAGUE'`). |
| `league_difficulty_coef` | `DECIMAL(3, 2)` | NO | Quality adjustment coefficient (Apex `1.00`, SL1 `0.90`, SL2 `0.70`, SL3 `0.60`, SL4 `0.40`, SL5 `0.20`). In webapp analytics dossiers, translation multipliers are calibrated relative to the user's managed squad standard (`target_coef / managed_coef`). |

---

## 5. Gold Schema: Fact Tables

### `gold.fact_outfield_player_stats`
* **Grain:** One row per player per team per season snapshot.
* **Description:** Comprehensive tactical and physical statistics normalized to per-90 rates.

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `player_key` | `INT` | **NO** (FK) | Reference to `gold.dim_player_info`. |
| `team_key` | `VARCHAR(10)` | **NO** (FK) | Reference to `gold.dim_team_info`. |
| `minutes_played` | `INT` | YES | Total competitive minutes recorded. |
| `team_goals_scored_per90` | `DECIMAL(5, 2)` | YES | Team scoring rate while player was on pitch. |
| `team_goals_conceded_per90` | `DECIMAL(5, 2)` | YES | Team concession rate while player was on pitch. |
| `goals` | `INT` | YES | Total season goals scored. |
| `goals_outside_the_box` | `INT` | YES | Goals scored from beyond the 18-yard box. |
| `shots_per90` | `DECIMAL(5, 2)` | YES | Total shots attempted per 90 minutes. |
| `xGoals_per_shot` | `DECIMAL(5, 2)` | YES | Shot quality metric ($xG/\text{shot}$). |
| `shot_accuracy` | `DECIMAL(5, 2)` | YES | Proportion of shots on target ($[0.00, 1.00]$). |
| `shots_on_target_per90` | `DECIMAL(5, 2)` | YES | Shots hitting target per 90. |
| `goals_per90` | `DECIMAL(5, 2)` | YES | Goals scored per 90. |
| `xGoals_per90` | `DECIMAL(5, 2)` | YES | Expected goals per 90. |
| `non_penalty_xGoals_per90`| `DECIMAL(5, 2)` | YES | Expected goals excluding penalties per 90. |
| `xGoals_overperformance` | `DECIMAL(5, 2)` | YES | Total delta ($\text{Goals} - xG$). |
| `conversion_rate` | `DECIMAL(5, 2)` | YES | Proportion of shots that resulted in goals. |
| `assists` | `INT` | YES | Total season assists. |
| `assists_per90` | `DECIMAL(5, 2)` | YES | Assists per 90 minutes. |
| `passes_attempted_per90` | `DECIMAL(5, 2)` | YES | Total passes attempted per 90. |
| `pass_accuracy` | `DECIMAL(5, 2)` | YES | Pass completion percentage ($[0.00, 1.00]$). |
| `xAssits_per90` | `DECIMAL(5, 2)` | YES | Expected assists per 90 ($xA/90$). |
| `open_play_key_passes_per90`| `DECIMAL(5, 2)` | YES | Key passes executed in open play per 90. |
| `chances_created_per90` | `DECIMAL(5, 2)` | YES | Clear chances created per 90. |
| `dribbles_made_per90` | `DECIMAL(5, 2)` | YES | Successful take-ons per 90. |
| `progressive_passes_per90`| `DECIMAL(5, 2)` | YES | Passes advancing play $\ge 10\text{m}$ towards goal per 90. |
| `open_play_crosses_attempted_per90` | `DECIMAL(5, 2)` | YES | Crosses attempted from open play per 90. |
| `open_play_cross_accuracy`| `DECIMAL(5, 2)` | YES | Open play cross completion rate ($[0.00, 1.00]$). |
| `crosses_attempted_per90` | `DECIMAL(5, 2)` | YES | All crosses attempted per 90. |
| `cross_accuracy` | `DECIMAL(5, 2)` | YES | Overall cross completion rate. |
| `tackles_attempted_per90` | `DECIMAL(5, 2)` | YES | Tackles attempted per 90. |
| `tackle_accuracy` | `DECIMAL(5, 2)` | YES | Tackle success rate ($[0.00, 1.00]$). |
| `pressures_attempted_per90`| `DECIMAL(5, 2)` | YES | Defensive pressing actions attempted per 90. |
| `pressures_completed_per90`| `DECIMAL(5, 2)` | YES | Successful defensive pressing actions per 90. |
| `possession_won_per90` | `DECIMAL(5, 2)` | YES | Total ball recoveries per 90. |
| `possession_lost_per90` | `DECIMAL(5, 2)` | YES | Turnovers and dispossessions per 90. |
| `key_tackles_per90` | `DECIMAL(5, 2)` | YES | Last-man or goal-preventing tackles per 90. |
| `interceptions_per90` | `DECIMAL(5, 2)` | YES | Intercepted passes per 90. |
| `clearances_per90` | `DECIMAL(5, 2)` | YES | Clearances per 90. |
| `blocks_per90` | `DECIMAL(5, 2)` | YES | Blocked passes and crosses per 90. |
| `shots_blocked_per90` | `DECIMAL(5, 2)` | YES | Blocked goal-bound shots per 90. |
| `headers_attempted_per90` | `DECIMAL(5, 2)` | YES | Aerial duels contested per 90. |
| `heading_accuracy` | `DECIMAL(5, 2)` | YES | Aerial duel win rate ($[0.00, 1.00]$). |
| `key_headers_per90` | `DECIMAL(5, 2)` | YES | Crucial aerial clearances won per 90. |
| `sprints_per90` | `DECIMAL(5, 2)` | YES | High-intensity sprint actions per 90. |
| `distance_covered_km_per90`| `DECIMAL(5, 2)` | YES | Kilometers traversed per 90 minutes. |
| `mistakes_leading_to_goals_per90` | `DECIMAL(5, 2)` | YES | Errors leading directly to opposition goals. |
| `fouls_made_per90` | `DECIMAL(5, 2)` | YES | Infractions conceded per 90. |
| `fouls_against_per90` | `DECIMAL(5, 2)` | YES | Free kicks won per 90. |
| `yellow_cards_per90` | `DECIMAL(5, 2)` | YES | Cautions received per 90. |
| `red_cards_per90` | `DECIMAL(5, 2)` | YES | Dismissals per 90. |
| `season` | `VARCHAR(9)` | NO | Competitive season snapshot. |

### `gold.fact_gk_stats`
* **Grain:** One row per goalkeeper per team per season snapshot.

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `player_key` | `INT` | **NO** (FK) | Reference to `gold.dim_player_info`. |
| `team_key` | `VARCHAR(10)` | **NO** (FK) | Reference to `gold.dim_team_info`. |
| `minutes_played` | `INT` | YES | Minutes played in goal. |
| `goals_conceded_per90` | `DECIMAL(5, 2)` | YES | Goals allowed per 90. |
| `saves_made_per90` | `DECIMAL(5, 2)` | YES | Total saves per 90. |
| `xGoals_prevented_per90` | `DECIMAL(5, 2)` | YES | Net goals saved above expectation ($\text{xGOT} - \text{Conceded}$). |
| `xSave_rate` | `DECIMAL(5, 2)` | YES | Expected save percentage based on shot quality. |
| `saves_tipped_per90` | `DECIMAL(5, 2)` | YES | Saves deflected over or wide per 90. |
| `saves_parried_per90` | `DECIMAL(5, 2)` | YES | Saves parried back into active play per 90. |
| `saves_held_per90` | `DECIMAL(5, 2)` | YES | Saves caught and retained cleanly per 90. |
| `saves_percentage` | `DECIMAL(5, 2)` | YES | Actual save percentage ($[0.00, 1.00]$). |
| `passes_attempted_per90` | `DECIMAL(5, 2)` | YES | Distribution passes attempted per 90. |
| `pass_accuracy` | `DECIMAL(5, 2)` | YES | Distribution pass accuracy ($[0.00, 1.00]$). |
| `interceptions_per90` | `DECIMAL(5, 2)` | YES | Sweeper keeper cutouts and claims outside 6-yard box. |
| `clearances_per90` | `DECIMAL(5, 2)` | YES | Sweeper clearances under pressure. |
| `penalties_faced_per90` | `DECIMAL(5, 2)` | YES | Penalties faced per 90. |
| `penalties_save_percentage`| `DECIMAL(5, 2)`| YES | Spot kick save rate. |
| `season` | `VARCHAR(9)` | NO | Competitive season snapshot. |

### `gold.fact_players_value`
* **Grain:** One row per player per team per season snapshot.

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `player_key` | `INT` | **NO** (FK) | Reference to `gold.dim_player_info`. |
| `team_key` | `VARCHAR(10)` | YES | Reference to `gold.dim_team_info`. |
| `wage_per_week_k` | `DECIMAL(10, 2)` | YES | Weekly contractual wage in thousands (£k). |
| `transfer_value_m` | `DECIMAL(10, 2)` | YES | Estimated transfer market valuation in millions (£M). |
| `season` | `VARCHAR(9)` | NO | Competitive season snapshot. |

---

## 6. Audit & Observability Tables

### `audit.etl_pipeline_log`
* **Grain:** One row per ETL pipeline execution step.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `log_id` | `INT IDENTITY(1,1)` (PK) | Auto-incrementing primary key. |
| `batch_id` | `UNIQUEIDENTIFIER` | GUID linking all steps in a single execution run. |
| `pipeline_layer` | `VARCHAR(20)` | ETL Layer (`'Bronze'`, `'Silver'`, `'Gold'`, `'Complete'`). |
| `step_name` | `VARCHAR(100)` | Name of procedure or operation executed. |
| `status` | `VARCHAR(20)` | Execution state (`'Started'`, `'Success'`, `'Failed'`). |
| `rows_affected` | `INT` | Number of records modified/inserted. |
| `start_time` | `DATETIME2` | Step start timestamp. |
| `end_time` | `DATETIME2` | Step completion timestamp. |
| `duration_ms` | `INT` | Duration in milliseconds. |
| `error_message` | `NVARCHAR(4000)` | Detailed SQL engine error if `status = 'Failed'`. |

---

## 7. Analytical Views & Derived Metrics Catalog

The Gold schema exposes 24 analytical views and dimensional marts categorized into five functional domains:
1. **Unified Scouting Marts** (Master player and goalkeeper marts with derived Moneyball indices)
2. **Positional Squad Performance Reports** (Detailed evaluations of user squad members by tactical role)
3. **Positional Replacement Similarity Engines** (Scale-weighted Euclidean distance matchers against starting benchmarks)
4. **Multi-Season Historical & Growth Tracking** (SCD Type 2 longitudinal player and goalkeeper career trajectories)
5. **Advanced Recruitment & Strategy Intelligence** (Squad succession planning, value-efficiency bargain scanners, and tactical archetypes)

---

### Executive Views Index

| View Name | Schema | Category | Grain | Primary Purpose |
| :--- | :--- | :--- | :--- | :--- |
| [`mart_outfield_scouting_summary`](#711-goldmart_outfield_scouting_summary) | `gold` | Unified Mart | 1 row per current outfield player | Master outfield mart with financial and tactical derived metrics. |
| [`mart_gk_scouting_summary`](#712-goldmart_gk_scouting_summary) | `gold` | Unified Mart | 1 row per current goalkeeper | Master goalkeeper mart with shot-stopping delta, hold rate %, and sweep index. |
| [`vw_transfer_targets_outfield`](#713-goldvw_transfer_targets_outfield) | `gold` | Unified Mart | 1 row per external target ($\ge 900\text{m}$) | Scouting target pool enriched with position-partitioned percentiles. |
| [`vw_squad_central_defender_detailed_reports`](#721-goldvw_squad_central_defender_detailed_reports) | `gold` | Squad Report | 1 row per squad CD ($\ge 1000\text{m}$) | Deep tactical report with weighted defensive actions ($W_{\text{def}}$) and key passes per progression. |
| [`vw_squad_central_defender_reports`](#722-goldvw_squad_central_defender_reports) | `gold` | Squad Report | 1 row per squad CD ($\ge 1000\text{m}$) | Standard squad central defender performance report. |
| [`vw_squad_flank_defender_reports`](#723-goldvw_squad_flank_defender_reports) | `gold` | Squad Report | 1 row per squad FB/WB ($\ge 1000\text{m}$) | Fullback/wingback report tracking crossing, progression, and sprint volume. |
| [`vw_squad_midfielder_reports`](#724-goldvw_squad_midfielder_reports) | `gold` | Squad Report | 1 row per squad CM ($\ge 1000\text{m}$) | Central midfielder report tracking tempo dictation, progression, and duel rates. |
| [`vw_squad_attacking_midfielder_reports`](#725-goldvw_squad_attacking_midfielder_reports) | `gold` | Squad Report | 1 row per squad CAM/Winger ($\ge 1000\text{m}$) | Attacking midfielder report tracking chance quality ($xA/\text{OP-KP}$), take-ons, and goal threat. |
| [`vw_squad_striker_reports`](#726-goldvw_squad_striker_reports) | `gold` | Squad Report | 1 row per squad ST ($\ge 1000\text{m}$) | Striker performance report tracking shrunk finishing, shot quality, and conversion. |
| [`vw_similar_strikers`](#731-goldvw_similar_strikers) | `gold` | Similarity Engine | 1 row per external ST ($\ge 900\text{m}$) | Matches external forwards against the user squad's primary starting striker. |
| [`vw_similar_central_defenders`](#732-goldvw_similar_central_defenders) | `gold` | Similarity Engine | 1 row per external CD ($\ge 900\text{m}$) | Matches external centerbacks against the user squad's starting central defender. |
| [`vw_similar_flank_defenders`](#733-goldvw_similar_flank_defenders) | `gold` | Similarity Engine | 1 row per external FB/WB ($\ge 900\text{m}$) | Matches external fullbacks against the starting flank defender. |
| [`vw_similar_defensive_midfielders`](#734-goldvw_similar_defensive_midfielders) | `gold` | Similarity Engine | 1 row per external DM/CM ($\ge 900\text{m}$) | Matches ball-winners and defensive midfielders against the starting DM. |
| [`vw_similar_central_midfielders`](#735-goldvw_similar_central_midfielders) | `gold` | Similarity Engine | 1 row per external CM/DM ($\ge 900\text{m}$) | Matches box-to-box and playmakers against the starting central midfielder. |
| [`vw_similar_midfielders`](#736-goldvw_similar_midfielders) | `gold` | Similarity Engine | 1 row per external CM/DM ($\ge 900\text{m}$) | Backward-compatible alias for `gold.vw_similar_central_midfielders`. |
| [`vw_similar_wingers`](#737-goldvw_similar_wingers) | `gold` | Similarity Engine | 1 row per external Winger ($\ge 900\text{m}$) | Matches wide attackers against the starting winger. |
| [`vw_similar_attacking_midfielders`](#738-goldvw_similar_attacking_midfielders) | `gold` | Similarity Engine | 1 row per external CAM ($\ge 900\text{m}$) | Matches #10 creators against the starting attacking midfielder. |
| [`vw_similar_goalkeepers`](#739-goldvw_similar_goalkeepers) | `gold` | Similarity Engine | 1 row per external GK ($\ge 900\text{m}$) | Matches shot-stoppers against the user squad's starting goalkeeper. |
| [`mart_outfield_historical_snapshots`](#741-goldmart_outfield_historical_snapshots) | `gold` | Growth & History | 1 row per outfield snapshot version | Chronological record of all player seasons across SCD Type 2 history. |
| [`vw_player_growth_tracking`](#742-goldvw_player_growth_tracking) | `gold` | Growth & History | 1 row per season transition | Computes exact season-over-season deltas ($\Delta$) in tactical output and market valuation. |
| [`vw_gk_growth_tracking`](#743-goldvw_gk_growth_tracking) | `gold` | Growth & History | 1 row per GK season transition | Computes season-over-season deltas ($\Delta$) in shot-stopping, hold rate %, and sweep actions. |
| [`vw_squad_succession_plan`](#751-goldvw_squad_succession_plan) | `gold` | Strategic Intel | 1 row per veteran-successor pair | Identifies top 3 U23 external replacements for aging squad starters ($\text{Age} \ge 28$). |
| [`vw_bargain_watch`](#752-goldvw_bargain_watch) | `gold` | Strategic Intel | 1 row per bargain target | Discovers elite contributors with undervalued contracts via Moneyball efficiency indices. |
| [`vw_tactical_archetypes`](#753-goldvw_tactical_archetypes) | `gold` | Strategic Intel | 1 row per player ($\ge 450\text{m}$) | Rule-based tactical classifier mapping players to 21 Football Manager archetypes. |

---

### 7.1. Unified Scouting Marts

#### 7.1.1. `gold.mart_outfield_scouting_summary`
* **Grain:** One row per active outfield player snapshot (`p.dwh_current_validity = 1` AND `s.dwh_current_validity = 1`).
* **Source Tables:** `gold.fact_outfield_player_stats` (s), `gold.dim_player_info` (p), `gold.dim_team_info` (t), `gold.dim_league` (l), `gold.fact_players_value` (v).
* **Business Purpose:** Central reporting mart for all outfield analysis. Combines demographic metadata, contractual valuations, and standard per-90 rates with derived tactical and financial efficiency metrics.

| Column Name | Data Type | Nullable | Description / Metric Formula |
| :--- | :--- | :--- | :--- |
| `player_key` | `INT` | NO | Surrogate foreign key referencing `gold.dim_player_info`. |
| `player_id` | `INT` | NO | FM natural key. |
| `player_name` | `VARCHAR(35)` | NO | Player full name. |
| `age` | `INT` | YES | Player age. |
| `position` | `VARCHAR(28)` | YES | Standardized primary position. |
| `club_name` | `NVARCHAR(35)` | YES | Club contracted to. |
| `league` | `NVARCHAR(35)` | YES | Competitive domestic league. |
| `league_difficulty_coef` | `DECIMAL(3,2)` | NO | Quality adjustment coefficient from `gold.dim_league` (default `1.00`). |
| `is_own_squad` | `BIT` | NO | `1` = Player in user's club; `0` = Scouted external transfer target. |
| `contracted` | `BIT` | NO | `1` = Professional contract active; `0` = Free agent. |
| `minutes_played` | `INT` | YES | Total competitive minutes in season snapshot. |
| `wage_per_week_k` | `DECIMAL(10,2)` | YES | Contractual weekly wage in thousands (£k). |
| `transfer_value_m` | `DECIMAL(10,2)` | YES | Estimated market transfer valuation in millions (£M). |
| `goals_per90` | `DECIMAL(5,2)` | YES | Goals scored per 90 minutes. |
| `assists_per90` | `DECIMAL(5,2)` | YES | Assists per 90 minutes. |
| `xGoals_per90` | `DECIMAL(5,2)` | YES | Expected goals per 90 minutes ($xG/90$). |
| `non_penalty_xGoals_per90` | `DECIMAL(5,2)` | YES | Non-penalty expected goals per 90 ($npxG/90$). |
| `xAssists_per90` | `DECIMAL(5,2)` | YES | Expected assists per 90 minutes ($xA/90$). (Also aliased as `xAssits_per90` for backward compatibility). |
| **`npxg_plus_xa_per90`** | `DECIMAL(5,2)` | YES | **Formula:** `non_penalty_xGoals_per90 + xAssists_per90`. Direct non-penalty goal involvement threat per 90 ($npxG+xA/90$). |
| **`finishing_overperf_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `(goals_per90 - xGoals_per90) * (minutes / (minutes + 900.0))`. Empirical Bayes shrinkage adjusting per-90 finishing run toward zero based on minute sample size. |
| **`goals_minus_xg_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `(goals_per90 - xGoals_per90) * (minutes / (minutes + 900.0))`. Shrunk goal conversion delta above expectation. |
| **`assist_luck`** | `DECIMAL(7,4)` | YES | **Formula:** `(assists_per90 - xAssists_per90) * (minutes / (minutes + 900.0))`. Shrunk teammate finishing delta adjusting for small sample size. |
| **`assist_luck_shrunk`** | `DECIMAL(7,4)` | YES | Alias of `assist_luck`. |
| **`goal_threat_share`** | `DECIMAL(7,4)` | YES | **Formula:** `npxG_per90 / NULLIF(npxG_per90 + xA_per90, 0)`. Proportion of total attacking threat coming from shooting vs passing ($[0.00, 1.00]$). |
| `shots_per90` | `DECIMAL(5,2)` | YES | Total shots attempted per 90. |
| **`shot_on_target_rate`** | `DECIMAL(7,4)` | YES | **Formula:** `shots_on_target_per90 / NULLIF(shots_per90, 0)`. Proportion of shot attempts on target. |
| `xGoals_per_shot` | `DECIMAL(5,2)` | YES | Average shot quality ($xG / \text{shot}$). |
| **`npxg_per_shot`** | `DECIMAL(7,4)` | YES | **Formula:** `non_penalty_xGoals_per90 / NULLIF(shots_per90, 0)`. Penalty-free average shot quality. |
| **`box_presence_proxy`** | `DECIMAL(7,4)` | YES | **Formula:** `shots_per90 * xGoals_per_shot`. Consistency proxy for penalty-box penetration. |
| **`shots_outside_box_ratio`** | `DECIMAL(7,4)` | YES | **Formula:** `shots_outside_box_per90 / NULLIF(shots_per90, 0)`. Proportion of shots taken from distance. |
| **`long_range_goal_share`** | `DECIMAL(7,4)` | YES | **Formula:** `goals_outside_the_box / NULLIF(goals, 0)`. Proportion of goals scored from outside the penalty area. |
| **`chance_creation_per_pass_pct`** | `DECIMAL(7,4)` | YES | **Formula:** `(chances_created_per90 / NULLIF(passes_attempted_per90, 0)) * 100.0`. Chances generated per 100 attempted passes. |
| **`xa_per_op_kp`** | `DECIMAL(7,4)` | YES | **Formula:** `xAssists_per90 / NULLIF(open_play_key_passes_per90, 0)`. Expected danger generated per open-play key pass. |
| **`assists_per_op_kp`** | `DECIMAL(7,4)` | YES | **Formula:** `assists_per90 / NULLIF(open_play_key_passes_per90, 0)`. Actual assist conversion per open-play key pass. |
| **`completed_crosses_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `crosses_attempted_per90 * cross_accuracy`. Accurate crosses delivered per 90 (all phases). |
| **`open_play_completed_crosses_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `open_play_crosses_attempted_per90 * open_play_cross_accuracy`. Accurate open-play crosses per 90. |
| **`crosses_share_of_creation`** | `DECIMAL(7,4)` | YES | **Formula:** `completed_crosses_per90 / NULLIF(chances_created_per90, 0)`. Proportion of chance creation driven by crossing. |
| `open_play_cross_accuracy` | `DECIMAL(5,2)` | YES | Cross completion rate in open play ($[0.00, 1.00]$). |
| `conversion_rate` | `DECIMAL(5,2)` | YES | Goal conversion rate from shots ($[0.00, 1.00]$). |
| `passes_attempted_per90` | `DECIMAL(5,2)` | YES | Total passes attempted per 90. |
| `pass_accuracy` | `DECIMAL(5,2)` | YES | Pass completion percentage ($[0.00, 1.00]$). |
| **`completed_passes_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `passes_attempted_per90 * pass_accuracy`. Accurate passes completed per 90. |
| `progressive_passes_per90` | `DECIMAL(5,2)` | YES | Passes moving the ball $\ge 10\text{m}$ toward goal per 90. |
| **`progressive_actions_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `progressive_passes_per90 + dribbles_made_per90`. Total territory-advancing actions per 90. |
| **`prog_rate_per_pass`** | `DECIMAL(7,4)` | YES | **Formula:** `(progressive_passes_per90 / NULLIF(passes_attempted_per90, 0)) * 100.0`. Percentage of passes that are progressive. |
| **`turnover_rate`** | `DECIMAL(7,4)` | YES | **Formula:** `possession_lost_per90 / NULLIF(passes_attempted_per90 + dribbles_made_per90, 0)`. Ball loss rate per on-ball action. |
| **`progressive_share_of_actions`** | `DECIMAL(7,4)` | YES | **Formula:** `progressive_passes_per90 / NULLIF(passes_attempted_per90 + dribbles_made_per90, 0)`. Share of on-ball touches that advance play. |
| **`net_ball_security`** | `DECIMAL(7,4)` | YES | **Formula:** `completed_passes_per90 - possession_lost_per90`. Net ball retention margin per 90. |
| `dribbles_made_per90` | `DECIMAL(5,2)` | YES | Successful dribbles/take-ons per 90. |
| `open_play_key_passes_per90` | `DECIMAL(5,2)` | YES | Key passes executed in open play per 90. |
| **`defensive_actions_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `(tackles * tackle_acc) + interceptions + blocks + shots_blocked + clearances`. Total volume of successful defensive interventions per 90. |
| **`non_clearance_def_actions_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `(tackles * tackle_acc) + interceptions + blocks + shots_blocked`. Defensive intervention volume excluding clearances (unbiased across CB vs Midfield). |
| **`reactive_vs_positional`** | `DECIMAL(7,4)` | YES | **Formula:** `interceptions / NULLIF(interceptions + (tackles * tackle_acc), 0)`. Ratio of positional reading to active ground tackling. |
| **`duels_won_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `(tackles * tackle_acc) + (headers * heading_acc)`. Total ground and aerial duels won per 90. |
| **`foul_per_duel`** | `DECIMAL(7,4)` | YES | **Formula:** `fouls_made_per90 / NULLIF(tackles_attempted_per90 + headers_attempted_per90, 0)`. Foul frequency per contested contest. |
| **`foul_per_tackle`** | `DECIMAL(7,4)` | YES | **Formula:** `fouls_made_per90 / NULLIF(tackles_attempted_per90, 0)`. Tendency to foul when contesting tackles. |
| **`clearance_per_aerial`** | `DECIMAL(7,4)` | YES | **Formula:** `clearances_per90 / NULLIF(headers_attempted_per90, 0)`. Ratio of clearances relative to aerial duels. |
| **`aerial_duels_won_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `headers_attempted_per90 * heading_accuracy`. Aerial contests won cleanly per 90. |
| `heading_accuracy` | `DECIMAL(5,2)` | YES | Proportion of aerial duels won ($[0.00, 1.00]$). |
| `tackles_attempted_per90` | `DECIMAL(5,2)` | YES | Tackles attempted per 90. |
| `tackle_accuracy` | `DECIMAL(5,2)` | YES | Tackle success rate ($[0.00, 1.00]$). |
| `key_tackles_per90` | `DECIMAL(5,2)` | YES | Goal-preventing or last-man tackles per 90. |
| `interceptions_per90` | `DECIMAL(5,2)` | YES | Intercepted passes per 90. |
| `clearances_per90` | `DECIMAL(5,2)` | YES | Clearances per 90. |
| `blocks_per90` | `DECIMAL(5,2)` | YES | Blocked passes and crosses per 90. |
| `shots_blocked_per90` | `DECIMAL(5,2)` | YES | Opponent shot attempts physically blocked per 90. |
| `pressures_attempted_per90` | `DECIMAL(5,2)` | YES | Pressing actions attempted per 90. |
| `pressures_completed_per90` | `DECIMAL(5,2)` | YES | Pressing actions resulting in a turnover or dispossession within 5 seconds. |
| **`pressure_accuracy`** | `DECIMAL(7,4)` | YES | **Formula:** `(pressures_completed_per90 / NULLIF(pressures_attempted_per90, 0)) * 100.0`. Success percentage of pressing challenges. |
| **`pressure_efficiency`** | `DECIMAL(7,4)` | YES | **Formula:** `pressures_completed_per90 / NULLIF(pressures_attempted_per90, 0)`. Fractional success rate ($[0.00, 1.00]$) of pressing challenges. |
| `sprints_per90` | `DECIMAL(5,2)` | YES | High-intensity sprint actions per 90. |
| `distance_covered_km_per90` | `DECIMAL(5,2)` | YES | Distance run in kilometers per 90 minutes. |
| **`sprints_per_km`** | `DECIMAL(7,4)` | YES | **Formula:** `sprints_per90 / NULLIF(distance_covered_km_per90, 0)`. Running intensity index (sprints per kilometer run). |
| **`high_press_load`** | `DECIMAL(7,4)` | YES | **Formula:** `pressures_attempted_per90 * sprints_per_km`. High-intensity pressing work-rate index. |
| `fouls_made_per90` | `DECIMAL(5,2)` | YES | Free kicks conceded per 90. |
| `fouls_against_per90` | `DECIMAL(5,2)` | YES | Free kicks won per 90. |
| **`fouls_won_net`** | `DECIMAL(7,4)` | YES | **Formula:** `fouls_against_per90 - fouls_made_per90`. Net fouls drawn over conceded. Positive indicates net foul winner. |
| **`discipline_index`** | `DECIMAL(7,4)` | YES | **Formula:** `fouls_made_per90 + (3.0 * yellow_cards_per90) + (8.0 * red_cards_per90)`. Composite disciplinary penalty index. |
| **`cards_per_foul`** | `DECIMAL(7,4)` | YES | **Formula:** `(yellow_cards_per90 + red_cards_per90) / NULLIF(fouls_made_per90, 0)`. Disciplinary severity per committed foul. |
| **`possession_differential`** | `DECIMAL(5,2)` | YES | **Formula:** `possession_won_per90 - possession_lost_per90`. Net ball recoveries minus dispossessions per 90. |
| `mistakes_leading_to_goals_per90` | `DECIMAL(5,2)` | YES | Individual errors directly leading to opponent goals. |
| **`goals_per90_adj`** | `DECIMAL(7,4)` | YES | **Formula:** `goals_per90 * league_difficulty_coef`. League-difficulty normalized goals per 90. |
| **`xGoals_per90_adj`** | `DECIMAL(7,4)` | YES | **Formula:** `xGoals_per90 * league_difficulty_coef`. League-difficulty normalized expected goals per 90. |
| **`non_penalty_xGoals_per90_adj`** | `DECIMAL(7,4)` | YES | **Formula:** `non_penalty_xGoals_per90 * league_difficulty_coef`. League-difficulty normalized non-penalty xG per 90. |
| **`xAssists_per90_adj`** | `DECIMAL(7,4)` | YES | **Formula:** `xAssists_per90 * league_difficulty_coef`. League-difficulty normalized expected assists per 90. |
| **`npxg_plus_xa_per90_adj`** | `DECIMAL(7,4)` | YES | **Formula:** `npxg_plus_xa_per90 * league_difficulty_coef`. League-difficulty normalized goal involvement threat per 90. |
| **`cost_per_xgi`** | `DECIMAL(10,2)` | YES | **Formula:** `((wage * 52) + (fee / 3)) / NULLIF(npxGI * (minutes / 90.0), 0)`. Annualized financial cost (£k) per annual expected goal involvement. |
| **`xgi_per_cost_m`** | `DECIMAL(7,4)` | YES | **Formula:** `(npxGI * (minutes / 90.0)) / NULLIF((wage * 0.052) + (fee / 3.0), 0)`. Annual expected goal contribution output per £1M annualized expenditure. |
| **`value_gap`** | `DECIMAL(5,1)` | YES | **Formula:** `PERCENT_RANK(npxGI) - PERCENT_RANK(transfer_value_m)`. Market undervaluation index ($[-100.0, +100.0]$). High positive values indicate elite performers available at discount fees. |
| **`on_pitch_goal_diff_per90`** | `DECIMAL(5,2)` | YES | **Formula:** `team_goals_scored_per90 - team_goals_conceded_per90`. Net team goal difference while player was on the pitch. |
| **`on_pitch_gd_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `(team_goals_scored - team_goals_conceded) * (minutes / (minutes + 900.0))`. Sample-size shrunk on-pitch goal differential. |

---

#### 7.1.2. `gold.mart_gk_scouting_summary`
* **Grain:** One row per active goalkeeper snapshot (`p.dwh_current_validity = 1` AND `s.dwh_current_validity = 1`).
* **Source Tables:** `gold.fact_gk_stats` (s), `gold.dim_player_info` (p), `gold.dim_team_info` (t), `gold.dim_league` (l), `gold.fact_players_value` (v).
* **Business Purpose:** Central reporting mart for goalkeeper recruitment and performance appraisal. Focuses on workload-normalized shot-stopping shrunk by shots faced ($k=300$), handling risk (parry rate vs hold rate over expected), proactive sweeper-keeper actions, and buildup distribution security. Ratios and deltas are rendered in high-resolution `DECIMAL(7,4)` precision.

| Column Name | Data Type | Nullable | Description / Metric Formula |
| :--- | :--- | :--- | :--- |
| `player_key`, `player_id`, `player_name`, `age`, `position` | Various | NO/YES | Identifiers and player attributes matching outfield mart. |
| `club_name`, `league`, `league_difficulty_coef` | Various | YES/NO | Club, league, and league coefficient. |
| `is_own_squad`, `contracted`, `minutes_played` | Various | NO/YES | Lineup status, contract status, and season minutes. |
| `wage_per_week_k`, `transfer_value_m` | `DECIMAL(10,2)` | YES | Financial contract wage (£k/wk) and valuation (£M). |
| **`shots_faced_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `saves_made_per90 + goals_conceded_per90`. Total shots on target faced per 90. |
| **`shots_faced_total`** | `DECIMAL(7,2)` | YES | **Formula:** `shots_faced_per90 * (minutes_played / 90.0)`. Cumulative sample size of shots on target faced across the season (foundation for shrinkage). |
| **`xgot_faced_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `xGoals_prevented_per90 + goals_conceded_per90`. Post-shot expected goals faced on target per 90. |
| **`shot_difficulty`** | `DECIMAL(7,4)` | YES | **Formula:** `xgot_faced_per90 / NULLIF(shots_faced_per90, 0)`. Average xGOT per shot faced ($[0.00, 1.00]$). Isolates defensive concession quality (higher means keeper faces harder shots). |
| **`xgp_per_shot`** | `DECIMAL(7,4)` | YES | **Formula:** `xGoals_prevented_per90 / NULLIF(shots_faced_per90, 0)`. Primary shot-stopping efficiency metric; isolates shot-prevention rate independent of defense workload. |
| **`xgp_per_shot_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `(shots_faced_total * xgp_per_shot + 300.0 * 0.0) / (shots_faced_total + 300.0)`. Empirical Bayes shrinkage based on shots faced ($k=300$). |
| **`goals_vs_xgot_ratio`** | `DECIMAL(7,4)` | YES | **Formula:** `goals_conceded_per90 / NULLIF(xgot_faced_per90, 0)`. Ratio of actual goals allowed to xGOT faced (< 1.0 indicates positive shot stopping). |
| **`xgp_per_season`** | `DECIMAL(7,4)` | YES | **Formula:** `xGoals_prevented_per90 * 38.0`. Projected net goals saved above expectation over a full 38-game league campaign. |
| **`xgp_per_season_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `sample_reliability * xGoals_prevented_per90 * 38.0`. Sample-reliability regressed seasonal goals prevented. |
| **`shot_stopping_delta`** | `DECIMAL(7,4)` | YES | **Formula:** `saves_percentage - xSave_rate`. Net save rate over expected. |
| **`shot_stopping_delta_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `(shots_faced_total * shot_stopping_delta) / (shots_faced_total + 300.0)`. Delta shrunk heavily for sample exposure. |
| `xGoals_prevented_per90` | `DECIMAL(5,2)` | YES | Goals saved above expectation per 90 ($\text{xGOT} - \text{Goals Conceded}$). |
| `saves_made_per90`, `saves_percentage`, `xSave_rate`, `goals_conceded_per90` | Various | YES | Volume and raw efficiency descriptors. |
| **`parry_rate`** | `DECIMAL(7,4)` | YES | **Formula:** `saves_parried_per90 / NULLIF(saves_made_per90, 0)`. Proportion of saves parried into play (rebound vulnerability risk metric). |
| **`tip_rate`** | `DECIMAL(7,4)` | YES | **Formula:** `saves_tipped_per90 / NULLIF(saves_made_per90, 0)`. Proportion of saves deflected over bar / around post (safe neutral outcome). |
| **`hold_rate`** | `DECIMAL(7,4)` | YES | **Formula:** `saves_held_per90 / NULLIF(saves_made_per90, 0)`. Raw proportion of saves caught cleanly. |
| **`hold_rate_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `(saves_total * hold_rate + 150.0 * 0.30) / (saves_total + 150.0)`. Hold rate regressed toward pool mean ($k=150$). |
| **`hold_rate_over_expected`** | `DECIMAL(7,4)` | YES | **Formula:** `hold_rate - (0.1143 + 0.5230 * shot_difficulty)`. Residual handling skill adjusted for incoming shot difficulty. |
| `hold_rate_pct` | `DECIMAL(7,4)` | YES | Backward-compatible hold rate percentage. |
| `saves_tipped_per90`, `saves_parried_per90`, `saves_held_per90` | `DECIMAL(5,2)` | YES | Volume breakdowns of save handling per 90. |
| **`sweeper_actions_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `interceptions_per90 + clearances_per90`. Total proactive and reactive actions outside the goal line per 90. |
| **`sweeper_share`** | `DECIMAL(7,4)` | YES | **Formula:** `interceptions_per90 / NULLIF(interceptions_per90 + clearances_per90, 0)`. Share of proactive claims/interceptions vs reactive clearances. |
| `sweeper_index` | `DECIMAL(7,4)` | YES | Backward-compatible alias for `sweeper_actions_per90`. |
| `interceptions_per90` | `DECIMAL(5,2)` | YES | In Football Manager: Through-balls, crosses, and passes intercepted outside the 6-yard box per 90. |
| `clearances_per90` | `DECIMAL(5,2)` | YES | In Football Manager: Kicking or punching clearances under opposition contest per 90. |
| **`completed_passes_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `passes_attempted_per90 * pass_accuracy`. Volume of accurate passes completed per 90. |
| **`giveaways_per90`** | `DECIMAL(7,4)` | YES | **Formula:** `passes_attempted_per90 * (1.0 - pass_accuracy)`. Incomplete passes / lost possessions per 90. |
| `passing_involvement` | `DECIMAL(5,2)` | YES | `passes_attempted_per90`. Volume descriptor of goalkeeper involvement in buildup. |
| `distribution_value` | `DECIMAL(7,4)` | YES | Backward-compatible alias for `completed_passes_per90`. |
| `pass_accuracy` | `DECIMAL(5,2)` | YES | Distribution pass completion rate ($[0.00, 1.00]$). |
| `penalties_faced_per90`, `penalties_save_percentage` | Various | YES | Raw penalty volumes and save percentage. |
| **`pen_save_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `(pens_faced * pen_save_pct + 20.0 * 0.20) / (pens_faced + 20.0)`. Heavily shrunk penalty stop rate ($k=20$, prior $0.20$). |
| **`errors_leading_to_goal_per90`**| `DECIMAL(7,4)` | YES | Unforced mistakes resulting directly in conceded goals per 90. |
| **`errors_leading_to_goal_shrunk`** | `DECIMAL(7,4)` | YES | **Formula:** `(minutes * errors_per90 + 5000.0 * 0.022) / (minutes + 5000.0)`. Regressed error rate ($k=5000$ mins). |
| `error_rate` | `DECIMAL(7,4)` | YES | Backward-compatible alias for `errors_leading_to_goal_per90`. |
| `possession_differential` | `DECIMAL(7,4)` | YES | `possession_won_per90 - possession_lost_per90`. |
| **`is_eligible`** | `BIT` | NO | Flags goalkeepers with reliable sample size (`shots_faced_total >= 40.0`). |
| **`sample_reliability`** | `DECIMAL(7,4)` | YES | **Formula:** `shots_faced_total / (shots_faced_total + 300.0)`. Bayesian confidence factor ($[0.00, 1.00]$). |
| **`value_gap`** | `DECIMAL(5,1)` | YES | **Formula:** `PERCENT_RANK(xgp_per_shot_shrunk) - PERCENT_RANK(transfer_value_m)`. Goalkeeper Moneyball valuation efficiency index ($[-100.0, +100.0]$). |

---

#### 7.1.3. `gold.vw_transfer_targets_outfield`
* **Grain:** One row per external target outfield player with $\ge 900$ minutes (`is_own_squad = 0 AND minutes_played >= 900`).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Business Purpose:** High-priority recruitment pool. Eliminates low-sample noise and enriches key performance metrics with positional percentiles (`PERCENT_RANK()`) across three core tactical pillars: attacking threat, passing progression, and defensive action volume.

| Column Name | Data Type | Description / Metric Formula |
| :--- | :--- | :--- |
| `player_key`, `player_name`, `age`, `position`, `club_name`, `league` | Various | Demographics and club information. |
| `wage_per_week_k`, `transfer_value_m`, `minutes_played` | Various | Contractual financials and minutes played. |
| `npxg_plus_xa_per90` | `DECIMAL(5,2)` | Non-penalty expected goal involvement per 90. |
| **`pct_npxg_xa`** | `FLOAT` | **Formula:** `PERCENT_RANK() OVER (PARTITION BY position ORDER BY npxg_plus_xa_per90)`. Position-relative attacking percentile ($[0.00, 1.00]$). |
| `assist_luck`, `finishing_overperf_shrunk`, `goal_threat_share`, `xa_per_op_kp`, `completed_crosses_per90` | Various | Attacking efficiency indicators. |
| `progressive_passes_per90`, `prog_rate_per_pass` | `DECIMAL(5,2)` | Passing progression volume and progression rate per pass. |
| **`pct_prog_rate`** | `FLOAT` | **Formula:** `PERCENT_RANK() OVER (PARTITION BY position ORDER BY prog_rate_per_pass)`. Position-relative ball progression percentile ($[0.00, 1.00]$). |
| `turnover_rate` | `DECIMAL(5,2)` | Ball loss percentage per on-ball action. |
| `defensive_actions_per90` | `DECIMAL(5,2)` | Total successful defensive actions per 90. |
| **`pct_def_actions`** | `FLOAT` | **Formula:** `PERCENT_RANK() OVER (PARTITION BY position ORDER BY defensive_actions_per90)`. Position-relative defensive activity percentile ($[0.00, 1.00]$). |
| `reactive_vs_positional`, `pressure_efficiency`, `aerial_duels_won_per90`, `heading_accuracy`, `discipline_index` | Various | Defensive and duel performance indicators. |
| `cost_per_xgi`, `league_difficulty_coef` | Various | Moneyball cost efficiency and competition adjustment. |

---

### 7.2. Positional Squad Performance Reports

These views evaluate user squad members (or players under active contract) with $\ge 1,000$ minutes to establish internal positional baselines and identify squad weaknesses.

#### 7.2.1. `gold.vw_squad_central_defender_detailed_reports`
* **Grain:** One row per contracted central defender with $\ge 1,000$ minutes (`position = 'Central Defender' AND contracted = 1 AND minutes_played >= 1000`).
* **Source Tables:** `gold.fact_outfield_player_stats`, `gold.dim_player_info`.
* **Business Purpose:** TDD-validated detailed analytical report for central defenders, featuring key pass progression ratios and weighted defensive actions.

| Column Name | Data Type | Description / Metric Formula |
| :--- | :--- | :--- |
| `player_key`, `age`, `minutes_played` | `INT` | Player key, age, and sample minutes. |
| **`tG_differential_per90`** | `DECIMAL(5,2)` | **Formula:** `team_goals_scored_per90 - team_goals_conceded_per90`. On-pitch goal difference per 90. |
| **`kP_per_ProgP`** | `DECIMAL(5,2)` | **Formula:** `open_play_key_passes_per90 / NULLIF(progressive_passes_per90, 0)`. Proportion of progressive passes that directly create shooting opportunities. |
| **`ProgRate_per_Pass`** | `DECIMAL(5,2)` | **Formula:** `(progressive_passes_per90 / NULLIF(passes_attempted_per90, 0)) * 100.0`. Percentage of passes that break lines. |
| `tackles_attempted_per90`, `tackle_accuracy`, `key_tackles_per90` | Various | Ground tackling volume and accuracy. |
| `pressures_attempted_per90` | `DECIMAL(5,2)` | Pressing volume per 90. |
| **`pressure_accuracy`** | `DECIMAL(5,2)` | **Formula:** `(pressures_completed_per90 / NULLIF(pressures_attempted_per90, 0)) * 100.0`. Press completion percentage. |
| `possession_differential` | `DECIMAL(5,2)` | Net ball recovery differential. |
| **`Wdefensive_actions`** | `DECIMAL(5,2)` | **Formula:** `(interceptions * 0.50) + (clearances * 0.25) + ((blocks + shots_blocked) * 0.25)`. Weighted defensive impact score prioritizing anticipatory interceptions over reactive clearances. |
| `headers_attempted_per90`, `heading_accuracy` | Various | Aerial duel volume and win rate. |

#### 7.2.2. `gold.vw_squad_central_defender_reports`
* **Grain:** One row per squad central defender with $\ge 1,000$ minutes (`(is_own_squad = 1 OR contracted = 1) AND position = 'Central Defender' AND minutes_played >= 1000`).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Key Columns & Metrics:** `player_name`, `age`, `minutes_played`, `tG_differential_per90`, `prog_rate_per_pass`, `tackles_attempted_per90`, `tackle_accuracy`, `key_tackles_per90`, `defensive_actions_per90`, `reactive_vs_positional`, `pressure_efficiency`, `pressure_accuracy`, `possession_differential`, `aerial_duels_won_per90`, `heading_accuracy`, `clearance_per_aerial`, `foul_per_tackle`, `discipline_index`.

#### 7.2.3. `gold.vw_squad_flank_defender_reports`
* **Grain:** One row per squad fullback/wingback with $\ge 1,000$ minutes (`position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender')`).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Key Derived Metrics:**
  * **`GA_per90`:** `goals_per90 + assists_per90` (Direct goal involvement per 90).
  * **`xGA_per90`:** `xGoals_per90 + xAssits_per90` (Expected goal involvement per 90).
  * `completed_crosses_per90`, `open_play_cross_accuracy`, `prog_rate_per_pass`, `turnover_rate`, `defensive_actions_per90`, `sprints_per90`.

#### 7.2.4. `gold.vw_squad_midfielder_reports`
* **Grain:** One row per squad central midfielder with $\ge 1,000$ minutes (`position = 'Central Midfielder'`).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Key Derived Metrics:** `tG_differential_per90`, `npxg_plus_xa_per90`, `assist_luck`, `passes_attempted_per90`, `pass_accuracy`, `progressive_passes_per90`, `prog_rate_per_pass`, `turnover_rate`, `chance_creation_per_pass_pct`, `xa_per_op_kp`, `defensive_actions_per90`, `possession_differential`, `sprints_per90`.

#### 7.2.5. `gold.vw_squad_attacking_midfielder_reports`
* **Grain:** One row per squad attacking midfielder or winger with $\ge 1,000$ minutes (`position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Central Attacking Midfielder')`).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Key Derived Metrics:** `GA_per90`, `xGA_per90`, `npxg_plus_xa_per90`, `assist_luck`, `goal_threat_share`, `open_play_key_passes_per90`, `xa_per_op_kp`, `prog_rate_per_pass`, `dribbles_made_per90`, `completed_crosses_per90`, `open_play_cross_accuracy`, `pressure_efficiency`, `sprints_per90`.

#### 7.2.6. `gold.vw_squad_striker_reports`
* **Grain:** One row per squad striker with $\ge 1,000$ minutes (`position = 'Striker'`).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Key Derived Metrics:**
  * **`finishing_delta`:** `goals_per90 - xGoals_per90` (Observed finishing variance per 90).
  * `team_goal_diff_per90`, `goals_per90`, `assists_per90`, `GA_per90`, `xGoals_per90`, `non_penalty_xGoals_per90`, `npxg_plus_xa_per90`, `goal_threat_share`, `finishing_overperf_shrunk`, `conversion_rate`, `shot_on_target_rate`, `xGoals_per_shot`, `shots_outside_box_ratio`, `aerial_duels_won_per90`, `heading_accuracy`, `pressure_efficiency`.

---

### 7.3. Positional Replacement Similarity Engines

Each similarity engine executes a **scale-weighted Euclidean profile comparison** matching external transfer targets (`is_own_squad = 0`, `minutes_played >= 900`) against the user club's incumbent starting player (the top-minute player in that position with `is_own_squad = 1`).

All metric comparisons are protected using `COALESCE` with neutral tactical baselines to guarantee complete null-safety:

$$D(\mathbf{Target}, \mathbf{Benchmark}) = \sqrt{\sum_{i=1}^{k} \left(\frac{\text{COALESCE}(\text{tgt}_i, \text{base}_i) - \text{COALESCE}(\text{bmk}_i, \text{base}_i)}{s_i}\right)^2 \times w_i^2}$$

#### 7.3.1. `gold.vw_similar_strikers`
* **Benchmark Target:** User club's Striker with most minutes.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.position = 'Striker' AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `non_penalty_xGoals_per90`: Weight $3.0$, Baseline $0.0$
  * `shot_on_target_rate`: Weight $5.0$, Baseline $0.40$
  * `aerial_duels_won_per90`: Weight $1.5$, Baseline $0.0$
  * `heading_accuracy`: Weight $4.0$, Baseline $0.50$
  * `pressure_efficiency`: Weight $3.0$, Baseline $0.25$
  * `xGoals_per_shot`: Weight $10.0$, Baseline $0.12$
  * `goal_threat_share`: Weight $3.0$, Baseline $0.65$
* **Output Columns:** `similarity_rank`, `similarity_distance` (`DECIMAL(5,2)`), player demographics, financials, and individual tactical metrics.

#### 7.3.2. `gold.vw_similar_central_defenders`
* **Benchmark Target:** User club's Central Defender with most minutes.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.position = 'Central Defender' AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `aerial_duels_won_per90` ($\times 1.5$), `heading_accuracy` ($\times 5.0$, base $0.65$)
  * `prog_rate_per_pass` ($/ 5.0$), `progressive_passes_per90` ($\times 1.0$)
  * `defensive_actions_per90` ($/ 2.0$), `tackle_accuracy` ($\times 5.0$, base $0.75$)
  * `reactive_vs_positional` ($\times 5.0$, base $0.50$), `discipline_index` ($/ 3.0$)

#### 7.3.3. `gold.vw_similar_flank_defenders`
* **Benchmark Target:** User club's Fullback/Wingback with most minutes.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.position IN ('Right Wingback', 'Left Wingback', 'Right Defender', 'Left Defender') AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `completed_crosses_per90` ($\times 2.0$), `open_play_cross_accuracy` ($\times 5.0$, base $0.15$)
  * `prog_rate_per_pass` ($/ 5.0$), `turnover_rate` ($\times 5.0$, base $0.15$)
  * `defensive_actions_per90` ($/ 2.0$), `tackle_accuracy` ($\times 5.0$, base $0.75$), `sprints_per90` ($/ 5.0$)

#### 7.3.4. `gold.vw_similar_defensive_midfielders`
* **Benchmark Target:** User club's top DM or CM.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.position IN ('Defensive Midfielder', 'Central Midfielder') AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `defensive_actions_per90` ($/ 2.0$), `tackle_accuracy` ($\times 5.0$, base $0.75$)
  * `reactive_vs_positional` ($\times 5.0$, base $0.50$), `aerial_duels_won_per90` ($\times 1.5$)
  * `progressive_passes_per90` ($\times 1.0$), `passes_attempted_per90` ($/ 10.0$), `pass_accuracy` ($\times 5.0$, base $0.85$)
  * `turnover_rate` ($\times 5.0$, base $0.15$), `pressure_efficiency` ($\times 3.0$, base $0.25$), `discipline_index` ($/ 3.0$)

#### 7.3.5. `gold.vw_similar_central_midfielders`
* **Benchmark Target:** User club's top Central Midfielder.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.position IN ('Central Midfielder', 'Defensive Midfielder') AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `progressive_passes_per90` ($\times 1.0$), `prog_rate_per_pass` ($/ 5.0$)
  * `passes_attempted_per90` ($/ 10.0$), `pass_accuracy` ($\times 5.0$, base $0.85$)
  * `turnover_rate` ($\times 5.0$, base $0.15$), `npxg_plus_xa_per90` ($\times 3.0$)
  * `defensive_actions_per90` ($/ 2.0$), `pressure_efficiency` ($\times 3.0$, base $0.25$)

#### 7.3.6. `gold.vw_similar_midfielders`
* Backward-compatible alias view querying `gold.vw_similar_central_midfielders`.

#### 7.3.7. `gold.vw_similar_wingers`
* **Benchmark Target:** User club's top Winger or Wide Midfielder.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.position IN ('Right Attacking Midfielder', 'Left Attacking Midfielder', 'Right Midfielder', 'Left Midfielder') AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `npxg_plus_xa_per90` ($\times 3.0$), `dribbles_made_per90` ($\times 1.0$)
  * `completed_crosses_per90` ($\times 2.0$), `open_play_cross_accuracy` ($\times 5.0$, base $0.15$)
  * `xa_per_op_kp` ($\times 5.0$, base $0.20$), `sprints_per90` ($/ 5.0$), `prog_rate_per_pass` ($/ 5.0$)
  * `goal_threat_share` ($\times 3.0$, base $0.50$), `pressure_efficiency` ($\times 3.0$, base $0.25$)

#### 7.3.8. `gold.vw_similar_attacking_midfielders`
* **Benchmark Target:** User club's top Central Attacking Midfielder.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.position IN ('Central Attacking Midfielder', 'Right Attacking Midfielder', 'Left Attacking Midfielder') AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `npxg_plus_xa_per90` ($\times 3.0$), `xa_per_op_kp` ($\times 5.0$, base $0.20$)
  * `open_play_key_passes_per90` ($\times 2.0$), `prog_rate_per_pass` ($/ 5.0$), `dribbles_made_per90` ($\times 1.0$)
  * `turnover_rate` ($\times 5.0$, base $0.15$), `goal_threat_share` ($\times 3.0$, base $0.50$), `pressure_efficiency` ($\times 3.0$, base $0.25$)

#### 7.3.9. `gold.vw_similar_goalkeepers`
* **Benchmark Target:** User club's Goalkeeper with most minutes.
* **Filter:** `tgt.is_own_squad = 0 AND tgt.minutes_played >= 900`.
* **Tactical Metric Weights & Baselines:**
  * `shot_stopping_delta` ($\times 5.0$, base $0.0$), `xGoals_prevented_per90` ($\times 3.0$, base $0.0$)
  * `hold_rate_pct` ($/ 10.0$, base $40.0$), `sweeper_index` ($/ 3.0$, base $0.0$)
  * `distribution_value` ($/ 10.0$, base $0.0$), `pass_accuracy` ($\times 5.0$, base $0.70$)

---

### 7.4. Multi-Season Historical & Growth Tracking Views

Powered by Slowly Changing Dimensions (SCD Type 2), these views preserve all historical season snapshots without in-place overwrites, enabling longitudinal player development tracking.

#### 7.4.1. `gold.mart_outfield_historical_snapshots`
* **Grain:** One row per outfield player per historical snapshot version.
* **Source Tables:** `gold.fact_outfield_player_stats` (s), `gold.dim_player_info` (p), `gold.dim_team_info` (t), `gold.fact_players_value` (v).
* **Lineage Tracking Columns:**
  * `snapshot_date`: `s.dwh_create_date` (Timestamp when the record became active).
  * `snapshot_valid_till`: `s.dwh_cd_valid_till` (Timestamp when superseded; `'9999-12-31'` if current).
  * `dwh_current_validity`: Bit flag (`1` = active version; `0` = historical).
  * `snapshot_status`: `'Current'` or `'Historical'`.
  * **`snapshot_version`:** `DENSE_RANK() OVER (PARTITION BY p.player_id ORDER BY s.dwh_create_date ASC)`. Chronological integer sequence ($1, 2, 3\dots$) representing successive seasons.

#### 7.4.2. `gold.vw_player_growth_tracking`
* **Grain:** One row per player season transition (`curr.snapshot_version = prev.snapshot_version + 1`).
* **Source Tables:** Self-join on `gold.mart_outfield_historical_snapshots`.
* **Business Purpose:** Tracks exact player progression, asset appreciation, or tactical decline from season $t-1$ to season $t$.

| Column Name | Data Type | Description / Delta Formula |
| :--- | :--- | :--- |
| `player_id`, `player_name`, `position` | Various | Player entity identifiers. |
| `current_club`, `previous_club` | `NVARCHAR(35)` | Club movement tracking between snapshots. |
| `current_season_num`, `previous_season_num` | `BIGINT` | Chronological version pair ($t$ and $t-1$). |
| `current_age`, `previous_age` | `INT` | Age progression. |
| `current_minutes`, `previous_minutes` | `INT` | Playing time comparison. |
| **`delta_transfer_value_m`** | `DECIMAL(6,2)` | **Formula:** `curr.transfer_value_m - prev.transfer_value_m`. Valuation growth (£M). |
| **`delta_wage_per_week_k`** | `DECIMAL(6,2)` | **Formula:** `curr.wage_per_week_k - prev.wage_per_week_k`. Wage progression (£k/wk). |
| **`delta_npxg_plus_xa_per90`** | `DECIMAL(5,2)` | **Formula:** `curr.npxg_plus_xa_per90 - prev.npxg_plus_xa_per90`. Direct attacking threat trajectory. |
| **`delta_key_passes_per90`** | `DECIMAL(5,2)` | **Formula:** `curr.open_play_key_passes_per90 - prev.open_play_key_passes_per90`. Playmaking volume delta. |
| **`delta_dribbles_per90`** | `DECIMAL(5,2)` | **Formula:** `curr.dribbles_made_per90 - prev.dribbles_made_per90`. 1v1 take-on development. |
| **`delta_prog_passes_per90`** | `DECIMAL(5,2)` | **Formula:** `curr.progressive_passes_per90 - prev.progressive_passes_per90`. Progression volume delta. |
| **`delta_prog_rate_pct`** | `DECIMAL(5,2)` | **Formula:** `curr.prog_rate_per_pass - prev.prog_rate_per_pass`. Progression efficiency delta. |
| **`delta_def_actions_per90`** | `DECIMAL(5,2)` | **Formula:** `curr.defensive_actions_per90 - prev.defensive_actions_per90`. Defensive involvement delta. |
| **`delta_aerial_duels_won_per90`** | `DECIMAL(5,2)` | **Formula:** `curr.aerial_duels_won_per90 - prev.aerial_duels_won_per90`. Aerial dominance growth. |
| **`delta_pressure_efficiency`** | `DECIMAL(5,2)` | **Formula:** `curr.pressure_efficiency - prev.pressure_efficiency`. Press recovery efficiency trajectory. |

#### 7.4.3. `gold.vw_gk_growth_tracking`
* **Grain:** One row per goalkeeper season transition (`curr.snapshot_version = prev.snapshot_version + 1`).
* **Source Tables:** Self-join on `gold.fact_gk_stats`, `gold.dim_player_info`, `gold.dim_team_info`, `gold.fact_players_value`.
* **Key Delta Metrics:**
  * **`delta_transfer_value_m`:** Valuation change (£M).
  * **`delta_shot_stopping`:** `curr.shot_stopping_delta - prev.shot_stopping_delta`. Save percentage overperformance trajectory.
  * **`delta_xg_prevented_per90`:** Change in expected goals prevented per 90.
  * **`delta_hold_rate_pct`:** `curr.hold_rate_pct - prev.hold_rate_pct`. Rebound prevention growth.
  * **`delta_sweeper_index`:** `curr.sweeper_index - prev.sweeper_index`. Sweeper activity growth.
  * **`delta_distribution_value`:** `curr.distribution_value - prev.distribution_value`. Passing value delta.

---

### 7.5. Advanced Recruitment & Strategic Intelligence Views

#### 7.5.1. `gold.vw_squad_succession_plan`
* **Grain:** One row per veteran-successor pairing (up to top 3 recommended U23 successors per aging starter).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Business Purpose:** Eliminates squad aging cliffs by proactively identifying U23 recruitment targets before veteran starters decline or retire.

| Column Name | Data Type | Description / Logic |
| :--- | :--- | :--- |
| `veteran_player` | `VARCHAR(35)` | Name of squad player approaching succession age ($\text{Age} \ge 28$, $\text{Minutes} \ge 450$, `is_own_squad = 1`). |
| `veteran_position` | `VARCHAR(28)` | Veteran position. |
| `veteran_age` | `INT` | Veteran age. |
| `veteran_minutes` | `INT` | Veteran season minutes. |
| `veteran_value_m` | `DECIMAL(10,2)` | Veteran market valuation. |
| **`replacement_urgency`** | `VARCHAR(20)` | **Logic:** `'Urgent (30+)'` if $\text{Age} \ge 30$; `'Approaching (28-29)'` if $\text{Age} \ge 28$. |
| **`candidate_rank`** | `BIGINT` | **Formula:** `ROW_NUMBER() OVER (PARTITION BY veteran ORDER BY u23_cost_per_xgi ASC, u23_minutes DESC)`. Position-specific rank ($\le 3$). |
| `recommended_successor` | `VARCHAR(35)` | Name of U23 transfer candidate ($\text{Age} \le 23$, $\text{Minutes} \ge 900$, `is_own_squad = 0`). |
| `u23_position`, `u23_age`, `u23_club`, `u23_league` | Various | Candidate demographics and club. |
| `u23_value_m`, `u23_wage_k` | `DECIMAL(10,2)` | Candidate transfer value and wage. |
| `u23_minutes`, `u23_npxg_plus_xa`, `u23_prog_passes`, `u23_def_actions` | Various | Candidate tactical output. |
| `u23_cost_per_xgi` | `DECIMAL(10,2)` | Candidate acquisition cost per expected goal involvement. |

#### 7.5.2. `gold.vw_bargain_watch`
* **Grain:** One row per external bargain target meeting Moneyball criteria.
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Filter:** `is_own_squad = 0 AND minutes_played >= 900 AND (transfer_value_m <= 15.0 OR wage_per_week_k <= 30.0) AND (npxg_plus_xa_per90 >= 0.30 OR defensive_actions_per90 >= 5.00)`.
* **Business Purpose:** Identifies elite per-90 performers who are commercially undervalued due to playing in lower-reputation leagues, for lower-tier clubs, or having expiring contracts.

| Column Name | Data Type | Nullable | Description / Metric Formula |
| :--- | :--- | :--- | :--- |
| `player_key`, `player_name`, `age`, `position`, `club_name`, `league` | Various | NO/YES | Identifiers and club context. |
| `wage_per_week_k`, `transfer_value_m`, `minutes_played` | Various | YES | Financials and competitive sample minutes. |
| `npxg_plus_xa_per90`, `goals_per90`, `assists_per90` | Various | YES | Attacking output per 90. |
| `progressive_passes_per90`, `defensive_actions_per90` | Various | YES | Ball progression and defensive output. |
| **`cost_per_xgi`** | `DECIMAL(10,2)` | YES | **Formula:** `(wage_per_week_k * 52.0 + transfer_value_m * 1000.0) / NULLIF(npxg_plus_xa_per90, 0)`. Total annual cost (£k) per non-penalty goal involvement. |
| **`attacking_bargain_index`** | `DECIMAL(10,2)` | YES | **Formula:** `CAST((npxg_plus_xa_per90 * 100.0) / NULLIF(transfer_value_m + (wage_per_week_k * 0.052), 0) AS DECIMAL(10,2))`. Non-penalty attacking threat delivered per £1M of total annual amortized player cost. |
| **`defensive_bargain_index`** | `DECIMAL(10,2)` | YES | **Formula:** `CAST((defensive_actions_per90 * 10.0) / NULLIF(transfer_value_m + (wage_per_week_k * 0.052), 0) AS DECIMAL(10,2))`. Defensive actions delivered per £1M of total annual amortized player cost. |
| **`cost_efficiency_rank`** | `BIGINT` | NO | **Formula:** `DENSE_RANK() OVER (ORDER BY cost_per_xgi ASC)`. Overall Moneyball recruitment priority ranking. |

#### 7.5.3. `gold.vw_tactical_archetypes`
* **Grain:** One row per player snapshot with $\ge 450$ minutes (`minutes_played >= 450`).
* **Source Tables:** `gold.mart_outfield_scouting_summary`.
* **Business Purpose:** Automatically maps player statistical profiles to Football Manager tactical roles using mutually exclusive threshold classification logic.

```
Tactical Archetype Classification Rules:
------------------------------------------------------------------------------------------------------
Position: Striker
  ├── Aerial Duels >= 4.0 AND Heading Acc >= 55%  ──> 'Target Forward'
  ├── NPxG/90 >= 0.35 AND Conversion Rate >= 15% ──> 'Poacher / Finisher'
  ├── xA/90 >= 0.12 AND OP-KP/90 >= 0.70          ──> 'Complete / Deep-Lying Forward'
  └── Otherwise                                   ──> 'Pressing / Advanced Forward'

Position: Central Defender
  ├── Prog Rate >= 5.0% AND Prog Passes >= 4.0    ──> 'Ball-Playing Defender'
  ├── Aerial Duels >= 3.5 AND Heading Acc >= 65%  ──> 'Stopper / No-Nonsense Defender'
  ├── Reactive vs Positional Ratio >= 0.55        ──> 'Cover / Interceptor Defender'
  └── Otherwise                                   ──> 'Traditional Central Defender'

Position: Flank Defender (Fullback / Wingback)
  ├── Completed Crosses >= 1.0 AND Sprints >= 12  ──> 'Attacking Wingback'
  ├── Prog Rate >= 6.0% AND Turnover Rate <= 18%  ──> 'Inverted Fullback'
  ├── Defensive Actions >= 5.5                    ──> 'Defensive Fullback'
  └── Otherwise                                   ──> 'Balanced Fullback'

Position: Central / Defensive Midfielder
  ├── Passes >= 55, Pass Acc >= 88%, Prog >= 4.5  ──> 'Deep-Lying Playmaker'
  ├── Defensive Actions >= 5.5 AND Press Eff >= 25%──> 'Ball-Winning Midfielder'
  ├── Distance >= 13km, Sprints >= 10, NPxG+xA >= 0.15 ──> 'Box-to-Box Midfielder'
  ├── Prog Rate >= 7.0%                           ──> 'Roaming Playmaker / Mezzala'
  └── Otherwise                                   ──> 'Central Midfielder'

Position: Attacking Midfielders & Wingers
  ├── Goal Threat Share >= 55% AND Shots >= 1.8   ──> 'Inside Forward'
  ├── Completed Crosses >= 1.0                    ──> 'Traditional Winger'
  ├── CAM AND Goal Threat Share >= 50%            ──> 'Shadow Striker'
  ├── xA/90 >= 0.20                               ──> 'Advanced Playmaker'
  └── Otherwise                                   ──> 'Utility Attacker'
```

#### 7.5.4. `gold.vw_team_tactical_archetypes`
* **Grain:** One row per club snapshot (`dwh_current_validity = 1`).
* **Source Tables:** `gold.dim_team_tactical_archetypes`.
* **Business Purpose:** Central reporting view for team-level tactical frameworks and affinity scores across 8 tactical archetypes.

| Column Name | Data Type | Nullable | Description / Metric Formula |
| :--- | :--- | :--- | :--- |
| `team_key`, `club_name`, `league` | Various | NO/YES | Club and league identifiers. |
| `active_players`, `total_minutes` | `INT` | NO | Squad sample size and cumulative playing time. |
| `tactical_archetype` | `VARCHAR(35)` | NO | Primary tactical style (e.g. `'Gegenpressing'`, `'Tiki-Taka'`, `'Route One'`). |
| `tactical_framework` | `VARCHAR(50)` | NO | Parent tactical framework category (1–7). |
| `tactical_description` | `NVARCHAR(500)`| YES | Tactical identity narrative. |
| `pressures_attempted_per90` | `DECIMAL(5,2)` | YES | Aggregated pressing load per 90. |
| `passes_attempted_per90` | `DECIMAL(5,2)` | YES | Aggregated passing volume per 90. |
| `pass_accuracy` | `DECIMAL(5,3)` | YES | Aggregated passing completion rate. |
| `progressive_passes_per90` | `DECIMAL(5,2)` | YES | Aggregated forward line-breaking passes per 90. |
| `crosses_attempted_per90` | `DECIMAL(5,2)` | YES | Aggregated flank deliveries per 90. |
| `headers_attempted_per90` | `DECIMAL(5,2)` | YES | Aggregated aerial duel volume per 90. |
| `clearances_per90` | `DECIMAL(5,2)` | YES | Aggregated defensive clearances per 90. |
| `blocks_per90` | `DECIMAL(5,2)` | YES | Aggregated defensive blocks per 90. |
| Affinity Scores ($0\text{--}100$) | `INT` | YES | `gegenpressing_score`, `tiki_taka_score`, `vertical_tiki_taka_score`, `fluid_counter_score`, `direct_counter_score`, `wing_play_score`, `route_one_score`, `catenaccio_score`. |

#### 7.5.5. `gold.vw_player_system_outliers`
* **Grain:** One row per player snapshot ($\ge 450$ minutes) joined to their club's tactical archetype.
* **Source Tables:** `gold.mart_outfield_scouting_summary` (p), `gold.dim_team_tactical_archetypes` (ta).
* **Business Purpose:** Flags players displaying statistically verified divergence from their team's prevailing tactical constraints, allowing recruitment teams to identify undervalued technical or physical gems operating in suboptimal tactical environments.

| Column Name | Data Type | Nullable | Description / Outlier Classification Rule |
| :--- | :--- | :--- | :--- |
| `player_key`, `player_name`, `position`, `club_name` | Various | NO/YES | Player attributes and club entity. |
| `team_tactical_archetype` | `VARCHAR(35)` | NO | Club's prevailing tactical archetype. |
| `team_tactical_framework` | `VARCHAR(50)` | NO | Club's parent tactical framework. |
| **`outlier_tag`** | `VARCHAR(50)` | YES | Categorical outlier anomaly label: |
| | | | • **`High-Precision Striker in Direct System`**: Striker with `pass_accuracy >= 0.84` and `passes >= 12.0/90` in Route One or Direct Counter. |
| | | | • **`High-Clearance Defender in Tiki-Taka`**: Defender with `clearances_per90 >= 1.45` in Tiki-Taka or Vertical Tiki-Taka. |
| | | | • **`Attacking Talisman in Deep Block`**: Attacker/Midfielder with `npxg_plus_xa_per90 >= 0.35` in Catenaccio. |
| | | | • **`Lone Pressing Engine`**: Forward/CAM with `pressures >= 13.0/90` in a team with team pressures $\le 7.2/90$. |
| | | | • **`Dynamic Ball Carrier in Direct System`**: Player with `dribbles_made_per90 >= 1.80` in Route One or Direct Counter. |
| | | | • **`Central Incision Playmaker in Flank System`**: CAM/CM with `OP-KP/90 >= 1.30` and crossing share $\le 25\%$ in Wing Play. |
| | | | • **`Ball-Playing Quarterback in Direct System`**: Central Defender with `prog_passes >= 4.50` and `pass_acc >= 0.86` in Route One. |
| | | | • **`Ball Retention Hub in Gegenpress`**: Midfielder with `pass_acc >= 0.89` and `turnover_rate <= 0.14` in Gegenpressing. |
| **`outlier_rationale`** | `NVARCHAR(500)`| YES | Narrative analytical explanation of the tactical anomaly. |
| Player Output Metrics | Various | YES | `player_pass_accuracy`, `player_clearances_per90`, `player_npxg_plus_xa_per90`, `player_dribbles_made_per90`, etc. |
| Team Benchmark Metrics | Various | YES | `team_pass_accuracy`, `team_clearances_per90`, `team_pressures_attempted_per90`, `team_headers_attempted_per90`, etc. |


