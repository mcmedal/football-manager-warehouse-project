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
  * Financials parsed into weekly wage (€k) and market transfer value (€M).

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
| `league_difficulty_coef` | `DECIMAL(3, 2)` | NO | Quality adjustment coefficient (e.g. `1.00` for Top 5, `0.85` for tier 2). |

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
| `wage_per_week_k` | `DECIMAL(10, 2)` | YES | Weekly contractual wage in thousands (€k). |
| `transfer_value_m` | `DECIMAL(10, 2)` | YES | Estimated transfer market valuation in millions (€M). |
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

## 7. Analytical Views Catalog

| View Name | Schema | Description |
| :--- | :--- | :--- |
| `mart_outfield_scouting_summary` | `gold` | Master outfield reporting mart with derived metrics ($NPxG+xA$, defensive actions, progressive rates). |
| `mart_gk_scouting_summary` | `gold` | Master goalkeeper reporting mart with shot-stopping delta, hold rate %, and sweeper keeper index. |
| `vw_transfer_targets_outfield` | `gold` | Pre-filtered target pool ($Mins \ge 900$, $is\_own\_squad = 0$) with `PERCENT_RANK()` percentiles. |
| `vw_similar_strikers` | `gold` | Striker replacement similarity engine matching against starting forward. |
| `vw_similar_central_defenders` | `gold` | Central defender replacement similarity engine matching against starting centerback. |
| `vw_similar_flank_defenders` | `gold` | Fullback / wingback replacement engine matching crossing, progressive running, and ball retention. |
| `vw_similar_defensive_midfielders`| `gold` | Ball-winner / deep defensive midfielder replacement engine. |
| `vw_similar_central_midfielders` | `gold` | Central midfielder engine matching volume passing, progression share, and box-to-box engine metrics. |
| `vw_similar_wingers` | `gold` | Wide attacker similarity engine matching dribbling, chance creation quality, and goal threat share. |
| `vw_similar_attacking_midfielders`| `gold` | Number 10 / attacking midfielder engine matching $xA$, key passes, and progression share. |
| `vw_similar_goalkeepers` | `gold` | Goalkeeper similarity engine matching shot stopping delta, hold rate, and sweep actions. |
| `mart_outfield_historical_snapshots` | `gold` | Full multi-season outfield player history spanning all SCD Type 2 snapshots. |
| `vw_player_growth_tracking` | `gold` | Season-over-season delta tracking ($\Delta NPxG+xA$, $\Delta$ Progressive Passes, $\Delta$ Valuation). |
| `vw_gk_growth_tracking` | `gold` | Season-over-season goalkeeper progression tracking ($\Delta$ Shot Stopping Delta, $\Delta$ Hold Rate). |
| `vw_squad_succession_plan` | `gold` | Squad age cohort analyzer automatically recommending U23 replacements for aging starters. |
| `vw_bargain_watch` | `gold` | Moneyball value-efficiency scanner finding top per-90 contributors with minimal cost per xGI. |
| `vw_tactical_archetypes` | `gold` | Categorizes all players into FM tactical roles (*Ball-Playing Defender*, *Inside Forward*, etc.). |
