# 🎮 Football Manager 24 Data Export Guide

This guide walks through configuring custom views in Football Manager 2024, exporting player performance and scouting data to CSV/HTML, and structuring files for ingestion into `DW`.

---

## 1. Overview of FM24 Data Extraction

Football Manager allows users to print or save any active grid view as a Web Page (`.html`) or Text file (`.txt` / `.csv`) via the standard `Ctrl + P` shortcut.

Place the player and goalkeeper CSV or HTML exports in your configured export directory or the project `/documents/` folder:

```text
documents/
├── team_players.csv               # Your squad's outfield players
├── team_gks.csv                   # Your squad's goalkeepers
├── interested_out_players*.csv    # Scouted outfield targets (1, 2, 3... up to 13+ partitions or single file)
├── interested_gks.csv             # Scouted goalkeeper targets
```

### 1.1. Default Export Locations
* **Linux (Steam Proton):**
  ```text
  /home/<username>/.local/share/Steam/steamapps/compatdata/2252570/pfx/drive_c/users/steamuser/Documents/Sports Interactive/Football Manager 2024/exported data/
  ```
* **Windows (Native):**
  ```text
  C:\Users\<username>\Documents\Sports Interactive\Football Manager 2024\exported data\
  ```

### 1.2. Changing Export & Ingestion Directories

If you store FM exports in a custom directory or want to change where standardized files are staged, configure the following:

#### A. Changing the Raw FM24 Source Directory (`FM_DATA_DIR`)
1. **Via `.env` file (Recommended):**
   Set `FM_DATA_DIR` in your [`.env`](file:///.env) file:
   ```bash
   FM_DATA_DIR="/path/to/your/custom/FM/exported data/"
   ```
   The ingestion pre-processor ([`python scripts/prepare_ingestion.py`](file:///python%20scripts/prepare_ingestion.py)) automatically detects `FM_DATA_DIR`, converts HTML exports to CSV, normalizes goalkeeper schemas, and deduplicates outfield partitions.
2. **Via Command Line:**
   Override the source directory explicitly when running the pre-processor:
   ```bash
   python3 "python scripts/prepare_ingestion.py" --source-dir "/path/to/your/custom/FM/exported data/"
   ```

#### B. Changing the Repository Staging Directory (`documents/`)
By default, the standardized UTF-8 CSVs ready for warehouse bulk insertion are written to the [`documents/`](file:///documents) directory. To change this destination:
1. **Pass `--target-dir` to the pre-processor:**
   ```bash
   python3 "python scripts/prepare_ingestion.py" --target-dir "custom_data_dir/"
   ```
2. **Update the Docker mount in [`docker-compose.yml`](file:///docker-compose.yml):**
   ```yaml
   volumes:
     - mssql_data:/var/opt/mssql
     - ./custom_data_dir:/data:ro     # Map your custom directory to /data
     - ./sql scripts:/scripts:ro
   ```
3. **Update [`run_pipeline_docker.sh`](file:///run_pipeline_docker.sh):**
   Update `--target-dir` (line 112) and the `docker cp` command (line 117) to reference your new folder.

#### C. Changing the SQL Server `BULK INSERT` Directory (`@DataPath`)
When executing stored procedures natively in SQL Server (SSMS, Azure Data Studio, or `sqlcmd`), pass your custom folder path to the `@DataPath` parameter:
* **Master Pipeline ([`sql scripts/orchestration/run_pipeline.sql`](file:///sql%20scripts/orchestration/run_pipeline.sql)):**
  ```sql
  EXEC dbo.usp_run_warehouse_pipeline 
      @Season = '2023/24', 
      @DataPath = 'C:\CustomPath\FM Data\', -- Use '/data/' for Docker container
      @RunTests = 1;
  ```
* **Bronze Loader ([`sql scripts/bronze/data-ingestion bronze.sql`](file:///sql%20scripts/bronze/data-ingestion%20bronze.sql)):**
  ```sql
  EXEC bronze.load_bronze @DataPath = 'C:\CustomPath\FM Data\';
  ```

#### D. Changing Analytical Dashboard Export Locations
The Python analytics engine ([`python scripts/scouting_dashboard.py`](file:///python%20scripts/scouting_dashboard.py)) outputs:
* **Moneyball Shortlist:** Defaults to `scouting_shortlist.csv` in the root directory. You can specify a custom path in [`export_shortlist(df, output_path="path/to/shortlist.csv")`](file:///python%20scripts/scouting_dashboard.py).
* **Visual Plots:** Radar (`radar_chart.png`) and scatter (`scatter_plot.png`) charts save directly to the working directory, configurable via `output_path` in `generate_radar_chart()` and `generate_scatter_plot()`.

---

## 2. In-Game View Setup & Required Columns

To ensure full compatibility with the Bronze landing tables, your custom views in Football Manager must include the exact columns listed below.

### 2.1. Outfield Players View (`team_players.csv` & `interested_out_players*.csv`)

In the **Squad** or **Scouting $\to$ Players** screen, create a custom view with the following fields:

| Category | Required FM24 Fields / Columns |
| :--- | :--- |
| **Demographics** | `ID` (Unique ID), `Player` / `Name`, `Club`, `Division` / `League`, `Position`, `Age`, `Mins` |
| **Contract / Value** | `Wage`, `Transfer Value`, `Contract Status` |
| **Attacking & Shooting**| `Gls`, `Gls Outside Box`, `Shots/90`, `xG/shot`, `Shots on Target/90`, `Shot %`, `Gls/90`, `xG`, `NP-xG/90`, `xG-OP`, `Shot Conv %` |
| **Creation & Passing** | `Ast`, `Ast/90`, `Pas A/90`, `Pas %`, `xA/90`, `OP-KP/90`, `Ch C/90`, `Drb/90`, `Pr passes/90` |
| **Crossing & Flank** | `OP-Crs A/90`, `OP-Cr %`, `Crs A/90`, `Cr C/A` |
| **Defending & Duels** | `Tck A`, `Tck R`, `Pres A/90`, `Pres C/90`, `Poss Won/90`, `Poss Lost/90`, `K Tck/90`, `Int/90`, `Clr/90`, `Blk/90`, `Sht Blk/90`, `Aer A/90`, `Hdr %`, `K Hdr/90` |
| **Physical & Athletic**| `Sprints/90`, `Dist/90` |
| **Discipline & Errors**| `Gl Mst`, `Fls`, `FA`, `Yel`, `Red` |

> [!TIP]
> **Enable Unique IDs:** In FM24, go to **Preferences $\to$ Advanced $\to$ Interface** and check **"Show screen IDs in title bar to assist skinning"** or ensure `ID` is enabled in your column picker.

---

### 2.2. Goalkeeper View (`team_gks.csv` & `interested_gks.csv`)

In your Squad or Scouting screen filtered by `Position: Goalkeeper`:

| Category | Required FM24 Fields / Columns |
| :--- | :--- |
| **Demographics** | `ID`, `Player` / `Name`, `Club`, `Division` / `League`, `Position`, `Age`, `Mins` |
| **Shot Stopping** | `Conc/90`, `Saves/90`, `xG Prevented/90`, `xSave %`, `Sv %`, `Tip/90`, `Par/90`, `Held/90` |
| **Distribution** | `Pas A/90`, `Pas %` |
| **Sweeping & Aerial**| `Int/90`, `Clr/90`, `Poss Won/90`, `Poss Lost/90` |
| **Penalties** | `Pens Faced`, `Pen Sv %` |
| **Physical & Errors**| `Dist/90`, `Gl Mst`, `FA` |

---

---

## 3. Step-by-Step Exporting Procedure

1. **Open View:** In Football Manager 2024, open the relevant screen (e.g. your first-team squad or scouting search).
2. **Apply Filter:**
   * For Squad: Ensure all first-team and youth reserve players you want analyzed are visible.
   * For Scouting: Set your scouting search criteria (e.g. *Transfer Interest: Slightly / Fairly / Very*, or leave open for a global database).
3. **Select All Rows:** Click any row in the table, then press **`Ctrl + A`** (or `Cmd + A` on macOS) to highlight all records.
4. **Trigger Export:** Press **`Ctrl + P`** (or click the FM menu $\to$ **Print**).
5. **Select Output Format:**
   * In the dialog, select **Web Page (*.html)**.
   * Save the file directly into the `documents/` folder (e.g. `team_players.html`, `interested_out_players1.html`, etc.).
6. **Convert HTML to CSV:**
   * Execute the included converter utility to transform the HTML files into clean UTF-8 CSV files:
     ```bash
     python "python scripts/html_to_csv.py" --dir documents/
     ```
   * Or convert an individual file:
     ```bash
     python "python scripts/html_to_csv.py" documents/team_players.html
     ```

> [!NOTE]
> **Dynamic Handling of Scouting Pools:** Football Manager may paginate or lag if exporting over 1,500 players at once. You can split large scouting pools by age or league into any number of partitions: `interested_out_players1.csv`, `interested_out_players2.csv`, `interested_out_players3.csv` ... up to `interested_out_players8.csv` (or even export a single `interested_out_players.csv`). The Bronze ingestion dynamically discovers and ingests all matching partitions into `bronze.fmdata_interested_out_players` without requiring any extra lines of code, and the Silver pipeline automatically deduplicates and standardizes the consolidated data.

---

## 4. Portability: Running on ANY Club or Save

`DW` is **100% club-agnostic**. You can use it for Arsenal, Borussia Dortmund, Ajax, or an amateur non-league club:

1. **Squad Mapping:**
   * Whatever players are exported into `team_players.csv` are automatically flagged with `is_own_squad = 1` in `gold.dim_player_info`.
   * The similarity engines dynamically identify your squad's primary starter in each position by sorting your squad by `minutes_played DESC`.
2. **Scouting Pool:**
   * All external targets in `interested_out_players*.csv` are tagged with `is_own_squad = 0`.
   * Even if players from your own club appear in the scouting export, Silver filters them out using `WHERE contracted = 0` against the target table to ensure no duplicate rows exist.
3. **League Difficulty Normalization:**
   * You can adjust league coefficients in `gold.dim_league` if playing in custom or lower-tier leagues (e.g. setting English Championship to `0.85` or Serie C to `0.65`).

---

## 5. Verification Checklist

Before running the warehouse pipeline, verify:
- [ ] All exported files exist in `documents/`.
- [ ] File headers match the expected names without stray trailing characters.
- [ ] UTF-8 encoding is preserved (especially for player names with accents like *Džeko*, *Ødegaard*, *Müller*).
- [ ] The files are closed in Excel before executing `BULK INSERT`.
