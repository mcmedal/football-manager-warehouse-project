# 🌐 Football Manager Analytics Web App (FBref & Opta Scouting Engine)

A modern, high-performance web analytics application coupled directly to your **Microsoft SQL Server Data Warehouse** (`DW`). It provides a quantitative scouting platform featuring **FBref scouting reports**, **Opta / StatsBomb-style Pizza Radars**, a **TradingView-style Player Screener** for outfield players and goalkeepers, and a **vector similarity matching engine**.

---

## ⚡ Quick Start

> [!TIP]
> **🔰 First-time setup or new to coding?**  
> Follow the **[Zero-Coding Beginner Setup Guide](../docs/BEGINNER_SETUP_GUIDE.md)** for a complete step-by-step walkthrough on Windows, macOS, and Linux.

### Option 1: 1-Click Launch from Repository Root (Recommended)
From the root of the project:
* **🍏 macOS & 🐧 Linux:**
  ```bash
  chmod +x run_webapp.sh
  ./run_webapp.sh
  ```
* **🪟 Windows (PowerShell):**
  ```powershell
  powershell -ExecutionPolicy Bypass -File .\run_webapp.ps1
  ```
This automatically verifies that your SQL Server Docker container is running, checks your environment, installs dependencies if needed, and boots the development server at [http://localhost:3000](http://localhost:3000).

### Option 2: Manual Start
```bash
cd webapp

# 1. Install dependencies (first time only)
npm install

# 2. Start the Next.js dev server
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your web browser.

---

## ⚙️ Configuration (`.env.local`)

The application connects to SQL Server via the `mssql` (pure JS `tedious`) driver. Credentials are configured in `webapp/.env.local`:

```ini
MSSQL_USER=sa
MSSQL_PASSWORD=McMedal@14
MSSQL_SERVER=localhost
MSSQL_PORT=1433
MSSQL_DATABASE=DW
```

* Ensure your SQL Server container is running:
  ```bash
  docker compose up -d sql-server
  ```

---

## 🔍 Core Application Modules

| Route | Module | Features |
| :--- | :--- | :--- |
| **`/`** | **Scouting Hub** | Filter and sort 11,359 players by position, minimum minutes, maximum transfer valuation, squad starter vs target, and offensive/defensive output. |
| **`/screener`** | **TradingView Screener** | Quantitative screener featuring active filter pill chips (`Age ≤ 21`, `xGI ≥ 0.40`), presets (*Wonderkids*, *Moneyball Gems*, *Elite Playmakers*, *Shot-Stoppers*), CSV export, and dual **Outfield** vs **Goalkeeper** modes. |
| **`/player/[id]`** | **Player Dossier** | Complete player report: **Opta Pizza Radar Chart** (dual slice & polygon modes), **FBref Percentile Table** (0–99th percentile progress bars), **FBref Similar Players Widget**, and **SCD Type 2 Historical Career Progression**. |
| **`/compare`** | **Head-to-Head** | Side-by-side comparison tool with dual search selectors, an overlaid comparison radar chart, and an automated statistical edge differential matrix. |
| **`/archetypes`** | **Tactical Roles** | Browse players grouped by 22 distinct tactical archetypes (*Ball-Playing Defender*, *Poacher*, *Roaming Playmaker*, *Inverted Winger*, etc.) from `gold.vw_tactical_archetypes`. |
| **`/metrics`** | **Metrics & Visual Guide** | Comprehensive dictionary & interactive guide explaining all 30+ Moneyball metrics, mathematical formulas, benchmarks, and Opta/FBref graphs. |

---

## 🧤 Goalkeeper Analytics Support

Goalkeepers are first-class citizens across the app:
* **Screener (`/screener`)**: Toggle to **`🧤 Goalkeepers (855)`** to screen by **xGoals Prevented/90**, **Shot-Stopping Delta** (`Save% - xSave%`), **Hold Rate %** (rebound prevention), **Sweeper Index** (clearances + mobility), and **Distribution Output**.
* **Pizza Radar**: Displays dedicated goalkeeper radial slices categorized by **Shot Stopping** (Amber), **Handling & Rebounds** (Purple), **Sweeping & High Claims** (Rose), and **Distribution** (Cyan).
* **GK Similarity Engine**: Uses a specialized 6-variable Euclidean distance model matching goalkeepers across shot-stopping overperformance, sweep distance, and distribution accuracy.

---

## 🛠️ Build for Production

To validate TypeScript compilation and generate an optimized production bundle:
```bash
npm run build
npm run start
```
