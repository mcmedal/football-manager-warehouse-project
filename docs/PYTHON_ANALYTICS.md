# 🐍 Python Analytics & Visual Dashboard

This guide documents the Python analytical engine located in [`python scripts/scouting_dashboard.py`](python%20scripts/scouting_dashboard.py).

---

## 1. Overview & Capabilities

While SQL Server serves as the enterprise data warehouse engine, the Python analytics tool provides visual decision support, multi-attribute radar profiles, Moneyball valuation scatter plots, and exportable shortlist generation.

### Key Capabilities
1. **Recruitment Shortlist Generation:** Computes composite Moneyball output scores across the scouting pool and exports ranked shortlists to CSV (`scouting_shortlist.csv`).
2. **Tactical Radar Comparison:** Renders 6-dimensional radar charts contrasting a prospective transfer target directly against your squad's incumbent starter.
3. **Moneyball Scatter Plots:** Plots market valuation (£M) against per-90 offensive output ($NPxG + xA$) to visually map the efficiency frontier and identify undervalued bargains.
4. **Zero-Dependency Resilience:** Includes a standard library fallback (`csv`, `math`) that functions even on minimal environments lacking `pandas` or `matplotlib`.

---

## 2. Command Line Interface (CLI) Reference

The script is executed from the terminal using Python:

```bash
python "python scripts/scouting_dashboard.py" [OPTIONS]
```

### Supported Arguments

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--mode` | `choice` | `export` | Operational mode: `export`, `radar`, `scatter`, or `all`. |
| `--target` | `str` | `'Lucas Beltrán'` | Name of the prospective transfer target to analyze. |
| `--starter` | `str` | Auto-detected | Name of the incumbent squad player to use as the baseline. |
| `--position` | `str` | `'Striker'` | Positional filter (e.g. `'Striker'`, `'Central Defender'`, `'Winger'`). |
| `--top` | `int` | `50` | Maximum number of candidates to export in the shortlist. |
| `--output` | `str` | `'scouting_shortlist.csv'` | Output filename for exported recruitment shortlists. |

---

## 3. Usage Modes & Examples

### 3.1. Mode 1: Recruitment Shortlist Export
Scans the scouting pool in `/documents/`, normalizes metrics, calculates composite Moneyball scores, and outputs a ranked CSV:

```bash
python "python scripts/scouting_dashboard.py" --mode export --top 50
```

#### Output Columns in `scouting_shortlist.csv`:
* `Rank`: Priority recruitment ranking.
* `Name`, `Age`, `Club`, `Position`, `Minutes`: Core demographics.
* `NPxG_per90`, `xA_per90`, `ProgPass_per90`: Tactical output rates.
* `Transfer_Value_M`, `Wage_k`: Contractual parameters.
* `Moneyball_Score`: Weighted composite performance index ($0 - 100$).

---

### 3.2. Mode 2: Tactical Radar Chart Comparison
Compares a prospective transfer target directly against your current starting player across six tactical axes (e.g. Scoring Threat, Clinical Finish, Creation Quality, Aerial Duel Win %, Pressing Work Rate, Ball Progression):

```bash
# Compare a target striker against your starting forward:
python "python scripts/scouting_dashboard.py" \
    --mode radar \
    --target "Lucas Beltrán" \
    --starter "M'Bala Nzola" \
    --position "Striker"
```

* Renders a dual-polygon radar chart highlighting strengths and tactical trade-offs.
* Saves the resulting figure to `scouting_radar_<target_name>.png`.

---

### 3.3. Mode 3: Moneyball Valuation Scatter Plot
Plots player valuation against on-pitch goal contribution rates:

```bash
python "python scripts/scouting_dashboard.py" --mode scatter --position "Striker"
```

* **X-Axis:** Estimated Transfer Value (£M).
* **Y-Axis:** Total Goal Contributions ($NPxG + xA$ per 90).
* **Quadrant Analysis:**
  * **Top-Left (High Output, Low Valuation):** High-priority Moneyball bargains.
  * **Top-Right (High Output, High Valuation):** Premium established stars.
  * **Bottom-Left (Low Output, Low Valuation):** Squad depth / development prospects.
  * **Bottom-Right (Low Output, High Valuation):** Overpriced assets to avoid.
* Saves the visualization to `scouting_scatter_moneyball.png`.

---

## 4. Software Dependencies

The dashboard is structured with progressive enhancement:

```python
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_DATA_STACK = True
except ImportError:
    HAS_DATA_STACK = False
```

* **Full Visualization Stack:** Requires `matplotlib`, `pandas`, `numpy`.
  ```bash
  pip install pandas matplotlib numpy
  ```
* **Fallback Mode:** If run in a lean server environment without graphics libraries, `--mode export` executes seamlessly using Python's standard `csv` library, ensuring that recruitment shortlist generation never fails.

---

## 5. HTML to CSV Ingestion Converter (`html_to_csv.py`)

Football Manager natively exports custom views as Web Pages (`.html`). The utility script [`python scripts/html_to_csv.py`](python%20scripts/html_to_csv.py) parses these exported tables and converts them into clean, UTF-8 CSV files ready for Bronze ingestion.

### Key Features
* **Zero External Dependencies:** Built entirely with Python's built-in `html.parser` and `csv` modules.
* **Accented Characters & Currencies:** Preserves player names (e.g. `Martin Ødegaard`, `Jan-Niklas Beste`) and financial values (`£25,000 p/w`, `£5.8M - £9M`).
* **UTF-8 with BOM (`utf-8-sig`):** Formats output for immediate compatibility with SQL Server `BULK INSERT` (`CODEPAGE = '65001'`), Microsoft Excel, and `pandas`.

### Usage Commands

```bash
# Convert an entire folder of HTML exports (batch mode)
python "python scripts/html_to_csv.py" --dir documents/

# Convert a single file (auto-names output to documents/team_players.csv)
python "python scripts/html_to_csv.py" documents/team_players.html

# Convert a single file with custom output name
python "python scripts/html_to_csv.py" custom_export.html documents/interested_out_players1.csv
```
