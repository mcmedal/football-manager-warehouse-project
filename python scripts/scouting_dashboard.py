"""
======================================================================
Football Manager Moneyball Scouting & Visual Analytics Engine
======================================================================
Purpose:
    Visual analytics tool that connects to the DW warehouse
    (or falls back to raw CSV exports in /documents) to produce:
      1. Player Comparison Radar / Spider Charts (Target vs Starter)
      2. Moneyball Quadrant Scatter Plots (Output vs Cost)
      3. Excel / CSV Shortlist Exporter with Moneyball Rankings

Usage:
    python "python scripts/scouting_dashboard.py" --mode scatter
    python "python scripts/scouting_dashboard.py" --mode radar --target "Lucas Beltrán"
    python "python scripts/scouting_dashboard.py" --mode export
======================================================================
"""

import os
import sys
import csv
import math
import glob
import argparse

try:
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    HAS_DATA_STACK = True
except ImportError:
    HAS_DATA_STACK = False


# ---------------------------------------------------------------------
# 1. DATA LOADING & MONEYBALL CALCULATION ENGINE
# ---------------------------------------------------------------------
def load_data(base_dir=None):
    """Loads outfield dataset either from warehouse CSVs or database."""
    if base_dir is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    doc_dir = os.path.join(base_dir, "documents")
    team_csv = os.path.join(doc_dir, "team_players.csv")
    int_csv_files = sorted(glob.glob(os.path.join(doc_dir, "interested_out_players*.csv")))

    if not os.path.exists(team_csv) or not int_csv_files:
        print(f"[ERROR] Could not find required data files in {doc_dir}")
        sys.exit(1)

    print(f">>> Loading squad dataset and {len(int_csv_files)} scouting partition(s)...")
    team_df = pd.read_csv(team_csv)
    team_df["is_own_squad"] = 1

    int_dfs = [pd.read_csv(f) for f in int_csv_files]
    int_df = pd.concat(int_dfs, ignore_index=True)
    if "UID" in int_df.columns:
        int_df = int_df.drop_duplicates(subset=["UID"], keep="first")
    int_df["is_own_squad"] = 0

    # Standardize column headers and concatenate
    combined = pd.concat([team_df, int_df], ignore_index=True)

    # Clean numeric fields
    def clean_num(val):
        if pd.isna(val) or val == "-" or val == "":
            return 0.0
        if isinstance(val, str):
            val = val.replace(",", "").replace("%", "").replace("km", "").replace("£", "").replace(" p/w", "")
            # Handle wage K formatting
            if "K" in val:
                val = float(val.replace("K", "")) * 1000
        try:
            return float(val)
        except:
            return 0.0

    def clean_val(val):
        if pd.isna(val) or val == "Not for Sale" or val == "-":
            return 25.0 # baseline median
        if isinstance(val, str):
            # Take upper bound of range: e.g. £10M - £12.5M
            if "-" in val:
                val = val.split("-")[-1].strip()
            val = val.replace("£", "").replace(",", "").strip()
            if "M" in val:
                return float(val.replace("M", "").strip())
            elif "K" in val:
                return float(val.replace("K", "").strip()) / 1000.0
        try:
            return float(val)
        except:
            return 10.0

    combined["minutes_played"] = combined["Mins"].apply(clean_num)
    combined["npxg_per90"] = combined["NP-xG/90"].apply(clean_num)
    combined["xa_per90"] = combined["xA/90"].apply(clean_num)
    combined["npxg_plus_xa"] = combined["npxg_per90"] + combined["xa_per90"]
    combined["prog_passes"] = combined["Pr passes/90"].apply(clean_num)
    combined["tackles_won"] = combined["Tck A"].apply(clean_num) * (combined["Tck R"].apply(clean_num) / 100.0)
    combined["interceptions"] = combined["Int/90"].apply(clean_num)
    combined["clearances"] = combined["Clr/90"].apply(clean_num)
    combined["blocks"] = combined["Blk/90"].apply(clean_num)
    combined["defensive_actions"] = combined["tackles_won"] + combined["interceptions"] + combined["clearances"] + combined["blocks"]
    combined["aerials_won"] = combined["Aer A/90"].apply(clean_num) * (combined["Hdr %"].apply(clean_num) / 100.0)
    combined["transfer_val_m"] = combined["Transfer Value"].apply(clean_val)
    combined["player_name"] = combined["Player"].str.replace(" - Pick Player", "").str.strip()

    # Cost per xGI
    combined["cost_per_xgi"] = (combined["transfer_val_m"] * 1000.0) / combined["npxg_plus_xa"].replace(0, np.nan)

    # Filter statistically valid sample
    qualified = combined[combined["minutes_played"] >= 900].copy()
    print(f"    [INFO] Processed {len(qualified)} qualified players (>= 900 minutes).")
    return qualified

# ---------------------------------------------------------------------
# 2. MONEYBALL QUADRANT SCATTER PLOT
# ---------------------------------------------------------------------
def generate_scatter_plot(df, output_path="moneyball_quadrant.png"):
    """Plots Output (NPxG+xA) vs Valuation with Moneyball Quadrants."""
    plt.figure(figsize=(12, 8), dpi=150)
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")

    targets = df[df["is_own_squad"] == 0]
    starters = df[df["is_own_squad"] == 1]

    # Medians for quadrant thresholds
    med_val = targets["transfer_val_m"].median()
    med_out = targets["npxg_plus_xa"].median()

    # Scatter points
    plt.scatter(targets["transfer_val_m"], targets["npxg_plus_xa"], color="#1f77b4", alpha=0.5, s=40, label="Transfer Targets")
    plt.scatter(starters["transfer_val_m"], starters["npxg_plus_xa"], color="#d62728", s=90, edgecolors="black", linewidths=1.5, label="Own Squad")

    # Quadrant lines
    plt.axvline(med_val, color="gray", linestyle="--", alpha=0.7)
    plt.axhline(med_out, color="gray", linestyle="--", alpha=0.7)

    # Annotate Top Bargains
    bargains = targets[(targets["transfer_val_m"] <= med_val) & (targets["npxg_plus_xa"] >= med_out)]
    top_bargains = bargains.sort_values(by="npxg_plus_xa", ascending=False).head(5)
    for _, row in top_bargains.iterrows():
        plt.annotate(row["player_name"], (row["transfer_val_m"], row["npxg_plus_xa"]),
                     fontsize=9, weight="bold", color="#0b5394",
                     xytext=(6, 4), textcoords="offset points")

    plt.title("Football Manager Moneyball Engine: Chance Creation vs Transfer Valuation", fontsize=14, weight="bold", pad=15)
    plt.xlabel("Transfer Value (£M)", fontsize=11, labelpad=10)
    plt.ylabel("NPxG + xA per 90 (Goal Threat)", fontsize=11, labelpad=10)
    plt.legend(frameon=True, facecolor="white", loc="upper right")
    plt.text(med_val * 0.2, med_out * 1.6, "★ MONEYBALL BARGAIN QUADRANT\n(High Output, Low Cost)", fontsize=10, color="green", weight="bold", alpha=0.8)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"    [SUCCESS] Moneyball scatter plot saved to: {output_path}")

# ---------------------------------------------------------------------
# 3. PLAYER COMPARISON RADAR / SPIDER CHART
# ---------------------------------------------------------------------
def generate_radar_chart(df, target_name, output_path="player_radar_comparison.png"):
    """Compares a target player against your top squad starter across 6 metrics."""
    categories = ["NPxG+xA /90", "Prog Passes /90", "Def Actions /90", "Aerial Duels /90", "Min Played (k)", "Value Efficiency"]
    N = len(categories)

    # Find target and starter
    target = df[df["player_name"].str.contains(target_name, case=False, na=False)]
    if target.empty:
        print(f"[WARNING] Player '{target_name}' not found. Using top qualified target.")
        target = df[df["is_own_squad"] == 0].sort_values(by="npxg_plus_xa", ascending=False).iloc[0:1]
    else:
        target = target.iloc[0:1]

    # Benchmark starter (top minute player in squad)
    starter = df[df["is_own_squad"] == 1].sort_values(by="minutes_played", ascending=False).iloc[0:1]

    def extract_vals(row):
        return [
            row["npxg_plus_xa"].values[0] * 2.0,            # scaled for 0-1 range
            row["prog_passes"].values[0] / 8.0,
            row["defensive_actions"].values[0] / 10.0,
            row["aerials_won"].values[0] / 6.0,
            row["minutes_played"].values[0] / 3000.0,
            min(1.0, 50.0 / max(row["cost_per_xgi"].values[0], 1.0))
        ]

    tgt_vals = extract_vals(target)
    bmk_vals = extract_vals(starter)

    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    tgt_vals += tgt_vals[:1]
    bmk_vals += bmk_vals[:1]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True), dpi=150)
    plt.xticks(angles[:-1], categories, color="grey", size=10, weight="bold")

    # Plot starter
    ax.plot(angles, bmk_vals, linewidth=2, linestyle="solid", label=f"Squad Starter: {starter['player_name'].values[0]}", color="#d62728")
    ax.fill(angles, bmk_vals, color="#d62728", alpha=0.15)

    # Plot target
    ax.plot(angles, tgt_vals, linewidth=2, linestyle="solid", label=f"Target: {target['player_name'].values[0]}", color="#1f77b4")
    ax.fill(angles, tgt_vals, color="#1f77b4", alpha=0.25)

    plt.title(f"Recruitment Profile Radar\n{target['player_name'].values[0]} vs {starter['player_name'].values[0]}", size=13, weight="bold", y=1.08)
    plt.legend(loc="upper right", bbox_to_anchor=(0.1, 0.1), frameon=True)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"    [SUCCESS] Radar comparison chart saved to: {output_path}")

# ---------------------------------------------------------------------
# 4. EXCEL / CSV SHORTLIST EXPORTER
# ---------------------------------------------------------------------
def export_shortlist(df, output_path="scouting_shortlist.csv"):
    """Exports ranked Moneyball recruitment shortlist."""
    targets = df[df["is_own_squad"] == 0].copy()
    targets["moneyball_rank"] = targets["cost_per_xgi"].rank(ascending=True)

    cols = ["moneyball_rank", "player_name", "Best Pos", "Age", "Club", "Division", "minutes_played",
            "transfer_val_m", "npxg_plus_xa", "prog_passes", "defensive_actions", "aerials_won", "cost_per_xgi"]
    existing_cols = [c for c in cols if c in targets.columns]

    shortlist = targets[existing_cols].sort_values(by="moneyball_rank").head(50)
    shortlist.to_csv(output_path, index=False)
    print(f"    [SUCCESS] Top 50 Moneyball targets exported to: {output_path}")

def export_shortlist_stdlib(base_dir=None, output_path="scouting_shortlist.csv"):
    """Fallback exporter using standard library csv module."""
    if base_dir is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    doc_dir = os.path.join(base_dir, "documents")
    int_csv_files = sorted(glob.glob(os.path.join(doc_dir, "interested_out_players*.csv")))
    if not int_csv_files:
        print(f"[ERROR] Could not find interested outfield player files in {doc_dir}")
        return

    rows = []
    seen_uids = set()
    for int_csv in int_csv_files:
        with open(int_csv, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for r in reader:
                uid = r.get("UID", "").strip()
                if uid and uid in seen_uids:
                    continue
                if uid:
                    seen_uids.add(uid)

                mins_str = r.get("Mins", "0").replace(",", "").strip()
                try:
                    mins = float(mins_str)
                except:
                    mins = 0.0
                if mins < 900:
                    continue

                try:
                    npxg = float(r.get("NP-xG/90", 0.0) or 0.0)
                except:
                    npxg = 0.0
                try:
                    xa = float(r.get("xA/90", 0.0) or 0.0)
                except:
                    xa = 0.0
                npxg_plus_xa = round(npxg + xa, 2)

                val_str = r.get("Transfer Value", "10").replace("£", "").replace(",", "")
                if "-" in val_str:
                    val_str = val_str.split("-")[-1].strip()
                if "M" in val_str:
                    val_m = float(val_str.replace("M", "").strip() or 10.0)
                elif "K" in val_str:
                    val_m = float(val_str.replace("K", "").strip() or 1000.0) / 1000.0
                else:
                    val_m = 10.0

                cost_per_xgi = round((val_m * 1000.0) / max(npxg_plus_xa, 0.05), 2)
                player_name = r.get("Name") or r.get("Player") or "Unknown"
                player_name = player_name.replace(" - Pick Player", "").strip()

                rows.append({
                    "Player": player_name,
                    "Position": r.get("Best Pos", ""),
                    "Age": r.get("Age", ""),
                    "Club": r.get("Club", ""),
                    "Division": r.get("Division", ""),
                    "Minutes": mins,
                    "Transfer_Value_M": val_m,
                    "NPxG_plus_xA_per90": npxg_plus_xa,
                    "Cost_per_xGI": cost_per_xgi
                })

    rows.sort(key=lambda x: x["Cost_per_xGI"])
    for idx, r in enumerate(rows[:50], 1):
        r["Moneyball_Rank"] = idx

    fieldnames = ["Moneyball_Rank", "Player", "Position", "Age", "Club", "Division", "Minutes", "Transfer_Value_M", "NPxG_plus_xA_per90", "Cost_per_xGI"]
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows[:50])
    print(f"    [SUCCESS] Top 50 Moneyball targets exported to: {output_path}")

# ---------------------------------------------------------------------
# MAIN CLI DISPATCHER
# ---------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Football Manager Analytics & Scouting Engine")
    parser.add_argument("--mode", choices=["scatter", "radar", "export", "all"], default="all", help="Analysis mode")
    parser.add_argument("--target", default="Lucas Beltrán", help="Target player name for radar comparison")
    args = parser.parse_args()

    if not HAS_DATA_STACK:
        print("[INFO] 'pandas' or 'matplotlib' not detected in environment.")
        print("       Exporting Moneyball Shortlist using Python standard library...")
        export_shortlist_stdlib()
        print("\n[TIP] To generate visual radar charts and scatter plots:")
        print("      Run: pip install pandas matplotlib")
    else:
        df = load_data()
        if args.mode in ["scatter", "all"]:
            generate_scatter_plot(df)
        if args.mode in ["radar", "all"]:
            generate_radar_chart(df, args.target)
        if args.mode in ["export", "all"]:
            export_shortlist(df)
        print("\n[COMPLETE] All visual analytics and scouting exports generated successfully!")

