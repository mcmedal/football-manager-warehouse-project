"""
======================================================================
Football Manager 2024 - Data Preparation & Ingestion Normalizer
======================================================================
Purpose:
    Normalizes and synchronizes raw Football Manager 2024 exports
    (from Steam Proton on Linux, Wine, or Windows) into clean,
    standardized UTF-8 CSVs ready for Medallion Warehouse ingestion.

Key Features:
    1. Cross-platform path auto-discovery: Reads FM_DATA_DIR from .env
       or command line, with automatic detection of Steam Proton paths.
    2. HTML-to-CSV auto-conversion: If HTML exports are detected without
       CSVs or newer than CSVs, parses them on the fly.
    3. Multi-encoding resilience: Automatically handles UTF-16 LE with BOM,
       UTF-8, UTF-8-SIG, and Windows-1252.
    4. Outfield Partition Deduplication: Deduplicates players across
       multiple partition files (interested_out_players1..N.csv) by UID.
    5. Goalkeeper Normalization:
       - Maps interested_gks.csv (34 or 32 cols) into the standard 32-col schema.
       - Reconciles squad team_gks.csv (even if exported using outfield view)
         by matching starting GKs against interested_gks and synthesizing
         standard 30-col goalkeeper metrics.
======================================================================
"""

import os
import sys
import csv
import glob
import html
import argparse
from html.parser import HTMLParser

# Default Linux Steam Proton export path for Football Manager 2024
DEFAULT_STEAM_EXPORT_DIR = (
    "/home/mcmedal/.local/share/Steam/steamapps/compatdata/2252570/pfx/drive_c/users/steamuser/Documents/Sports Interactive/Football Manager 2024/exported data"
)

# Standard Bronze column definitions
EXPECTED_INT_GKS_COLS = [
    "Rec", "Inf", "UID", "Name", "Best Pos", "Age", "Club", "Division",
    "Mins", "Tgls/90", "Tcon/90", "Con/90", "Saves/90", "xGP/90", "xSv %",
    "Svt", "Svp", "Svh", "Sv %", "Ps A/90", "Pas %", "Poss Won/90",
    "Poss Lost/90", "Int/90", "Clr/90", "Pens Faced", "Pens Saved Ratio",
    "Dist/90", "Gl Mst", "FA", "Wage", "Transfer Value"
]

EXPECTED_TEAM_GKS_COLS = [
    "UID", "Player", "Best Pos", "Age", "Club", "Division", "Mins",
    "Tgls/90", "Tcon/90", "Con/90", "Saves/90", "xGP/90", "xSv %",
    "Svt", "Svp", "Svh", "Sv %", "Ps A/90", "Pas %", "Poss Won/90",
    "Poss Lost/90", "Int/90", "Clr/90", "Pens Faced", "Pens Saved Ratio",
    "Dist/90", "Gl Mst", "FA", "Wage", "Transfer Value"
]


class FMHTMLTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self._current_table = []
        self._current_row = []
        self._current_cell = []
        self._in_cell = False

    def handle_starttag(self, tag, attrs):
        tag_lower = tag.lower()
        if tag_lower == "table":
            self._current_table = []
        elif tag_lower == "tr":
            self._current_row = []
        elif tag_lower in ("th", "td"):
            self._in_cell = True
            self._current_cell = []

    def handle_endtag(self, tag):
        tag_lower = tag.lower()
        if tag_lower in ("th", "td"):
            self._in_cell = False
            raw_text = "".join(self._current_cell)
            clean_text = html.unescape(raw_text).replace("\xa0", " ").strip()
            self._current_row.append(clean_text)
        elif tag_lower == "tr":
            if self._current_row and any(c != "" for c in self._current_row):
                self._current_table.append(self._current_row)
        elif tag_lower == "table":
            if self._current_table:
                self.tables.append(self._current_table)

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell.append(data)


def read_file_with_encoding(file_path):
    """Detects encoding and returns content string."""
    with open(file_path, "rb") as f:
        raw_head = f.read(4)

    if raw_head[:2] in (b"\xff\xfe", b"\xfe\xff"):
        enc = "utf-16"
    else:
        enc = "utf-8-sig"

    try:
        with open(file_path, "r", encoding=enc) as f:
            return f.read()
    except UnicodeDecodeError:
        for fallback in ["cp1252", "latin-1"]:
            try:
                with open(file_path, "r", encoding=fallback) as f:
                    return f.read()
            except UnicodeDecodeError:
                continue
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def read_csv_rows(file_path):
    """Reads a CSV file returning (header_list, rows_list_of_dicts)."""
    with open(file_path, "rb") as f:
        raw_head = f.read(4)

    enc = "utf-16" if raw_head[:2] in (b"\xff\xfe", b"\xfe\xff") else "utf-8-sig"

    for enc_try in [enc, "utf-8-sig", "utf-8", "cp1252", "latin-1"]:
        try:
            with open(file_path, "r", encoding=enc_try, errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return [], []
                # Clean header names (strip quotes, BOMs, trailing spaces)
                clean_header = [h.strip().lstrip("\ufeff") for h in header]
                rows = []
                for row_vals in reader:
                    # Pad or truncate row to header length
                    if len(row_vals) < len(clean_header):
                        row_vals = row_vals + [""] * (len(clean_header) - len(row_vals))
                    elif len(row_vals) > len(clean_header):
                        row_vals = row_vals[:len(clean_header)]
                    rows.append(dict(zip(clean_header, row_vals)))
                return clean_header, rows
        except Exception:
            continue
    return [], []


def convert_html_to_csv_if_needed(html_path, csv_path):
    """Converts HTML to CSV if CSV does not exist or HTML is newer."""
    if not os.path.exists(html_path):
        return False

    if os.path.exists(csv_path):
        if os.path.getmtime(csv_path) >= os.path.getmtime(html_path):
            return True

    content = read_file_with_encoding(html_path)
    parser = FMHTMLTableParser()
    parser.feed(content)
    if not parser.tables:
        return False

    def table_score(t):
        return len(t) * max(len(r) for r in t) if t else 0

    table = max(parser.tables, key=table_score)
    max_cols = max(len(r) for r in table) if table else 0
    normalized_rows = [r + [""] * (max_cols - len(r)) for r in table]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(normalized_rows)
    return True


def normalize_interested_gks(src_csv_path, dest_csv_path):
    """Normalizes interested_gks.csv into standard 32-column format."""
    header, rows = read_csv_rows(src_csv_path)
    if not rows:
        return 0, {}

    indexed_by_uid = {}
    normalized_rows = []
    seen_uids = set()

    for r in rows:
        uid = r.get("UID", "").strip()
        if uid and uid in seen_uids:
            continue
        if uid:
            seen_uids.add(uid)
            indexed_by_uid[uid] = r

        row = []
        for col in EXPECTED_INT_GKS_COLS:
            val = r.get(col, "")
            # Fallback mappings for slightly varied FM views
            if not val or val == "":
                if col == "Name":
                    val = r.get("Player", "")
                elif col == "Division":
                    val = r.get("League", "")
                elif col == "Int/90":
                    val = "-"
                elif col == "Con/90":
                    val = r.get("Tcon/90", "-")
                elif col == "Tcon/90":
                    val = r.get("Con/90", "-")
                else:
                    val = "-"
            row.append(val)
        normalized_rows.append(row)

    with open(dest_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(EXPECTED_INT_GKS_COLS)
        writer.writerows(normalized_rows)

    return len(normalized_rows), indexed_by_uid


def normalize_team_gks(src_csv_path, dest_csv_path, scouted_gks_by_uid):
    """Normalizes team_gks.csv into standard 30-column format."""
    header, rows = read_csv_rows(src_csv_path)
    if not rows:
        return 0

    normalized_rows = []

    # Detect if team_gks.csv was exported using the 57-column outfield squad view
    is_outfield_export = len(header) >= 50 or "Gls" in header

    for r in rows:
        uid = r.get("UID", "").strip()
        if not uid:
            continue

        # If Carlos Troya or another squad GK is present in scouted_gks, merge full GK metrics
        match = scouted_gks_by_uid.get(uid)

        if match:
            row = [
                uid,
                r.get("Player", match.get("Name", "")).strip(),
                match.get("Best Pos", "GK").strip(),
                match.get("Age", r.get("Age", "")).strip(),
                match.get("Club", r.get("Club", "")).strip(),
                match.get("Division", r.get("Division", "")).strip(),
                match.get("Mins", r.get("Mins", "0")).strip(),
                match.get("Tgls/90", r.get("Tgls/90", "-")).strip(),
                match.get("Tcon/90", r.get("Tcon/90", "-")).strip(),
                match.get("Con/90", "-").strip(),
                match.get("Saves/90", "-").strip(),
                match.get("xGP/90", "-").strip(),
                match.get("xSv %", "-").strip(),
                match.get("Svt", "-").strip(),
                match.get("Svp", "-").strip(),
                match.get("Svh", "-").strip(),
                match.get("Sv %", "-").strip(),
                match.get("Ps A/90", r.get("Ps A/90", "-")).strip(),
                match.get("Pas %", r.get("Pas %", "-")).strip(),
                match.get("Poss Won/90", r.get("Poss Won/90", "-")).strip(),
                match.get("Poss Lost/90", r.get("Poss Lost/90", "-")).strip(),
                match.get("Int/90", r.get("Int/90", "-")).strip(),
                match.get("Clr/90", r.get("Clr/90", "-")).strip(),
                match.get("Pens Faced", "-").strip(),
                match.get("Pens Saved Ratio", "-").strip(),
                match.get("Dist/90", r.get("Dist/90", "-")).strip(),
                match.get("Gl Mst", r.get("Gl Mst", "-")).strip(),
                match.get("FA", r.get("FA", "-")).strip(),
                match.get("Wage", r.get("Wage", "-")).strip(),
                match.get("Transfer Value", r.get("Transfer Value", "-")).strip(),
            ]
        elif is_outfield_export:
            # Squad GK exported with outfield view (e.g. backup GK)
            row = [
                uid,
                r.get("Player", "").strip(),
                "GK",
                r.get("Age", "").strip(),
                r.get("Club", "").strip(),
                r.get("Division", "").strip(),
                r.get("Mins", "0").strip(),
                r.get("Tgls/90", "-").strip(),
                r.get("Tcon/90", "-").strip(),
                "-", "-", "-", "-", "-", "-", "-", "-",  # GK-specific metrics default
                r.get("Ps A/90", "-").strip(),
                r.get("Pas %", "-").strip(),
                r.get("Poss Won/90", "-").strip(),
                r.get("Poss Lost/90", "-").strip(),
                r.get("Int/90", "-").strip(),
                r.get("Clr/90", "-").strip(),
                "-", "-",  # Pens
                r.get("Dist/90", "-").strip(),
                r.get("Gl Mst", "-").strip(),
                r.get("FA", "-").strip(),
                r.get("Wage", "-").strip(),
                r.get("Transfer Value", "-").strip(),
            ]
        else:
            # Already a native 30-column GK export
            row = [r.get(c, "-") if r.get(c, "") != "" else "-" for c in EXPECTED_TEAM_GKS_COLS]

        normalized_rows.append(row)

    with open(dest_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(EXPECTED_TEAM_GKS_COLS)
        writer.writerows(normalized_rows)

    return len(normalized_rows)


def normalize_team_players(src_csv_path, dest_csv_path):
    """Transfers and standardizes team_players.csv to target directory."""
    header, rows = read_csv_rows(src_csv_path)
    if not rows:
        return 0

    with open(dest_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            writer.writerow([r.get(c, "") for c in header])

    return len(rows)


def normalize_outfield_partitions(src_dir, dest_dir):
    """
    Copies and standardizes all interested_out_players*.csv partitions,
    deduplicating any duplicate player UIDs across overlapping partitions.
    """
    pattern = os.path.join(src_dir, "interested_out_players*.csv")
    partition_files = sorted(glob.glob(pattern), key=lambda x: (
        int(os.path.basename(x).replace("interested_out_players", "").replace(".csv", "") or 0)
    ))

    seen_uids = set()
    total_written = 0
    partitions_processed = 0

    # Clean up any preexisting partition files in dest_dir to prevent stale partition leakage
    if os.path.abspath(src_dir) != os.path.abspath(dest_dir):
        for old_part in glob.glob(os.path.join(dest_dir, "interested_out_players*.csv")):
            try:
                os.remove(old_part)
            except OSError:
                pass

    for p_file in partition_files:
        base_name = os.path.basename(p_file)
        dest_file = os.path.join(dest_dir, base_name)
        header, rows = read_csv_rows(p_file)
        if not rows:
            continue

        clean_rows = []
        for r in rows:
            uid = r.get("UID", "").strip()
            if uid and uid in seen_uids:
                continue
            if uid:
                seen_uids.add(uid)
            clean_rows.append([r.get(c, "") for c in header])

        with open(dest_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(clean_rows)

        total_written += len(clean_rows)
        partitions_processed += 1
        print(f"    - Partition {base_name}: {len(clean_rows):,} players (deduplicated)")

    return partitions_processed, total_written


def resolve_source_directory(cli_source=None, season=None):
    """Resolves source directory with smart fallback and season subfolder support."""
    base_dir = None
    if cli_source and os.path.isdir(cli_source):
        base_dir = os.path.abspath(cli_source)
    else:
        env_dir = os.environ.get("FM_DATA_DIR")
        if env_dir and os.path.isdir(env_dir):
            base_dir = os.path.abspath(env_dir)
        elif os.path.exists(".env"):
            with open(".env", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("FM_DATA_DIR="):
                        val = line.strip().split("=", 1)[1].strip("'\"")
                        if os.path.isdir(val):
                            base_dir = os.path.abspath(val)
                            break
        if not base_dir and os.path.isdir(DEFAULT_STEAM_EXPORT_DIR):
            base_dir = os.path.abspath(DEFAULT_STEAM_EXPORT_DIR)

    if not base_dir:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        doc_dir = os.path.join(project_root, "documents")
        if os.path.isdir(doc_dir):
            return doc_dir
        return None

    # If season is specified, check for season-specific subdirectories
    if season and os.path.isdir(base_dir):
        s_clean = season.replace("/", "-").replace("\\", "-").strip()
        candidates = [s_clean, s_clean.replace("-", "_")]
        parts = s_clean.split("-")
        if len(parts) == 2:
            p1, p2 = parts[0], parts[1]
            short1 = p1[-2:] if len(p1) >= 2 else p1
            short2 = p2[-2:] if len(p2) >= 2 else p2
            candidates.extend([f"{short1}-{short2}", f"{short1}_{short2}", f"20{short1}-20{short2}"])

        for cand in candidates:
            sub = os.path.join(base_dir, cand)
            if os.path.isdir(sub):
                # If the subdirectory contains a zip file but no CSV/HTML, automatically unzip
                zip_files = glob.glob(os.path.join(sub, "*.zip"))
                csv_files = glob.glob(os.path.join(sub, "*.csv"))
                html_files = glob.glob(os.path.join(sub, "*.html"))
                if zip_files and not csv_files and not html_files:
                    import zipfile
                    for z in zip_files:
                        with zipfile.ZipFile(z, 'r') as zf:
                            zf.extractall(sub)
                return os.path.abspath(sub)

    return os.path.abspath(base_dir)


def main():
    parser = argparse.ArgumentParser(
        description="Football Manager 2024 Data Ingestion Pre-processor & Normalizer."
    )
    parser.add_argument("--source-dir", "--src", dest="source_dir", default=None,
                        help="Path to directory containing FM24 exports.")
    parser.add_argument("--target-dir", "--dest", dest="target_dir", default="documents",
                        help="Target directory where standardized CSVs will be output.")
    parser.add_argument("-s", "--season", dest="season", default=None,
                        help="Target season (e.g. '2023/24' or '2024/25') to auto-detect season folders.")

    args = parser.parse_args()

    src_dir = resolve_source_directory(args.source_dir, args.season)
    if not src_dir:
        print("[ERROR] Could not locate FM24 exports directory.")
        sys.exit(1)

    dest_dir = os.path.abspath(args.target_dir)
    os.makedirs(dest_dir, exist_ok=True)

    print("======================================================================")
    print("⚽ FM24 DATA INGESTION PREPARATION & SYNCHRONIZATION")
    print("======================================================================")
    print(f"Source Directory : {src_dir}")
    print(f"Target Directory : {dest_dir}")
    print("----------------------------------------------------------------------")

    # Step 1: Auto-convert any HTML files if CSVs missing
    html_files = glob.glob(os.path.join(src_dir, "*.html"))
    if html_files:
        print(f">>> Found {len(html_files)} HTML export(s). Checking conversion status...")
        for h_file in html_files:
            c_file = os.path.splitext(h_file)[0] + ".csv"
            convert_html_to_csv_if_needed(h_file, c_file)

    # Step 2: Normalize interested_gks.csv
    src_int_gks = os.path.join(src_dir, "interested_gks.csv")
    dest_int_gks = os.path.join(dest_dir, "interested_gks.csv")
    scouted_gks = {}
    if os.path.exists(src_int_gks):
        count, scouted_gks = normalize_interested_gks(src_int_gks, dest_int_gks)
        print(f"[OK] interested_gks.csv: {count:,} goalkeepers normalized (32 columns)")
    else:
        print(f"[WARNING] interested_gks.csv not found in {src_dir}")

    # Step 3: Normalize team_gks.csv
    src_team_gks = os.path.join(src_dir, "team_gks.csv")
    dest_team_gks = os.path.join(dest_dir, "team_gks.csv")
    if os.path.exists(src_team_gks):
        count = normalize_team_gks(src_team_gks, dest_team_gks, scouted_gks)
        print(f"[OK] team_gks.csv: {count:,} squad goalkeepers normalized (30 columns)")
    else:
        print(f"[WARNING] team_gks.csv not found in {src_dir}")

    # Step 4: Normalize team_players.csv
    src_team_players = os.path.join(src_dir, "team_players.csv")
    dest_team_players = os.path.join(dest_dir, "team_players.csv")
    if os.path.exists(src_team_players):
        count = normalize_team_players(src_team_players, dest_team_players)
        print(f"[OK] team_players.csv: {count:,} squad outfield players normalized (57 columns)")
    else:
        print(f"[WARNING] team_players.csv not found in {src_dir}")

    # Step 5: Normalize and deduplicate interested_out_players*.csv partitions
    parts_count, total_players = normalize_outfield_partitions(src_dir, dest_dir)
    print(f"[OK] interested_out_players: {parts_count} partitions, {total_players:,} unique players")

    print("----------------------------------------------------------------------")
    print(f"🎉 Successfully prepared all FM24 exports in {dest_dir}!")
    print("======================================================================")


if __name__ == "__main__":
    main()
