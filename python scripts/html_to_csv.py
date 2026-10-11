"""
======================================================================
Football Manager HTML to CSV Converter & Data Ingestion Utility
======================================================================
Purpose:
    Football Manager 2024 exports scouting and squad views as Web Pages
    (*.html) or Text files via Ctrl + P. This utility parses exported
    HTML data grids and converts them into standardized, UTF-8 CSV files
    ready for Medallion Warehouse ingestion (Bronze layer) and Python analytics.

Key Features:
    1. Zero external dependencies: Built with Python's standard library
       (html.parser, csv, os, sys, argparse).
    2. Robust entity decoding: Handles &nbsp;, special accents (e.g. Ø, é, ć),
       currency symbols (£, €, $), and ranges (£5M - £8M).
    3. Multi-encoding resilience: Automatically detects UTF-8, Windows-1252,
       and ISO-8859-1 exports.
    4. Single-file and batch directory conversion: Convert individual files
       or entire directories (e.g. documents/*.html) in one command.
    5. Clean formatting: Outputs UTF-8 with BOM (utf-8-sig) for seamless
       compatibility with SQL Server BULK INSERT, Excel, and pandas.

Usage:
    # Single file conversion (auto-names output to documents/team_players.csv)
    python "python scripts/html_to_csv.py" documents/team_players.html

    # Single file conversion with explicit output path
    python "python scripts/html_to_csv.py" input.html documents/team_players.csv

    # Batch convert all HTML files in documents/ directory
    python "python scripts/html_to_csv.py" --dir documents/

    # Auto-detect and convert all HTML exports in the project's documents folder
    python "python scripts/html_to_csv.py"
======================================================================
"""

import os
import sys
import csv
import glob
import html
import argparse
from html.parser import HTMLParser


class FMHTMLTableParser(HTMLParser):
    """
    Fast, streaming HTML table parser using Python's built-in HTMLParser.
    Extracts all tabular rows and cells while sanitizing entities and tags.
    """

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
            # Unescape HTML entities (&nbsp;, &amp;, etc.) and normalize whitespace
            clean_text = html.unescape(raw_text).replace("\xa0", " ").strip()
            self._current_row.append(clean_text)
        elif tag_lower == "tr":
            if self._current_row:
                # Discard completely empty placeholder rows
                if any(cell != "" for cell in self._current_row):
                    self._current_table.append(self._current_row)
        elif tag_lower == "table":
            if self._current_table:
                self.tables.append(self._current_table)

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell.append(data)


def read_html_file(file_path, forced_encoding=None):
    """
    Reads an HTML file, testing common encodings to prevent decoding errors.
    """
    encodings_to_try = [forced_encoding] if forced_encoding else ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    for enc in encodings_to_try:
        if not enc:
            continue
        try:
            with open(file_path, mode="r", encoding=enc) as f:
                content = f.read()
                return content, enc
        except (UnicodeDecodeError, LookupError):
            continue

    # Fallback with replacement of un-decodable bytes
    with open(file_path, mode="r", encoding="utf-8", errors="replace") as f:
        return f.read(), "utf-8-replace"


def extract_primary_table(html_content):
    """
    Parses HTML content and extracts the primary data table.
    If multiple tables exist, returns the table with the largest cell count.
    """
    parser = FMHTMLTableParser()
    parser.feed(html_content)

    if not parser.tables:
        return []

    # Football Manager exports may have a metadata header table.
    # We select the table with the maximum total cell volume (rows * max_cols).
    def table_score(table):
        if not table:
            return 0
        row_count = len(table)
        col_count = max(len(r) for r in table) if row_count > 0 else 0
        return row_count * col_count

    primary_table = max(parser.tables, key=table_score)
    return primary_table


def convert_html_file(input_path, output_path=None, encoding=None):
    """
    Converts a single Football Manager HTML export to a CSV file.

    Parameters:
        input_path  (str): Path to the source HTML file.
        output_path (str): Optional path for the target CSV file.
                           Defaults to the same filename with a .csv extension.
        encoding    (str): Optional explicit encoding for reading the HTML.

    Returns:
        tuple: (success (bool), row_count (int), col_count (int), output_path (str))
    """
    if not os.path.exists(input_path):
        print(f"[ERROR] Input file does not exist: {input_path}")
        return False, 0, 0, None

    if not output_path:
        base, _ = os.path.splitext(input_path)
        output_path = base + ".csv"

    # Ensure parent output directory exists
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # Read and parse HTML
    content, detected_enc = read_html_file(input_path, forced_encoding=encoding)
    table_rows = extract_primary_table(content)

    if not table_rows:
        print(f"[WARNING] No valid table data found in: {input_path}")
        return False, 0, 0, output_path

    # Standardize column count across all rows (pad ragged rows if needed)
    max_cols = max(len(r) for r in table_rows)
    normalized_rows = [r + [""] * (max_cols - len(r)) for r in table_rows]

    # Write output CSV (using utf-8-sig for Excel and SQL Server compatibility)
    with open(output_path, mode="w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        writer.writerows(normalized_rows)

    row_count = len(normalized_rows)
    col_count = max_cols
    return True, row_count, col_count, output_path


def convert_directory(dir_path, output_dir=None, recursive=False, encoding=None):
    """
    Finds and converts all HTML / HTM files in a directory to CSV files.
    """
    if not os.path.isdir(dir_path):
        print(f"[ERROR] Directory does not exist: {dir_path}")
        return 0, 0

    pattern = os.path.join(dir_path, "**" if recursive else "", "*.*")
    candidates = glob.glob(pattern, recursive=recursive)
    html_files = [f for f in candidates if f.lower().endswith((".html", ".htm"))]

    if not html_files:
        print(f"[INFO] No .html or .htm files found in: {dir_path}")
        return 0, 0

    print(f">>> Found {len(html_files)} HTML file(s) in {dir_path}. Converting to CSV...")
    success_count = 0
    total_rows = 0

    for idx, html_path in enumerate(sorted(html_files), 1):
        filename = os.path.basename(html_path)
        base_name, _ = os.path.splitext(filename)

        if output_dir:
            target_csv = os.path.join(output_dir, base_name + ".csv")
        else:
            target_csv = os.path.splitext(html_path)[0] + ".csv"

        success, rows, cols, out_path = convert_html_file(html_path, target_csv, encoding=encoding)
        if success:
            success_count += 1
            total_rows += rows
            print(f"    [{idx}/{len(html_files)}] Converted: {filename} -> {os.path.basename(out_path)} ({rows:,} rows, {cols} cols)")
        else:
            print(f"    [{idx}/{len(html_files)}] Failed: {filename}")

    print(f"\n[SUMMARY] Successfully converted {success_count}/{len(html_files)} files ({total_rows:,} total rows).")
    return success_count, total_rows


def auto_detect_and_convert():
    """
    Auto-detection mode: Checks the project's documents/ folder for HTML exports.
    If none exist, displays helpful CLI instructions.
    """
    # Locate project root documents folder relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    documents_dir = os.path.join(project_root, "documents")

    if os.path.isdir(documents_dir):
        html_files = [f for f in os.listdir(documents_dir) if f.lower().endswith((".html", ".htm"))]
        if html_files:
            print(f">>> Auto-detected {len(html_files)} HTML export(s) in: {documents_dir}")
            convert_directory(documents_dir)
            return

    # If no files found, print guidance
    print("======================================================================")
    print("Football Manager HTML to CSV Converter")
    print("======================================================================")
    print("No HTML files found in the default documents/ folder.")
    print("\nHow to use this utility:")
    print("  1. Single file:")
    print("     python \"python scripts/html_to_csv.py\" path/to/export.html")
    print("\n  2. Custom output filename:")
    print("     python \"python scripts/html_to_csv.py\" input.html documents/team_players.csv")
    print("\n  3. Convert an entire folder:")
    print("     python \"python scripts/html_to_csv.py\" --dir documents/")
    print("\nIn Football Manager 2024:")
    print("  - Highlight rows (Ctrl + A) -> Print (Ctrl + P) -> Save as Web Page (*.html).")
    print("  - Run this script to generate clean CSV files for the warehouse.")
    print("======================================================================")


def main():
    parser = argparse.ArgumentParser(
        description="Convert Football Manager HTML web page exports (*.html) into clean CSV files for data warehouse ingestion."
    )
    parser.add_argument("input", nargs="?", default=None, help="Input HTML file path or directory.")
    parser.add_argument("output", nargs="?", default=None, help="Optional output CSV file path.")
    parser.add_argument("--dir", "-d", dest="directory", default=None, help="Directory containing HTML files to batch convert.")
    parser.add_argument("--outdir", "-o", dest="outdir", default=None, help="Target directory for converted CSV files.")
    parser.add_argument("--recursive", "-r", action="store_true", help="Recursively search subdirectories.")
    parser.add_argument("--encoding", "-e", default=None, help="Force specific text encoding (e.g. utf-8, cp1252).")

    args = parser.parse_args()

    # 1. Directory mode via --dir flag
    if args.directory:
        convert_directory(args.directory, output_dir=args.outdir, recursive=args.recursive, encoding=args.encoding)
        return

    # 2. Positional input provided
    if args.input:
        # Check if the positional argument is a directory
        if os.path.isdir(args.input):
            convert_directory(args.input, output_dir=args.output or args.outdir, recursive=args.recursive, encoding=args.encoding)
        else:
            # Single file conversion
            success, rows, cols, out_path = convert_html_file(args.input, args.output, encoding=args.encoding)
            if success:
                print(f"[SUCCESS] Converted {args.input} -> {out_path} ({rows:,} rows, {cols} columns)")
            else:
                sys.exit(1)
        return

    # 3. No arguments provided -> run auto-detection
    auto_detect_and_convert()


if __name__ == "__main__":
    main()
