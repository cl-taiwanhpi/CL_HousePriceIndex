#!/usr/bin/env python3
"""
Taiwan Housing Price Index – Quarterly Data Update Script
==========================================================
Run this script after each quarterly update of the CL index to refresh
the website data files.

Requirements:
    pip install pandas pyreadstat requests beautifulsoup4

Usage:
    python scripts/update_data.py [--cll-quarterly PATH] [--cll-annual PATH]

The script will:
  1. Re-read the CL quarterly/annual index CSV (or DTA) files
  2. Attempt to fetch updated comparison indices from public sources
  3. Rewrite all JSON/CSV data files in website/data/
  4. Print a summary of what changed

Comparison indices that require manual update (see instructions at bottom):
  - Sinyi HPI   → https://www.sinyinews.com.tw/quarterly
  - Cathay HPI  → https://www.cathay-red.com.tw/tw/About/House
  - AIFE HPI    → https://aife.site.nthu.edu.tw
  - Official    → https://pip.moi.gov.tw/Publicize/Info/E2010
"""

import argparse
import json
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
WEBSITE_DIR  = SCRIPT_DIR.parent
DATA_DIR     = WEBSITE_DIR / "data"
PROJECT_ROOT = WEBSITE_DIR.parent                        # Taiwan Price Index_claude/
RS_DIR       = PROJECT_ROOT / "Repeat_Sales"

# ── City metadata ─────────────────────────────────────────────────────────────
CITY_META = {
    "all":       {"label": "Taiwan Overall",  "color": "#2c3e50"},
    "taipei":    {"label": "Taipei City",     "color": "#e74c3c"},
    "newtaipei": {"label": "New Taipei City", "color": "#e67e22"},
    "taoyuan":   {"label": "Taoyuan City",    "color": "#d4ac0d"},
    "hsinchu":   {"label": "Hsinchu City/County", "color": "#27ae60"},
    "taichung":  {"label": "Taichung City",       "color": "#16a085"},
    "tainan":    {"label": "Tainan City",         "color": "#2980b9"},
    "kaohsiung": {"label": "Kaohsiung City",      "color": "#8e44ad"},
    "keelung":   {"label": "Keelung City",        "color": "#566573"},
    "chiayi":    {"label": "Chiayi City/County",  "color": "#c0392b"},
    "yunlin":    {"label": "Yunlin County",       "color": "#795548"},
    "pingtung":  {"label": "Pingtung County", "color": "#607d8b"},
    "hualien":   {"label": "Hualien County",  "color": "#00838f"},
    "taitung":   {"label": "Taitung County",  "color": "#bf360c"},
}


# ── Helper: read CL index file (CSV or DTA) ──────────────────────────────────
def read_cll(path: Path) -> pd.DataFrame:
    """Return a DataFrame from a CSV or Stata DTA file."""
    if path.suffix.lower() == ".dta":
        return pd.read_stata(path)
    return pd.read_csv(path)


# ── Build quarterly JSON ──────────────────────────────────────────────────────
def build_quarterly_json(df: pd.DataFrame) -> dict:
    quarters = df["date"].str.upper().tolist()
    cities = {}
    for col in df.columns:
        if not col.startswith("rsfull_"):
            continue
        key = col.replace("rsfull_", "")
        meta = CITY_META.get(key)
        if meta is None:
            continue
        vals = df[col].tolist()
        cities[key] = {
            "label": meta["label"],
            "color": meta["color"],
            "data": [round(float(v), 6) if pd.notna(v) else None for v in vals],
        }
    return {"quarters": quarters, "cities": cities}


# ── Build annual JSON ─────────────────────────────────────────────────────────
def build_annual_json(df: pd.DataFrame) -> dict:
    years = df["year"].astype(int).tolist()
    cities = {}
    for col in df.columns:
        if not col.startswith("rsfull_"):
            continue
        key = col.replace("rsfull_", "")
        meta = CITY_META.get(key)
        if meta is None:
            continue
        vals = df[col].tolist()
        cities[key] = {
            "label": meta["label"],
            "color": meta["color"],
            "data": [round(float(v), 6) if pd.notna(v) else None for v in vals],
        }
    return {"years": years, "cities": cities}


# ── Build comparison JSON ─────────────────────────────────────────────────────
def build_comparison_json(quarterly_df: pd.DataFrame) -> dict:
    """
    Merge the CL aggregate series with the four comparison indices.
    Comparison DTA files are read from Repeat_Sales/ if available.
    """
    INDEX_META = {
        "cll":      {"label": "CL Index (This Study)", "color": "#e74c3c"},
        "official": {"label": "Official (MOI)",          "color": "#2980b9"},
        "sinyi":    {"label": "Sinyi HPI",               "color": "#27ae60"},
        "cathay":   {"label": "Cathay HPI",              "color": "#f39c12"},
        "aife":     {"label": "AIFE/NTHU HPI",           "color": "#8e44ad"},
    }

    def to_quarter(dt):
        return pd.to_datetime(dt).to_period("Q").strftime("%YQ%q").upper()

    comp: dict[str, dict] = {}

    # CL aggregate
    for _, row in quarterly_df.iterrows():
        q = row["date"].upper()
        comp.setdefault(q, {})["cll"] = (
            round(float(row["rsfull_all"]), 6) if pd.notna(row["rsfull_all"]) else None
        )

    # Comparison indices
    COMP_FILES = {
        "sinyi":    (RS_DIR / "sinyi_hpi.dta",    "sinyi_all"),
        "cathay":   (RS_DIR / "cathay_hpi.dta",   "cathay_all"),
        "aife":     (RS_DIR / "aife_hpi.dta",     "aife_all"),
        "official": (RS_DIR / "official_hpi.dta", "govt_all"),
    }
    for key, (fpath, col) in COMP_FILES.items():
        # Also check for CSV override in data/ folder
        csv_override = DATA_DIR / f"{key}_hpi.csv"
        if csv_override.exists():
            df = pd.read_csv(csv_override)
            df["quarter"] = df["quarter"].str.upper()
        elif fpath.exists():
            df = pd.read_stata(fpath)
            df["quarter"] = df["date"].apply(to_quarter)
        else:
            print(f"  [WARN] {key} source not found, skipping.")
            continue

        for _, row in df.iterrows():
            q = row["quarter"]
            comp.setdefault(q, {})[key] = (
                round(float(row[col]), 6) if pd.notna(row.get(col)) else None
            )

    all_quarters = sorted(comp.keys())
    indices = {}
    for key, meta in INDEX_META.items():
        indices[key] = {
            "label": meta["label"],
            "color": meta["color"],
            "data":  [comp[q].get(key) for q in all_quarters],
        }

    return {"quarters": all_quarters, "indices": indices}


# ── Build latest stats JSON ───────────────────────────────────────────────────
def build_latest_stats(quarterly_json: dict) -> dict:
    quarters = quarterly_json["quarters"]
    stats = {}
    for key, meta in quarterly_json["cities"].items():
        data = meta["data"]
        valid = [(quarters[i], data[i]) for i in range(len(data)) if data[i] is not None]
        if len(valid) < 5:
            continue
        lq, lv = valid[-1]
        _, pv   = valid[-2]
        _, yv   = valid[-5]
        stats[key] = {
            "label":      meta["label"],
            "latest_q":   lq,
            "latest_val": round(lv, 4),
            "qoq_growth": round((lv / pv - 1) * 100, 2) if pv else None,
            "yoy_growth": round((lv / yv - 1) * 100, 2) if yv else None,
        }
    return stats


# ── Write helpers ─────────────────────────────────────────────────────────────
def write_json(data, path: Path, indent=None):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    print(f"  ✓ {path.relative_to(WEBSITE_DIR)}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Update Taiwan HPI website data")
    parser.add_argument("--cll-quarterly", default=None,
                        help="Path to quarterly CL index CSV/DTA (default: auto-detect)")
    parser.add_argument("--cll-annual",    default=None,
                        help="Path to annual CL index CSV/DTA (default: auto-detect)")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)

    # Locate CL files
    q_path = Path(args.cll_quarterly) if args.cll_quarterly else (
        RS_DIR / "fulltime_index_q_nonew.csv"
        if (RS_DIR / "fulltime_index_q_nonew.csv").exists()
        else RS_DIR / "fulltime_index_q_nonew.dta"
    )
    a_path = Path(args.cll_annual) if args.cll_annual else (
        RS_DIR / "fulltime_index_a_nonew.csv"
        if (RS_DIR / "fulltime_index_a_nonew.csv").exists()
        else RS_DIR / "fulltime_index_a_nonew.dta"
    )

    if not q_path.exists():
        sys.exit(f"ERROR: quarterly CL file not found at {q_path}")
    if not a_path.exists():
        sys.exit(f"ERROR: annual CL file not found at {a_path}")

    print(f"\nReading quarterly CLL from: {q_path}")
    q_df = read_cll(q_path)
    print(f"  Rows: {len(q_df)}, latest: {q_df['date'].iloc[-1]}")

    print(f"Reading annual CLL from: {a_path}")
    a_df = read_cll(a_path)

    print("\nBuilding data files...")

    q_json = build_quarterly_json(q_df)
    write_json(q_json, DATA_DIR / "quarterly_cll.json")

    a_json = build_annual_json(a_df)
    write_json(a_json, DATA_DIR / "annual_cll.json")

    comp_json = build_comparison_json(q_df)
    write_json(comp_json, DATA_DIR / "comparison_indices.json")

    stats_json = build_latest_stats(q_json)
    write_json(stats_json, DATA_DIR / "latest_stats.json")

    # CSV copies for download
    import shutil
    shutil.copy(q_path, DATA_DIR / "cll_quarterly_index.csv")
    shutil.copy(a_path, DATA_DIR / "cll_annual_index.csv")

    # Comparison CSV
    qs = comp_json["quarters"]
    rows = {"quarter": qs}
    for key, meta in comp_json["indices"].items():
        rows[key] = meta["data"]
    pd.DataFrame(rows).to_csv(DATA_DIR / "comparison_indices.csv", index=False)
    print(f"  ✓ data/comparison_indices.csv")

    print(f"\n✅ Done. Latest quarter: {q_json['quarters'][-1]}")
    print(f"   Cities updated: {len(q_json['cities'])}")
    print(f"   Comparison indices: {list(comp_json['indices'].keys())}")

    print("""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 MANUAL STEPS for comparison indices
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 The comparison index DTA files in Repeat_Sales/ may be outdated.
 To update them:

 1. SINYI HPI
    Source: https://www.sinyinews.com.tw/quarterly
    or:     https://www.ncscre.nccu.edu.tw/taxonomy/term/8
    Action: Download the latest quarterly report, read off the
            aggregate sinyi_all value (2012Q3-normalized), and
            append rows to data/sinyi_hpi.csv:
              quarter,sinyi_all
              2023Q3,<value>
              ...

 2. CATHAY HPI
    Source: https://www.cathay-red.com.tw/tw/About/House
    Action: Download the latest quarterly PDF. The cathay_all value
            (normalized to 2012Q3=1) is in the index table.
            Append to data/cathay_hpi.csv:
              quarter,cathay_all
              2023Q3,<value>
              ...

 3. AIFE/NTHU HPI
    Source: https://aife.site.nthu.edu.tw
    Action: Download from houseplus.com.tw/reportIndex.
            Append to data/aife_hpi.csv:
              quarter,aife_all
              2023Q2,<value>
              ...

 4. OFFICIAL MOI HPI
    Source: https://pip.moi.gov.tw/Publicize/Info/E2010
    Action: Click "住宅價格指數", select quarterly, download Excel.
            Normalize to 2012Q3=1 (divide each value by the 2012Q3 value).
            Append to data/official_hpi.csv:
              quarter,govt_all
              2023Q2,<value>
              ...

 After adding the CSV files to website/data/, re-run this script.
 The script will prefer CSV overrides over the DTA files.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")


if __name__ == "__main__":
    main()
