#!/usr/bin/env python3
"""
Rebase all Taiwan HPI price series from 2012Q3 = 1.00 to 2001Q1 = 1.00.

For each city column in cll_quarterly_index.csv, divides every value by the
column's 2001Q1 value.  Hualien (missing 2001Q1) is divided by the all-Taiwan
2001Q1 divisor so the scale stays consistent.

For the comparison indices:
  - Sinyi and Cathay have 2001Q1 data → divided by their own 2001Q1 values.
  - Official and AIFE start at 2012Q3 → multiplied by (1 / all_taiwan_2001q1)
    so that their 2012Q3 value matches the new CLL value at 2012Q3.

Outputs (all in website/data/):
  cll_quarterly_index.csv   – rebased
  cll_annual_index.csv      – rebased
  sinyi_hpi.csv             – rebased
  cathay_hpi.csv            – rebased
  official_hpi.csv          – rescaled
  aife_hpi.csv              – rescaled
  comparison_indices.csv    – rebuilt
  quarterly_cll.json        – rebuilt
  annual_cll.json           – rebuilt
  comparison_indices.json   – rebuilt
  latest_stats.json         – rebuilt
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR  = Path(__file__).parent
WEBSITE_DIR = SCRIPT_DIR.parent
DATA_DIR    = WEBSITE_DIR / "data"

BASE_QUARTER = "2001q1"   # target base (lowercase, matches CSV format)
BASE_YEAR    = 2001

# ── City metadata (same as update_data.py) ───────────────────────────────────
CITY_META = {
    "all":       {"label": "Taiwan Overall",       "color": "#2c3e50"},
    "taipei":    {"label": "Taipei City",          "color": "#e74c3c"},
    "newtaipei": {"label": "New Taipei City",      "color": "#e67e22"},
    "taoyuan":   {"label": "Taoyuan City",         "color": "#d4ac0d"},
    "hsinchu":   {"label": "Hsinchu City/County",  "color": "#27ae60"},
    "taichung":  {"label": "Taichung City",        "color": "#16a085"},
    "tainan":    {"label": "Tainan City",          "color": "#2980b9"},
    "kaohsiung": {"label": "Kaohsiung City",       "color": "#8e44ad"},
    "keelung":   {"label": "Keelung City",         "color": "#566573"},
    "chiayi":    {"label": "Chiayi City/County",   "color": "#c0392b"},
    "yunlin":    {"label": "Yunlin County",        "color": "#795548"},
    "pingtung":  {"label": "Pingtung County",      "color": "#607d8b"},
    "hualien":   {"label": "Hualien County",       "color": "#00838f"},
    "taitung":   {"label": "Taitung County",       "color": "#bf360c"},
}

INDEX_META = {
    "cll":      {"label": "CL Index (This Study)", "color": "#e74c3c"},
    "official": {"label": "Official (MOI)",         "color": "#2980b9"},
    "sinyi":    {"label": "Sinyi HPI",              "color": "#27ae60"},
    "cathay":   {"label": "Cathay HPI",             "color": "#f39c12"},
    "aife":     {"label": "AIFE/NTHU HPI",          "color": "#8e44ad"},
}

# ── Step 1: rebase cll_quarterly_index.csv ───────────────────────────────────
print("=== Rebasing cll_quarterly_index.csv ===")
q_df = pd.read_csv(DATA_DIR / "cll_quarterly_index.csv")

# Find the 2001Q1 row
base_mask = q_df["date"].str.lower() == BASE_QUARTER
if not base_mask.any():
    raise ValueError(f"No row found for {BASE_QUARTER} in cll_quarterly_index.csv")

# Extract all-Taiwan 2001Q1 value as fallback for missing columns
all_divisor = float(q_df.loc[base_mask, "rsfull_all"].iloc[0])
print(f"  all-Taiwan 2001Q1 divisor: {all_divisor:.8f}")

for col in q_df.columns:
    if not col.startswith("rsfull_"):
        continue
    base_val = q_df.loc[base_mask, col].iloc[0]
    if pd.isna(base_val):
        # Use all-Taiwan divisor for regions missing 2001Q1 data
        divisor = all_divisor
        print(f"  {col}: missing 2001Q1 → using all-Taiwan divisor {divisor:.8f}")
    else:
        divisor = float(base_val)
        print(f"  {col}: 2001Q1 = {divisor:.8f}")
    q_df[col] = q_df[col] / divisor

q_df.to_csv(DATA_DIR / "cll_quarterly_index.csv", index=False)
print("  ✓ Saved cll_quarterly_index.csv\n")

# ── Step 2: rebase cll_annual_index.csv ──────────────────────────────────────
print("=== Rebasing cll_annual_index.csv ===")
a_df = pd.read_csv(DATA_DIR / "cll_annual_index.csv")

base_mask_a = a_df["year"].astype(int) == BASE_YEAR
if not base_mask_a.any():
    raise ValueError(f"No row found for year {BASE_YEAR} in cll_annual_index.csv")

all_divisor_a = float(a_df.loc[base_mask_a, "rsfull_all"].iloc[0])
print(f"  all-Taiwan {BASE_YEAR} divisor: {all_divisor_a:.8f}")

for col in a_df.columns:
    if not col.startswith("rsfull_"):
        continue
    base_val = a_df.loc[base_mask_a, col].iloc[0]
    if pd.isna(base_val):
        divisor = all_divisor_a
        print(f"  {col}: missing {BASE_YEAR} → using all-Taiwan divisor {divisor:.8f}")
    else:
        divisor = float(base_val)
        print(f"  {col}: {BASE_YEAR} = {divisor:.8f}")
    a_df[col] = a_df[col] / divisor

a_df.to_csv(DATA_DIR / "cll_annual_index.csv", index=False)
print("  ✓ Saved cll_annual_index.csv\n")

# ── Step 3: rebase sinyi_hpi.csv ─────────────────────────────────────────────
print("=== Rebasing sinyi_hpi.csv ===")
sinyi_df = pd.read_csv(DATA_DIR / "sinyi_hpi.csv")
sinyi_df["quarter"] = sinyi_df["quarter"].str.upper()
base_mask_s = sinyi_df["quarter"] == "2001Q1"
sinyi_divisor = float(sinyi_df.loc[base_mask_s, "sinyi_all"].iloc[0])
print(f"  Sinyi 2001Q1 = {sinyi_divisor:.8f}")
sinyi_df["sinyi_all"] = sinyi_df["sinyi_all"] / sinyi_divisor
sinyi_df.to_csv(DATA_DIR / "sinyi_hpi.csv", index=False)
print("  ✓ Saved sinyi_hpi.csv\n")

# ── Step 4: rebase cathay_hpi.csv ────────────────────────────────────────────
print("=== Rebasing cathay_hpi.csv ===")
cathay_df = pd.read_csv(DATA_DIR / "cathay_hpi.csv")
cathay_df["quarter"] = cathay_df["quarter"].str.upper()
base_mask_c = cathay_df["quarter"] == "2001Q1"
cathay_divisor = float(cathay_df.loc[base_mask_c, "cathay_all"].iloc[0])
print(f"  Cathay 2001Q1 = {cathay_divisor:.8f}")
cathay_df["cathay_all"] = cathay_df["cathay_all"] / cathay_divisor
cathay_df.to_csv(DATA_DIR / "cathay_hpi.csv", index=False)
print("  ✓ Saved cathay_hpi.csv\n")

# ── Step 5: rescale official_hpi.csv ─────────────────────────────────────────
# Official starts at 2012Q3, so we scale it so its 2012Q3 value = new CLL 2012Q3.
# New CLL 2012Q3 = old CLL 2012Q3 / all_divisor = 1.0 / all_divisor
print("=== Rescaling official_hpi.csv ===")
official_df = pd.read_csv(DATA_DIR / "official_hpi.csv")
official_df["quarter"] = official_df["quarter"].str.upper()
new_2012q3 = 1.0 / all_divisor   # CLL at 2012Q3 under new base
print(f"  Scaling factor (1/all_divisor = new CLL at 2012Q3): {new_2012q3:.8f}")
official_df["govt_all"] = official_df["govt_all"] * new_2012q3
official_df.to_csv(DATA_DIR / "official_hpi.csv", index=False)
print("  ✓ Saved official_hpi.csv\n")

# ── Step 6: rescale aife_hpi.csv ─────────────────────────────────────────────
print("=== Rescaling aife_hpi.csv ===")
aife_df = pd.read_csv(DATA_DIR / "aife_hpi.csv")
aife_df["quarter"] = aife_df["quarter"].str.upper()
print(f"  Scaling factor: {new_2012q3:.8f}")
aife_df["aife_all"] = aife_df["aife_all"] * new_2012q3
aife_df.to_csv(DATA_DIR / "aife_hpi.csv", index=False)
print("  ✓ Saved aife_hpi.csv\n")

# ── Step 7: rebuild comparison_indices.csv ───────────────────────────────────
print("=== Rebuilding comparison_indices.csv ===")

# Reload rebased files
q_df2     = pd.read_csv(DATA_DIR / "cll_quarterly_index.csv")
sinyi_df2 = pd.read_csv(DATA_DIR / "sinyi_hpi.csv")
sinyi_df2["quarter"] = sinyi_df2["quarter"].str.upper()
cathay_df2 = pd.read_csv(DATA_DIR / "cathay_hpi.csv")
cathay_df2["quarter"] = cathay_df2["quarter"].str.upper()
official_df2 = pd.read_csv(DATA_DIR / "official_hpi.csv")
official_df2["quarter"] = official_df2["quarter"].str.upper()
aife_df2 = pd.read_csv(DATA_DIR / "aife_hpi.csv")
aife_df2["quarter"] = aife_df2["quarter"].str.upper()

# Build merged dict
comp = {}
for _, row in q_df2.iterrows():
    q = row["date"].upper()
    val = row["rsfull_all"]
    comp.setdefault(q, {})["cll"] = round(float(val), 6) if pd.notna(val) else None

for key, df, col in [
    ("sinyi",    sinyi_df2,    "sinyi_all"),
    ("cathay",   cathay_df2,   "cathay_all"),
    ("official", official_df2, "govt_all"),
    ("aife",     aife_df2,     "aife_all"),
]:
    for _, row in df.iterrows():
        q = row["quarter"]
        val = row[col]
        comp.setdefault(q, {})[key] = round(float(val), 6) if pd.notna(val) else None

all_quarters = sorted(comp.keys())
rows = {"quarter": all_quarters}
for key in ["cll", "official", "sinyi", "cathay", "aife"]:
    rows[key] = [comp[q].get(key) for q in all_quarters]
comp_df = pd.DataFrame(rows)
comp_df.to_csv(DATA_DIR / "comparison_indices.csv", index=False)
print("  ✓ Saved comparison_indices.csv\n")

# ── Step 8: rebuild quarterly_cll.json ───────────────────────────────────────
print("=== Rebuilding quarterly_cll.json ===")
q_json = {"quarters": q_df2["date"].str.upper().tolist(), "cities": {}}
for col in q_df2.columns:
    if not col.startswith("rsfull_"):
        continue
    key = col.replace("rsfull_", "")
    meta = CITY_META.get(key)
    if meta is None:
        continue
    vals = q_df2[col].tolist()
    q_json["cities"][key] = {
        "label": meta["label"],
        "color": meta["color"],
        "data":  [round(float(v), 6) if pd.notna(v) else None for v in vals],
    }
with open(DATA_DIR / "quarterly_cll.json", "w", encoding="utf-8") as f:
    json.dump(q_json, f, ensure_ascii=False)
print("  ✓ Saved quarterly_cll.json\n")

# ── Step 9: rebuild annual_cll.json ──────────────────────────────────────────
print("=== Rebuilding annual_cll.json ===")
a_json = {"years": a_df["year"].astype(int).tolist(), "cities": {}}
for col in a_df.columns:
    if not col.startswith("rsfull_"):
        continue
    key = col.replace("rsfull_", "")
    meta = CITY_META.get(key)
    if meta is None:
        continue
    vals = a_df[col].tolist()
    a_json["cities"][key] = {
        "label": meta["label"],
        "color": meta["color"],
        "data":  [round(float(v), 6) if pd.notna(v) else None for v in vals],
    }
with open(DATA_DIR / "annual_cll.json", "w", encoding="utf-8") as f:
    json.dump(a_json, f, ensure_ascii=False)
print("  ✓ Saved annual_cll.json\n")

# ── Step 10: rebuild comparison_indices.json ─────────────────────────────────
print("=== Rebuilding comparison_indices.json ===")
comp_json = {"quarters": all_quarters, "indices": {}}
for key, meta in INDEX_META.items():
    comp_json["indices"][key] = {
        "label": meta["label"],
        "color": meta["color"],
        "data":  [comp[q].get(key) for q in all_quarters],
    }
with open(DATA_DIR / "comparison_indices.json", "w", encoding="utf-8") as f:
    json.dump(comp_json, f, ensure_ascii=False)
print("  ✓ Saved comparison_indices.json\n")

# ── Step 11: rebuild latest_stats.json ───────────────────────────────────────
print("=== Rebuilding latest_stats.json ===")
quarters = q_json["quarters"]
stats = {}
for key, meta in q_json["cities"].items():
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
with open(DATA_DIR / "latest_stats.json", "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)
print("  ✓ Saved latest_stats.json\n")

print("=" * 60)
print("Done. All price series rebased to 2001Q1 = 1.00.")
print(f"  New CLL value at 2012Q3 ≈ {new_2012q3:.4f}")
print(f"  (was 1.0000 under old 2012Q3 base)")
