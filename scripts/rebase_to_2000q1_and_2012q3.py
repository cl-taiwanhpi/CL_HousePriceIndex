#!/usr/bin/env python3
"""
Rebase Taiwan HPI website data:
  - cll_quarterly_index.csv / cll_annual_index.csv   → 2000Q1 / 2000 = 1.00
  - quarterly_cll.json / annual_cll.json / latest_stats.json → same
  - comparison_indices.csv / comparison_indices.json  → all indices 2012Q3 = 1.00
  - sinyi_hpi.csv / cathay_hpi.csv / official_hpi.csv / aife_hpi.csv → 2012Q3 = 1.00

Starting state: all CLL files at 2001Q1 = 1.00; comparison files at 2001Q1 scale.
"""

import json
from pathlib import Path
import pandas as pd

SCRIPT_DIR  = Path(__file__).parent
DATA_DIR    = SCRIPT_DIR.parent / "data"

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


# ── Step 1: Rebase cll_quarterly_index.csv to 2000Q1 = 1.00 ─────────────────
print("=== Rebasing cll_quarterly_index.csv → 2000Q1 = 1.00 ===")
q_df = pd.read_csv(DATA_DIR / "cll_quarterly_index.csv")

base_mask = q_df["date"].str.lower() == "2000q1"
if not base_mask.any():
    raise ValueError("No 2000Q1 row found in cll_quarterly_index.csv")

# Use all-Taiwan 2000Q1 as fallback for any column missing that value
all_divisor_q = float(q_df.loc[base_mask, "rsfull_all"].iloc[0])
print(f"  all-Taiwan 2000Q1 divisor: {all_divisor_q:.8f}")

for col in q_df.columns:
    if not col.startswith("rsfull_"):
        continue
    bv = q_df.loc[base_mask, col].iloc[0]
    divisor = all_divisor_q if pd.isna(bv) else float(bv)
    if pd.isna(bv):
        print(f"  {col}: missing 2000Q1 → using all-Taiwan divisor {divisor:.8f}")
    else:
        print(f"  {col}: 2000Q1 = {divisor:.8f}")
    q_df[col] = q_df[col] / divisor

q_df.to_csv(DATA_DIR / "cll_quarterly_index.csv", index=False)
print("  ✓ Saved cll_quarterly_index.csv\n")


# ── Step 2: Rebase cll_annual_index.csv to 2000 = 1.00 ──────────────────────
print("=== Rebasing cll_annual_index.csv → 2000 = 1.00 ===")
a_df = pd.read_csv(DATA_DIR / "cll_annual_index.csv")

base_mask_a = a_df["year"].astype(int) == 2000
if not base_mask_a.any():
    raise ValueError("No year-2000 row found in cll_annual_index.csv")

all_divisor_a = float(a_df.loc[base_mask_a, "rsfull_all"].iloc[0])
print(f"  all-Taiwan 2000 divisor: {all_divisor_a:.8f}")

for col in a_df.columns:
    if not col.startswith("rsfull_"):
        continue
    bv = a_df.loc[base_mask_a, col].iloc[0]
    divisor = all_divisor_a if pd.isna(bv) else float(bv)
    if pd.isna(bv):
        print(f"  {col}: missing 2000 → using all-Taiwan divisor {divisor:.8f}")
    a_df[col] = a_df[col] / divisor

a_df.to_csv(DATA_DIR / "cll_annual_index.csv", index=False)
print("  ✓ Saved cll_annual_index.csv\n")


# ── Step 3: Rebuild quarterly_cll.json ──────────────────────────────────────
print("=== Rebuilding quarterly_cll.json ===")
q_json = {"quarters": q_df["date"].str.upper().tolist(), "cities": {}}
for col in q_df.columns:
    if not col.startswith("rsfull_"):
        continue
    key = col.replace("rsfull_", "")
    meta = CITY_META.get(key)
    if meta is None:
        continue
    vals = q_df[col].tolist()
    q_json["cities"][key] = {
        "label": meta["label"],
        "color": meta["color"],
        "data":  [round(float(v), 6) if pd.notna(v) else None for v in vals],
    }
with open(DATA_DIR / "quarterly_cll.json", "w", encoding="utf-8") as f:
    json.dump(q_json, f, ensure_ascii=False)
print("  ✓ Saved quarterly_cll.json\n")


# ── Step 4: Rebuild annual_cll.json ─────────────────────────────────────────
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


# ── Step 5: Rebuild latest_stats.json ───────────────────────────────────────
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


# ── Step 6: Rebase comparison indices back to 2012Q3 = 1.00 ─────────────────

# -- sinyi: divide by current 2012Q3 value
print("=== Rebasing sinyi_hpi.csv → 2012Q3 = 1.00 ===")
sinyi_df = pd.read_csv(DATA_DIR / "sinyi_hpi.csv")
sinyi_df["quarter"] = sinyi_df["quarter"].str.upper()
sinyi_2012q3 = float(sinyi_df.loc[sinyi_df["quarter"] == "2012Q3", "sinyi_all"].iloc[0])
print(f"  Current sinyi 2012Q3 = {sinyi_2012q3:.8f}")
sinyi_df["sinyi_all"] = sinyi_df["sinyi_all"] / sinyi_2012q3
sinyi_df.to_csv(DATA_DIR / "sinyi_hpi.csv", index=False)
print("  ✓ Saved sinyi_hpi.csv\n")

# -- cathay: divide by current 2012Q3 value
print("=== Rebasing cathay_hpi.csv → 2012Q3 = 1.00 ===")
cathay_df = pd.read_csv(DATA_DIR / "cathay_hpi.csv")
cathay_df["quarter"] = cathay_df["quarter"].str.upper()
cathay_2012q3 = float(cathay_df.loc[cathay_df["quarter"] == "2012Q3", "cathay_all"].iloc[0])
print(f"  Current cathay 2012Q3 = {cathay_2012q3:.8f}")
cathay_df["cathay_all"] = cathay_df["cathay_all"] / cathay_2012q3
cathay_df.to_csv(DATA_DIR / "cathay_hpi.csv", index=False)
print("  ✓ Saved cathay_hpi.csv\n")

# -- official: divide by current 2012Q3 value
print("=== Rebasing official_hpi.csv → 2012Q3 = 1.00 ===")
official_df = pd.read_csv(DATA_DIR / "official_hpi.csv")
official_df["quarter"] = official_df["quarter"].str.upper()
official_2012q3 = float(official_df.loc[official_df["quarter"] == "2012Q3", "govt_all"].iloc[0])
print(f"  Current official 2012Q3 = {official_2012q3:.8f}")
official_df["govt_all"] = official_df["govt_all"] / official_2012q3
official_df.to_csv(DATA_DIR / "official_hpi.csv", index=False)
print("  ✓ Saved official_hpi.csv\n")

# -- aife: divide by current 2012Q3 value
print("=== Rebasing aife_hpi.csv → 2012Q3 = 1.00 ===")
aife_df = pd.read_csv(DATA_DIR / "aife_hpi.csv")
aife_df["quarter"] = aife_df["quarter"].str.upper()
aife_2012q3 = float(aife_df.loc[aife_df["quarter"] == "2012Q3", "aife_all"].iloc[0])
print(f"  Current aife 2012Q3 = {aife_2012q3:.8f}")
aife_df["aife_all"] = aife_df["aife_all"] / aife_2012q3
aife_df.to_csv(DATA_DIR / "aife_hpi.csv", index=False)
print("  ✓ Saved aife_hpi.csv\n")


# ── Step 7: Rebuild comparison_indices.csv and comparison_indices.json ───────
# CLL is taken from cll_quarterly_index.csv (now at 2000Q1=1) but needs to be
# normalised to 2012Q3=1.00 for the comparison chart.
print("=== Rebuilding comparison_indices (2012Q3 = 1.00) ===")

# Find CLL 2012Q3 value under the new (2000Q1) base
cll_2012q3 = float(q_df.loc[q_df["date"].str.lower() == "2012q3", "rsfull_all"].iloc[0])
print(f"  CLL 2012Q3 under 2000Q1 base = {cll_2012q3:.8f}  → divisor for comparison")

comp = {}

# CLL: normalise to 2012Q3 = 1.00
for _, row in q_df.iterrows():
    q = row["date"].upper()
    val = row["rsfull_all"]
    comp.setdefault(q, {})["cll"] = (
        round(float(val) / cll_2012q3, 6) if pd.notna(val) else None
    )

# Other indices (already at 2012Q3 = 1.00 after steps 6a-d)
for key, df, col in [
    ("sinyi",    sinyi_df,    "sinyi_all"),
    ("cathay",   cathay_df,   "cathay_all"),
    ("official", official_df, "govt_all"),
    ("aife",     aife_df,     "aife_all"),
]:
    # Reload to pick up the just-saved normalised values
    df2 = pd.read_csv(DATA_DIR / f"{key}_hpi.csv")
    col2 = col if col in df2.columns else df2.columns[1]
    for _, row in df2.iterrows():
        q = row["quarter"].upper()
        val = row[col2]
        comp.setdefault(q, {})[key] = (
            round(float(val), 6) if pd.notna(val) else None
        )

all_quarters = sorted(comp.keys())

# CSV
rows = {"quarter": all_quarters}
for key in ["cll", "official", "sinyi", "cathay", "aife"]:
    rows[key] = [comp[q].get(key) for q in all_quarters]
pd.DataFrame(rows).to_csv(DATA_DIR / "comparison_indices.csv", index=False)
print("  ✓ Saved comparison_indices.csv")

# JSON
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


# ── Summary ──────────────────────────────────────────────────────────────────
print("=" * 60)
print("Done.")
print(f"  CLL 2000Q1 = 1.0000  (main pages base)")
print(f"  CLL 2012Q3 ≈ {cll_2012q3:.4f}  (under 2000Q1 base)")
print(f"  All comparison indices: 2012Q3 = 1.0000")
