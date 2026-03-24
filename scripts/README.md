# Taiwan HPI Website – Update & Replication Guide

## Repository Structure

```
website/
├── index.html           Main time-series page
├── map.html             GIS map page
├── comparison.html      Cross-index comparison page
├── methodology.html     Methodology description page
├── css/style.css        Stylesheet
├── data/
│   ├── quarterly_cll.json       CLL index (quarterly, JSON for chart)
│   ├── annual_cll.json          CLL index (annual, JSON for chart)
│   ├── comparison_indices.json  All indices merged (JSON for chart)
│   ├── latest_stats.json        Latest values & growth rates for map
│   ├── cll_quarterly_index.csv  CLL quarterly (CSV download)
│   ├── cll_annual_index.csv     CLL annual (CSV download)
│   ├── comparison_indices.csv   All indices (CSV download)
│   ├── taiwan_regions.geojson   Taiwan administrative boundaries
│   ├── sinyi_hpi.csv            Sinyi HPI manual override (optional)
│   ├── cathay_hpi.csv           Cathay HPI manual override (optional)
│   ├── aife_hpi.csv             AIFE HPI manual override (optional)
│   └── official_hpi.csv         Official MOI HPI manual override (optional)
└── scripts/
    ├── update_data.py   Quarterly update script
    └── README.md        This file
```

## Quarterly Update Workflow

### Step 1: Update the CLL Index (requires Stata)

1. Download new quarterly data from MOI:
   https://plvr.land.moi.gov.tw/DownloadOpenData

2. Back up `master/` to `master_old/`

3. Extract the new quarterly CSV files into:
   `lvr_landcsv/<YY>Q<N>/`  (e.g., `lvr_landcsv/25Q1/`)

4. Open Stata and run:
   ```stata
   do "YOURPATH/Taiwan Price Index_claude/code/run_this_code.do"
   ```
   This produces updated `Repeat_Sales/fulltime_index_q_nonew.csv`
   and `Repeat_Sales/fulltime_index_a_nonew.csv`.

5. Optionally generate plots:
   ```stata
   do "YOURPATH/Taiwan Price Index_claude/code/time_series_plot.do"
   ```

### Step 2: Update comparison indices (manual)

For each index, download the latest data and create/update a CSV override
file in `website/data/`. The update script reads these CSV files in preference
to the original DTA files.

#### Sinyi HPI
- URL: https://www.sinyinews.com.tw/quarterly
- or: https://www.ncscre.nccu.edu.tw/taxonomy/term/8
- Reference file: `Repeat_Sales/sinyi_hpi.dta` (covers 2001Q1–2023Q2)
- Create/update `website/data/sinyi_hpi.csv`:
  ```csv
  quarter,sinyi_all
  2001Q1,0.409499
  ...
  2023Q3,<new value>
  ```
- **Normalization**: sinyi_all is normalized so 2012Q3 = 1.00.
  If the source gives a different base, divide all values by the 2012Q3 value.

#### Cathay HPI (國泰房地產指數)
- URL: https://www.cathay-red.com.tw/tw/About/House
- Reference file: `Repeat_Sales/cathay_hpi.dta` (covers 2004Q1–2023Q2)
- Create/update `website/data/cathay_hpi.csv`:
  ```csv
  quarter,cathay_all
  2004Q1,0.598090
  ...
  2023Q3,<new value>
  ```

#### AIFE/NTHU HPI (清大安富指數)
- URL: https://aife.site.nthu.edu.tw
- Download link: https://houseplus.com.tw/reportIndex
- Reference file: `Repeat_Sales/aife_hpi.dta` (covers 2012Q3–2023Q1)
- Create/update `website/data/aife_hpi.csv`:
  ```csv
  quarter,aife_all
  2012Q3,1.000000
  ...
  2023Q2,<new value>
  ```

#### Official MOI HPI (住宅價格指數)
- URL: https://pip.moi.gov.tw/Publicize/Info/E2010
- Click "住宅價格指數" → Select "Quarterly" → Download Excel
- Reference file: `Repeat_Sales/official_hpi.dta` (covers 2012Q3–2023Q1)
- Normalize: divide all values by the 2012Q3 value
- Create/update `website/data/official_hpi.csv`:
  ```csv
  quarter,govt_all
  2012Q3,1.000000
  ...
  2023Q2,<new value>
  ```

### Step 3: Regenerate website data files

```bash
cd "Taiwan Price Index_claude/website"
python scripts/update_data.py
```

This rewrites all JSON/CSV files in `website/data/`.

### Step 4: Preview the website

Open `website/index.html` in a browser. For the map to work properly,
serve from a local HTTP server (due to CORS restrictions on file:// URLs):

```bash
cd "Taiwan Price Index_claude/website"
python -m http.server 8080
# Then open: http://localhost:8080
```

---

## Replicating the CLL Index from Scratch

All Stata code is in `../code/`. The full pipeline:

| Script | Input | Output |
|--------|-------|--------|
| `make_master_tw.do` | `lvr_landcsv/` CSVs | `master/*.dta` |
| `make_estim_sample_tw.do` | `master/*.dta` | `sample_deprec_estim.dta` |
| `make_repeat_sales_tw.do` | `sample_deprec_estim.dta`, `local_building_clean.dta` | `Repeat_Sales/rs_m*.dta`, `fulltime_index_q_nonew.csv` |
| `make_fulltime_annual.do` | quarterly index | `fulltime_index_a_nonew.csv` |
| `time_series_plot.do` | all index files | EPS plots |

Master entry point: `run_this_code.do`

---

## Notes on Regional Aggregation

The CLL index consolidates the following regions to ensure sufficient sample sizes:

| CLL Region | Included Administrative Regions |
|-----------|--------------------------------|
| hsinchu | Hsinchu City + Hsinchu County |
| chiayi | Chiayi City + Chiayi County |
| yunlin | Yunlin County + Penghu County + Kinmen County + Lienchiang County |

Regions not separately identified in the CLL index (grouped with neighbors or
excluded due to small sample): Miaoli, Nantou, Changhua, Yilan.
