/* Taiwan HPI – Internationalisation (Traditional Chinese / English)
   Default language: zh (Traditional Chinese)
   Usage:
     t('key')                   → translated string for current lang
     getCityLabel(key, fallback) → city name in current lang
     getIndexLabel(key, fallback)→ index name in current lang
     setLang('zh'|'en')         → switch language + re-render
     initLang()                 → call once at page load
*/

const TRANSLATIONS = {
  zh: {
    /* ── Page titles ─────────────────────────────────────────────── */
    'page.title.index':       'CL 臺灣住宅價格指數',
    'page.title.comparison':  '指數比較 – 臺灣住宅價格指數',
    'page.title.map':         '地圖 – 臺灣住宅價格指數',
    'page.title.methodology': '研究方法 – 臺灣住宅價格指數',

    /* ── Nav ─────────────────────────────────────────────────────── */
    'nav.timeseries':  '時間序列',
    'nav.gismap':      '地圖',
    'nav.comparison':  '指數比較',
    'nav.methodology': '研究方法',

    /* ── index.html ──────────────────────────────────────────────── */
    'index.hero.title':    'CL 臺灣住宅價格指數',
    'index.hero.desc':     '以重複交易法建構臺灣15個行政區之季度與年度住宅價格指數，2000Q1–2025Q4。資料來源：內政部不動產交易實價登錄。',
    'index.dl.label':      '📥 下載資料：',
    'index.dl.q_csv':      '季指數 (CSV)',
    'index.dl.a_csv':      '年指數 (CSV)',
    'index.dl.q_json':     '季指數 (JSON)',
    'index.dl.a_json':     '年指數 (JSON)',
    'index.chart.title':   '城市住宅價格指數',
    'index.chart.badge':   '更新至 2025Q4',
    'index.freq.q':        '季',
    'index.freq.a':        '年',
    'index.freq.note':     '基期 = 2012Q3 &nbsp;|&nbsp; 點擊圖例切換城市',
    'index.btn.all':       '全選',
    'index.btn.clear':     '清除',
    'index.chart.note':    '將滑鼠移至任一序列可反白顯示。所有指數以 2012Q3 = 1.00 為基準。數值代表相對於基期的價格水準。',
    'index.chart.yaxis':   '指數 (2012Q3 = 1.00)',
    'index.latest.title':  '最新指數值 — 2025Q4',
    'index.tbl.region':    '地區',
    'index.tbl.index':     '指數 (2025Q4)',
    'index.tbl.qoq':       '季增率',
    'index.tbl.yoy':       '年增率',
    'index.tbl.note':      '基期：2012Q3 = 1.00。季增率 = 季對季；年增率 = 年對年（4季）。',
    'index.footer':        '臺灣住宅價格指數 &nbsp;|&nbsp; 資料來源：內政部實價登錄 &nbsp;|&nbsp; 方法論：CL 重複交易指數',

    /* ── comparison.html ─────────────────────────────────────────── */
    'comp.hero.title':    '臺灣住宅價格指數比較',
    'comp.hero.desc':     'CL 指數與官方（內政部）、信義、國泰及安富指數之比較，所有指數以 2012Q3 = 1.00 為基準。',
    'comp.note':          '💡 <strong>資料涵蓋：</strong>CL 指數涵蓋至 2025Q4；信義（2025Q4）、國泰（2025Q4）、安富（2025Q4）及官方（2025Q3）指數已更新至最新可取得資料。',
    'comp.dl.label':      '📥 下載：',
    'comp.dl.csv':        '所有指數 (CSV)',
    'comp.dl.json':       '所有指數 (JSON)',
    'comp.chart.title':   '臺灣各住宅價格指數（基期：2012Q3 = 1.00）',
    'comp.chart.note':    '將滑鼠移至序列可查看詳細數值。點擊上方色塊切換指數顯示。',
    'comp.chart.yaxis':   '指數 (2012Q3 = 1.00)',
    'comp.about.title':   '各指數說明',
    'comp.corr.title':    '指數間相關係數（重疊期間 2012Q3–2025Q3）',
    'comp.corr.note':     '皮爾森相關係數，計算所有兩指數均有有效資料的季度。',
    'comp.footer':        '臺灣住宅價格指數 &nbsp;|&nbsp; 資料來源：內政部、信義、國泰、安富/清大、CL 重複交易',
    'comp.coverage':      '涵蓋期間',
    'comp.base':          '基期',

    /* ── map.html ────────────────────────────────────────────────── */
    'map.hero.title':     '地圖 — 各地區住宅價格',
    'map.hero.desc':      '將滑鼠移至或點擊任一地區，查看最新住宅價格指數及增長率。',
    'map.note':           '💡 顯示為<strong>藍色梯度</strong>的地區有 CL 指數資料。灰色地區（苗栗、南投、彰化、宜蘭）未單獨涵蓋於此指數中。新竹市+縣及嘉義市+縣各別整合為一個地區。',
    'map.panel.hover':    '將滑鼠移至地區',
    'map.panel.default':  '點擊地圖上的地區以查看住宅價格統計資料。',
    'map.panel.nodata':   '此地區在 CL 指數中已與鄰近地區合併計算。',
    'map.legend.title':   '地圖圖例',
    'map.legend.desc':    '顏色深淺 = 年增率（顏色越深代表增長率越高）',
    'map.legend.nodata':  '無獨立資料（已整合）',
    'map.legend.to':      '至',
    'map.legend.yoy':     '年增率',
    'map.stat.latest':    '最新期間：',
    'map.stat.index':     '住宅價格指數',
    'map.stat.base':      '(2012Q3 = 1.00)',
    'map.stat.qoq':       '季增率',
    'map.stat.yoy':       '年增率',
    'map.stat.risen':     '上漲',
    'map.stat.fallen':    '下跌',
    'map.stat.desc':      '指數顯示自基期（2012Q3）以來價格已',
    'map.footer':         '臺灣住宅價格指數 &nbsp;|&nbsp; 地圖：OpenStreetMap 貢獻者，Leaflet &nbsp;|&nbsp; 資料：內政部實價登錄',

    /* ── methodology.html ────────────────────────────────────────── */
    'meth.hero.title':    '研究方法',
    'meth.hero.desc':     'CL 重複交易住宅價格指數的建構方式，以及其與現有臺灣價格指數之差異。',
    'meth.adv.title':     'CL 指數之主要特點',
    'meth.src.title':     '資料來源',
    'meth.rs.title':      '重複交易迴歸',
    'meth.methods.title': '四種房屋配對方法',
    'meth.wf.title':      '指數建構流程',
    'meth.sample.title':  '樣本篩選標準',
    'meth.cite.title':    '參考文獻',
    'meth.footer':        '臺灣住宅價格指數 &nbsp;|&nbsp; 方法論基於 CL 重複交易框架 &nbsp;|&nbsp; 資料：內政部實價登錄',
  },

  en: {
    /* ── Page titles ─────────────────────────────────────────────── */
    'page.title.index':       'CL Taiwan Housing Price Index',
    'page.title.comparison':  'Index Comparison – Taiwan HPI',
    'page.title.map':         'Map – Taiwan HPI',
    'page.title.methodology': 'Methodology – Taiwan HPI',

    /* ── Nav ─────────────────────────────────────────────────────── */
    'nav.timeseries':  'Time Series',
    'nav.gismap':      'Map',
    'nav.comparison':  'Index Comparison',
    'nav.methodology': 'Methodology',

    /* ── index.html ──────────────────────────────────────────────── */
    'index.hero.title':    'CL Taiwan Housing Price Index',
    'index.hero.desc':     'Repeat-sales based housing price indices for 15 regions of Taiwan, 2000Q1–2025Q4. Based on transaction-level data from the Ministry of Interior.',
    'index.dl.label':      '📥 Download Data:',
    'index.dl.q_csv':      'Quarterly Index (CSV)',
    'index.dl.a_csv':      'Annual Index (CSV)',
    'index.dl.q_json':     'Quarterly (JSON)',
    'index.dl.a_json':     'Annual (JSON)',
    'index.chart.title':   'City-Level Housing Price Indices',
    'index.chart.badge':   'Updated 2025Q4',
    'index.freq.q':        'Quarterly',
    'index.freq.a':        'Annual',
    'index.freq.note':     'Base = 2012Q3 &nbsp;|&nbsp; Click legend to toggle cities',
    'index.btn.all':       'Select All',
    'index.btn.clear':     'Clear',
    'index.chart.note':    'Hover over any series to highlight it. All indices normalized so that 2012Q3 = 1.00. Values represent the price level relative to the base period.',
    'index.chart.yaxis':   'Index (2012Q3 = 1.00)',
    'index.latest.title':  'Latest Index Values — 2025Q4',
    'index.tbl.region':    'Region',
    'index.tbl.index':     'Index (2025Q4)',
    'index.tbl.qoq':       'QoQ Growth',
    'index.tbl.yoy':       'YoY Growth',
    'index.tbl.note':      'Base period: 2012Q3 = 1.00. QoQ = quarter-on-quarter; YoY = year-on-year (4 quarters).',
    'index.footer':        'Taiwan Housing Price Index &nbsp;|&nbsp; Data: Ministry of Interior (MOI) Actual Price Registration &nbsp;|&nbsp; Methodology: CL Repeat-Sales Index',

    /* ── comparison.html ─────────────────────────────────────────── */
    'comp.hero.title':    'Comparison of Taiwan Housing Price Indices',
    'comp.hero.desc':     'The CL repeat-sales index plotted alongside the Official (MOI), Sinyi, Cathay, and AIFE indices. All indices normalized to 2012Q3 = 1.00.',
    'comp.note':          '💡 <strong>Data coverage:</strong> CL index runs through 2025Q4. Sinyi (2025Q4), Cathay (2025Q4), AIFE (2025Q4), and Official (2025Q3) indices are updated to the latest available data.',
    'comp.dl.label':      '📥 Download:',
    'comp.dl.csv':        'All Indices (CSV)',
    'comp.dl.json':       'All Indices (JSON)',
    'comp.chart.title':   'All Taiwan Housing Price Indices (Base: 2012Q3 = 1.00)',
    'comp.chart.note':    'Hover over series for exact values. Click chips above to toggle indices.',
    'comp.chart.yaxis':   'Index (2012Q3 = 1.00)',
    'comp.about.title':   'About Each Index',
    'comp.corr.title':    'Cross-Index Correlation (overlapping period 2012Q3–2025Q3)',
    'comp.corr.note':     'Pearson correlation coefficients computed over all quarters where both indices have valid data.',
    'comp.footer':        'Taiwan Housing Price Index &nbsp;|&nbsp; Data sources: MOI, Sinyi, Cathay, AIFE/NTHU, CL repeat-sales',
    'comp.coverage':      'Coverage',
    'comp.base':          'Base',

    /* ── map.html ────────────────────────────────────────────────── */
    'map.hero.title':     'Map — Regional Housing Prices',
    'map.hero.desc':      'Hover or click on any region to see the latest housing price index and growth rates.',
    'map.note':           '💡 Regions shown in <strong>blue gradient</strong> have CL index data. Gray regions (Miaoli, Nantou, Changhua, Yilan) are not covered separately in this index. Hsinchu City+County and Chiayi City+County are each consolidated into one region.',
    'map.panel.hover':    'Hover over a region',
    'map.panel.default':  'Point to a region on the map to see its housing price statistics.',
    'map.panel.nodata':   'This region is grouped with adjacent areas in the CL index.',
    'map.legend.title':   'Map Legend',
    'map.legend.desc':    'Color intensity = YoY price growth (darker = higher growth)',
    'map.legend.nodata':  'No separate data (aggregated)',
    'map.legend.to':      'to',
    'map.legend.yoy':     'YoY',
    'map.stat.latest':    'Latest period:',
    'map.stat.index':     'Price Index',
    'map.stat.base':      '(2012Q3 = 1.00)',
    'map.stat.qoq':       'QoQ Growth',
    'map.stat.yoy':       'YoY Growth',
    'map.stat.risen':     'risen',
    'map.stat.fallen':    'fallen',
    'map.stat.desc':      'The index value indicates that prices have',
    'map.footer':         'Taiwan Housing Price Index &nbsp;|&nbsp; Map: OpenStreetMap contributors, Leaflet &nbsp;|&nbsp; Data: MOI APR',

    /* ── methodology.html ────────────────────────────────────────── */
    'meth.hero.title':    'Methodology',
    'meth.hero.desc':     'How the CL Repeat-Sales Housing Price Index is constructed — and why it improves on existing Taiwan price indices.',
    'meth.adv.title':     'Key Advantages of the CL Index',
    'meth.src.title':     'Data Source',
    'meth.rs.title':      'Repeat-Sales Regression',
    'meth.methods.title': 'Four Property-Matching Methods',
    'meth.wf.title':      'Index Construction Workflow',
    'meth.sample.title':  'Sample Selection Criteria',
    'meth.cite.title':    'Reference',
    'meth.footer':        'Taiwan Housing Price Index &nbsp;|&nbsp; Methodology based on CL repeat-sales framework &nbsp;|&nbsp; Data: Ministry of Interior APR',
  }
};

/* ── City label translations ──────────────────────────────────────── */
const CITY_LABELS_ZH = {
  all:        '全臺灣',
  taipei:     '臺北市',
  newtaipei:  '新北市',
  taoyuan:    '桃園市',
  hsinchu:    '新竹市/縣',
  taichung:   '臺中市',
  tainan:     '臺南市',
  kaohsiung:  '高雄市',
  keelung:    '基隆市',
  chiayi:     '嘉義市/縣',
  yunlin:     '雲林縣',
  pingtung:   '屏東縣',
  hualien:    '花蓮縣',
  taitung:    '臺東縣',
};

/* ── Index label translations ─────────────────────────────────────── */
const INDEX_LABELS_ZH = {
  cll:     'CL 指數（本網站）',
  official: '內政部住宅價格指數',
  sinyi:    '信義房屋住宅價格指數',
  cathay:   '國泰房地產指數',
  aife:     '清華安富指數',
};

/* ── Helper functions ─────────────────────────────────────────────── */
function t(key) {
  const lang = document.documentElement.lang || 'zh';
  const dict = TRANSLATIONS[lang] || TRANSLATIONS.zh;
  if (dict[key] !== undefined) return dict[key];
  if (TRANSLATIONS.en[key] !== undefined) return TRANSLATIONS.en[key];
  return key;
}

function getCityLabel(key, fallback) {
  if ((document.documentElement.lang || 'zh') === 'zh' && CITY_LABELS_ZH[key])
    return CITY_LABELS_ZH[key];
  return fallback;
}

function getIndexLabel(key, fallback) {
  if ((document.documentElement.lang || 'zh') === 'zh' && INDEX_LABELS_ZH[key])
    return INDEX_LABELS_ZH[key];
  return fallback;
}

/* ── Core language switcher ───────────────────────────────────────── */
function setLang(lang) {
  document.documentElement.lang = lang;
  localStorage.setItem('hpi-lang', lang);

  // Update all static data-i18n elements
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.getAttribute('data-i18n');
    const val = (TRANSLATIONS[lang] || {})[key];
    if (val !== undefined) el.innerHTML = val;
  });

  // Update page <title>
  const titleEl = document.querySelector('title[data-i18n]');
  if (titleEl) {
    const val = (TRANSLATIONS[lang] || {})[titleEl.getAttribute('data-i18n')];
    if (val !== undefined) titleEl.textContent = val;
  }

  // Toggle button label: show the OTHER language
  const btn = document.getElementById('lang-toggle');
  if (btn) btn.textContent = lang === 'zh' ? 'EN' : '中文';

  // Trigger page-specific re-render of dynamic content
  if (typeof onLangChange === 'function') onLangChange(lang);
}

function initLang() {
  const saved = localStorage.getItem('hpi-lang') || 'zh';
  document.documentElement.lang = saved;   // set before any dynamic build
  setLang(saved);
}
