# -*- coding: utf-8 -*-
"""
allocation_history_loader.py
────────────────────────────
Robust loader for the isolated "היסטוריית אלוקציה" feature.

Design goals:
- Do not touch the existing app logic outside this feature.
- Prefer downloading the whole workbook as XLSX (more stable than guessing gid's).
- Fall back to public CSV export only if XLSX loading fails.
- Be tolerant to messy sheet structures: title rows, empty rows, unnamed columns,
  month/year split columns, percent values stored as text, and mixed date formats.
- Return detailed debug warnings without conflating parsing errors with auth errors.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import requests
import streamlit as st

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Text cleaning helpers
# ──────────────────────────────────────────────────────────────────────────────
_INVIS_RE = re.compile(
    r'[\u200b\u200c\u200d\u200e\u200f'
    r'\u202a\u202b\u202c\u202d\u202e'
    r'\u2066\u2067\u2068\u2069'
    r'\ufeff\u00a0\u00ad]'
)


def _clean(value: object) -> str:
    return _INVIS_RE.sub('', str(value)).strip()


def _norm_text(value: object) -> str:
    return _clean(value).lower().replace('_', ' ').replace('-', ' ')


def _looks_blank(value: object) -> bool:
    s = _norm_text(value)
    return s in {'', 'nan', 'none', 'nat'}


# ──────────────────────────────────────────────────────────────────────────────
# Domain mappings
# ──────────────────────────────────────────────────────────────────────────────
_HEB_MONTHS = {
    "ינואר": 1, "פברואר": 2, "מרץ": 3, "מרס": 3,
    "אפריל": 4, "מאי": 5, "יוני": 6,
    "יולי": 7, "אוגוסט": 8, "ספטמבר": 9,
    "אוקטובר": 10, "נובמבר": 11, "דצמבר": 12,
}
_EN_MONTHS = {
    'jan': 1, 'january': 1,
    'feb': 2, 'february': 2,
    'mar': 3, 'march': 3,
    'apr': 4, 'april': 4,
    'may': 5,
    'jun': 6, 'june': 6,
    'jul': 7, 'july': 7,
    'aug': 8, 'august': 8,
    'sep': 9, 'sept': 9, 'september': 9,
    'oct': 10, 'october': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12,
}

_DATE_KEYWORDS = {
    'תאריך', 'חודש', 'חודשים', 'תקופה', 'חודש דיווח', 'תאריך דיווח',
    'month', 'months', 'date', 'period', 'report month', 'report date',
    'month date', 'time', 'as of'
}
_YEAR_KEYWORDS = {'שנה', 'year', 'yyyy'}
_MONTH_COL_KEYWORDS = {'חודש', 'month', 'mm', 'חודשים'}
_TYPE_KEYWORDS = {'סוג', 'type', 'kind', 'סוג התאריך', 'period type'}
_MONTH_TYPE_VALUES = {'month', 'חודשי', 'חודש', 'monthly'}
_SKIP_HEADER_HINTS = {'unnamed', 'index', 'מספר', 'id'}

_SHEET_META: dict[str, dict] = {
    'הראל כללי': {'manager': 'הראל', 'track': 'כללי'},
    'הראל מנייתי': {'manager': 'הראל', 'track': 'מנייתי'},
}
_MANAGER_PATTERNS = [
    'הראל', 'מגדל', 'כלל', 'מנורה', 'הפניקס', 'אנליסט', 'מיטב',
    'ילין', 'פסגות', 'אלטשולר', 'ברקת', 'אלומות',
]
_TRACK_PATTERNS = {
    'כלל': 'כללי', 'כללי': 'כללי',
    'מנייתי': 'מנייתי', 'מניות': 'מנייתי',
}


# ──────────────────────────────────────────────────────────────────────────────
# Metadata inference
# ──────────────────────────────────────────────────────────────────────────────
def _infer_meta(sheet_name: str) -> dict:
    s = _clean(sheet_name)
    for key, meta in _SHEET_META.items():
        if _clean(key) in s:
            return meta
    manager = next((m for m in _MANAGER_PATTERNS if m in s), s)
    track = 'כללי'
    for pat, val in _TRACK_PATTERNS.items():
        if pat in s:
            track = val
            break
    return {'manager': manager, 'track': track}


def _extract_sheet_id(url: str) -> str:
    m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
    if not m:
        raise ValueError(f'לא ניתן לחלץ Sheet ID מהכתובת: {url}')
    return m.group(1)


def _xlsx_export_url(sheet_id: str) -> str:
    return f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx'


def _csv_export_url(sheet_id: str, gid: int = 0) -> str:
    return f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'


# ──────────────────────────────────────────────────────────────────────────────
# CSV/XLSX reading helpers
# ──────────────────────────────────────────────────────────────────────────────
def _read_csv_rows(csv_text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        cleaned = [_clean(c) for c in row]
        if any(not _looks_blank(c) for c in cleaned):
            rows.append(cleaned)
    return rows


def _read_excel_rows(raw_df: pd.DataFrame) -> list[list[str]]:
    rows: list[list[str]] = []
    for _, row in raw_df.iterrows():
        cleaned = [_clean(v) for v in row.tolist()]
        if any(not _looks_blank(c) for c in cleaned):
            rows.append(cleaned)
    return rows


def _is_numeric_like(value: object) -> bool:
    s = _clean(value).replace('%', '').replace(',', '.').replace('−', '-').strip()
    if not s:
        return False
    if re.fullmatch(r'-?\d+(?:\.\d+)?', s):
        return True
    return False


def _header_row_score(row: list[str]) -> int:
    score = 0
    norm_cells = [_norm_text(c) for c in row if not _looks_blank(c)]
    if not norm_cells:
        return -999

    for cell in norm_cells:
        if any(kw in cell for kw in _DATE_KEYWORDS):
            score += 5
        if any(kw == cell or kw in cell for kw in _YEAR_KEYWORDS):
            score += 3
        if any(kw == cell or kw in cell for kw in _MONTH_COL_KEYWORDS):
            score += 3
        if any(kw in cell for kw in _TYPE_KEYWORDS):
            score += 1
        if any(skip in cell for skip in _SKIP_HEADER_HINTS):
            score -= 1
        if not _is_numeric_like(cell):
            score += 1

    non_numeric = sum(1 for c in norm_cells if not _is_numeric_like(c))
    numeric = sum(1 for c in norm_cells if _is_numeric_like(c))

    if non_numeric >= 3:
        score += 3
    if numeric >= non_numeric and numeric > 2:
        score -= 4

    return score


def _find_header_row(rows: list[list[str]], max_scan: int = 25) -> int:
    best_idx = 0
    best_score = -999
    for i, row in enumerate(rows[:max_scan]):
        score = _header_row_score(row)
        if score > best_score:
            best_score = score
            best_idx = i
    return best_idx


# ──────────────────────────────────────────────────────────────────────────────
# DataFrame construction from raw rows
# ──────────────────────────────────────────────────────────────────────────────
def _dedupe_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for idx, h in enumerate(headers):
        base = _clean(h) or f'Unnamed_{idx}'
        n = seen.get(base, 0)
        seen[base] = n + 1
        out.append(base if n == 0 else f'{base}__{n+1}')
    return out


def _rows_to_dataframe(rows: list[list[str]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    header_idx = _find_header_row(rows)
    max_cols = max(len(r) for r in rows)
    padded = [r + [''] * (max_cols - len(r)) for r in rows]
    headers = _dedupe_headers(padded[header_idx])
    data = padded[header_idx + 1:]
    if not data:
        return pd.DataFrame(columns=headers)
    df = pd.DataFrame(data, columns=headers)
    df = df.apply(lambda col: col.mask(col.str.strip() == '') if col.dtype == object else col)
    df = df.dropna(how='all').reset_index(drop=True)
    df.attrs['header_row_index'] = header_idx
    return df


def _read_csv_smart(csv_text: str) -> pd.DataFrame:
    rows = _read_csv_rows(csv_text)
    return _rows_to_dataframe(rows)


def _read_excel_smart(raw_df: pd.DataFrame) -> pd.DataFrame:
    rows = _read_excel_rows(raw_df)
    return _rows_to_dataframe(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Column role detection
# ──────────────────────────────────────────────────────────────────────────────
def _find_date_col(columns: list[str]) -> Optional[str]:
    cleaned = {c: _norm_text(c) for c in columns}

    def is_type_col(txt: str) -> bool:
        return any(tk in txt for tk in _TYPE_KEYWORDS)

    for c, txt in cleaned.items():
        if txt in _DATE_KEYWORDS and not is_type_col(txt):
            return c
    for c, txt in cleaned.items():
        if not is_type_col(txt) and any(txt.endswith(kw) for kw in _DATE_KEYWORDS):
            return c
    for c, txt in cleaned.items():
        if not is_type_col(txt) and any(kw in txt for kw in _DATE_KEYWORDS):
            return c
    return None


def _find_type_col(columns: list[str]) -> Optional[str]:
    for c in columns:
        txt = _norm_text(c)
        if any(tk in txt for tk in _TYPE_KEYWORDS):
            return c
    return None


def _find_year_col(columns: list[str]) -> Optional[str]:
    for c in columns:
        txt = _norm_text(c)
        if txt in _YEAR_KEYWORDS or any(kw in txt for kw in _YEAR_KEYWORDS):
            return c
    return None


def _find_month_col(columns: list[str], exclude: set[str]) -> Optional[str]:
    for c in columns:
        if c in exclude:
            continue
        txt = _norm_text(c)
        if txt in _MONTH_COL_KEYWORDS or any(kw in txt for kw in _MONTH_COL_KEYWORDS):
            return c
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Date / percent parsing
# ──────────────────────────────────────────────────────────────────────────────
def _parse_excel_serial(value: object) -> Optional[datetime]:
    try:
        f = float(str(value).strip())
    except Exception:
        return None
    if f < 20000 or f > 80000:
        return None
    try:
        ts = pd.to_datetime('1899-12-30') + pd.to_timedelta(f, unit='D')
        return ts.replace(day=1).to_pydatetime()
    except Exception:
        return None


def _coerce_year(y: object) -> Optional[int]:
    s = _clean(y)
    if not s:
        return None
    m = re.search(r'(19\d{2}|20\d{2})', s)
    if m:
        return int(m.group(1))
    if re.fullmatch(r'\d{2}', s):
        yy = int(s)
        return 2000 + yy if yy <= 50 else 1900 + yy
    return None


def _coerce_month(mv: object) -> Optional[int]:
    if mv is None:
        return None
    s = _norm_text(mv)
    if not s:
        return None
    if s.isdigit():
        m = int(s)
        if 1 <= m <= 12:
            return m
    for name, month in _HEB_MONTHS.items():
        if name in s:
            return month
    for name, month in _EN_MONTHS.items():
        if re.search(rf'\b{name}\b', s):
            return month
    return None


def _parse_date_value(val: object) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if isinstance(val, (datetime, pd.Timestamp)):
        return pd.Timestamp(val).replace(day=1).to_pydatetime()

    serial = _parse_excel_serial(val)
    if serial is not None:
        return serial

    s = _clean(val)
    if not s or s.lower() in {'nan', 'none', 'nat'}:
        return None

    for heb, mn in _HEB_MONTHS.items():
        if heb in s:
            y = re.search(r'(19\d{2}|20\d{2})', s)
            if y:
                return datetime(int(y.group(1)), mn, 1)

    # MM.YYYY / MM-YYYY / YYYYMM / YYYY-MM / MM/YYYY
    patterns = [
        (r'^(\d{1,2})[./-](\d{4})$', 'my'),
        (r'^(\d{4})[./-](\d{1,2})$', 'ym'),
        (r'^(\d{4})(\d{2})$', 'ym'),
        (r'^(\d{1,2})[./-](\d{2})$', 'my2'),
    ]
    for pat, mode in patterns:
        m = re.match(pat, s)
        if not m:
            continue
        a, b = int(m.group(1)), int(m.group(2))
        if mode == 'my' and 1 <= a <= 12:
            return datetime(b, a, 1)
        if mode == 'ym' and 1 <= b <= 12:
            return datetime(a, b, 1)
        if mode == 'my2' and 1 <= a <= 12:
            year = 2000 + b if b <= 50 else 1900 + b
            return datetime(year, a, 1)

    # Month name + year
    s_norm = _norm_text(s)
    for name, month in _EN_MONTHS.items():
        if re.search(rf'\b{name}\b', s_norm):
            y = re.search(r'(19\d{2}|20\d{2}|\d{2})', s_norm)
            if y:
                year = _coerce_year(y.group(1))
                if year:
                    return datetime(year, month, 1)

    for fmt in (
        '%Y-%m-%d', '%d/%m/%Y', '%m/%Y', '%Y-%m', '%b-%Y',
        '%B %Y', '%b %Y', '%Y/%m/%d', '%d-%m-%Y', '%d.%m.%Y'
    ):
        try:
            return datetime.strptime(s, fmt).replace(day=1)
        except ValueError:
            pass

    try:
        ts = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if pd.notna(ts):
            return ts.replace(day=1).to_pydatetime()
    except Exception:
        pass

    return None


def _parse_percent(val: object) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None

    raw = _clean(val)
    if not raw:
        return None

    s = raw.replace('%', '').replace(',', '.').replace('−', '-').strip()
    if not s:
        return None
    if not re.fullmatch(r'-?\d+(?:\.\d+)?', s):
        return None

    f = float(s)
    raw_lower = raw.lower()
    # Convert decimals only when the raw representation strongly suggests a ratio.
    if '%' in raw_lower:
        return round(f, 4)
    if -1 <= f <= 1 and (raw.startswith('0.') or raw.startswith('-0.') or raw in {'0', '1'}):
        return round(f * 100, 4)
    return round(f, 4)




def _looks_like_allocation_label(val: object) -> bool:
    s = _clean(val)
    if not s:
        return False
    if _is_numeric_like(s):
        return False
    s_norm = _norm_text(s)
    if any(k in s_norm for k in ['סה"כ', 'total', 'manager', 'track']):
        return False
    return True


def _detect_wide_date_columns(columns: list[str]) -> dict[str, datetime]:
    detected: dict[str, datetime] = {}
    for col in columns:
        col_clean = _clean(col)
        if not col_clean or _norm_text(col_clean).startswith('unnamed'):
            continue
        dt = _parse_date_value(col_clean)
        if dt is not None:
            detected[col] = dt
    return detected




def _collapse_monthly_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    out = out.dropna(subset=["date"])
    out = (
        out.groupby(["manager", "track", "date", "allocation_name", "source_sheet"], as_index=False)["allocation_value"]
        .mean()
    )
    out["year"] = out["date"].dt.year
    out["month"] = out["date"].dt.month
    return out.sort_values(["manager", "track", "allocation_name", "date"]).reset_index(drop=True)


def _parse_wide_sheet_df(raw: pd.DataFrame, sheet_name: str, debug_warnings: list[str]) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()

    meta = _infer_meta(sheet_name)
    raw = raw.copy()
    raw.columns = [_clean(c) for c in raw.columns]
    header_row_index = raw.attrs.get('header_row_index', '?')

    date_cols = _detect_wide_date_columns(list(raw.columns))
    if len(date_cols) < 2:
        return pd.DataFrame()

    non_date_cols = [c for c in raw.columns if c not in date_cols]
    label_col = next((c for c in non_date_cols if not _norm_text(c).startswith('unnamed')), None)
    if label_col is None:
        return pd.DataFrame()

    rows: list[dict] = []
    for _, row in raw.iterrows():
        alloc_name = _clean(row.get(label_col))
        if not _looks_like_allocation_label(alloc_name):
            continue

        added = 0
        for col, dt in sorted(date_cols.items(), key=lambda x: x[1]):
            val = _parse_percent(row.get(col))
            if val is None:
                continue
            rows.append({
                'manager': meta['manager'],
                'track': meta['track'],
                'date': pd.Timestamp(dt),
                'year': dt.year,
                'month': dt.month,
                'allocation_name': alloc_name,
                'allocation_value': val,
                'source_sheet': sheet_name,
            })
            added += 1

        if added == 0:
            continue

    if not rows:
        debug_warnings.append(
            f"⚠️ גליון **{sheet_name}**: זוהה מבנה רוחבי, אבל לא הופקו שורות נתונים. "
            f"header row: `{header_row_index}` | עמודת רכיב: `{label_col}` | תאריכים שזוהו: {len(date_cols)}"
        )
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    return _collapse_monthly_duplicates(df)


# ──────────────────────────────────────────────────────────────────────────────
# Normalisation
# ──────────────────────────────────────────────────────────────────────────────
def _format_debug_preview(df: pd.DataFrame) -> str:
    try:
        return df.head(5).fillna('').astype(str).to_string(index=False)
    except Exception:
        return '(preview unavailable)'


def _normalise_sheet_df(raw: pd.DataFrame, sheet_name: str, debug_warnings: list[str]) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()

    raw = raw.copy()
    raw.columns = [_clean(c) for c in raw.columns]
    header_row_index = raw.attrs.get('header_row_index', '?')
    meta = _infer_meta(sheet_name)

    # ניסיון 1: מבנה "ארוך" קלאסי — כל שורה היא תקופה, וכל עמודה היא רכיב
    date_col = _find_date_col(list(raw.columns))
    type_col = _find_type_col(list(raw.columns))
    year_col = _find_year_col(list(raw.columns))
    month_col = _find_month_col(list(raw.columns), exclude={c for c in [date_col, type_col, year_col] if c})

    long_raw = raw.copy()
    if type_col is not None:
        month_mask = long_raw[type_col].astype(str).map(_clean).str.lower().isin(_MONTH_TYPE_VALUES)
        if month_mask.any():
            long_raw = long_raw[month_mask].copy()

    alloc_skip = {c for c in [date_col, type_col, year_col, month_col] if c}
    alloc_cols = [
        c for c in long_raw.columns
        if c not in alloc_skip
        and not _norm_text(c).startswith('unnamed')
        and not _looks_blank(c)
    ]

    rows: list[dict] = []
    if date_col is not None or (year_col and month_col):
        for _, row in long_raw.iterrows():
            dt: Optional[datetime] = None
            if date_col is not None:
                dt = _parse_date_value(row.get(date_col))
            if dt is None and year_col and month_col:
                y = _coerce_year(row.get(year_col))
                m = _coerce_month(row.get(month_col))
                if y and m:
                    dt = datetime(y, m, 1)
            if dt is None:
                continue

            for col in alloc_cols:
                val = _parse_percent(row.get(col))
                if val is None:
                    continue
                rows.append({
                    'manager': meta['manager'],
                    'track': meta['track'],
                    'date': pd.Timestamp(dt),
                    'year': dt.year,
                    'month': dt.month,
                    'allocation_name': _clean(col),
                    'allocation_value': val,
                    'source_sheet': sheet_name,
                })

    long_df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not long_df.empty:
        long_df['date'] = pd.to_datetime(long_df['date'])
        long_df = _collapse_monthly_duplicates(long_df)

    # ניסיון 2: מבנה "רוחבי" — עמודות הן חודשים/תאריכים, והשורות הן רכיבי אלוקציה
    wide_df = _parse_wide_sheet_df(raw, sheet_name, debug_warnings)

    # מעדיפים את הפענוח העשיר יותר בתאריכים כדי להימנע מגרף "ישר" ודליל מדי
    if not wide_df.empty and (long_df.empty or wide_df['date'].nunique() > long_df['date'].nunique()):
        return wide_df
    if not long_df.empty:
        return long_df
    if not wide_df.empty:
        return wide_df

    debug_warnings.append(
        f"⚠️ גליון **{sheet_name}**: לא זוהה מבנה נתונים תקין. "
        f"header row: `{header_row_index}` | עמודות: `{list(raw.columns)[:12]}`\n"
        f"תצוגה מקדימה:\n```\n{_format_debug_preview(raw)}\n```"
    )
    return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Public transports
# ──────────────────────────────────────────────────────────────────────────────
def _load_via_public_xlsx(sheet_id: str, debug_warnings: list[str]) -> pd.DataFrame:
    url = _xlsx_export_url(sheet_id)
    try:
        r = requests.get(url, timeout=30, allow_redirects=True)
        if r.status_code in (401, 403):
            debug_warnings.append(
                f'🔒 הורדת XLSX נחסמה (HTTP {r.status_code}). הגיליון כנראה לא פתוח לקריאה ציבורית.'
            )
            return pd.DataFrame()
        if r.status_code != 200:
            debug_warnings.append(f'⚠️ הורדת XLSX נכשלה (HTTP {r.status_code}).')
            return pd.DataFrame()
        ct = r.headers.get('Content-Type', '').lower()
        if 'html' in ct or r.text[:100].lower().startswith('<!doctype'):
            debug_warnings.append('⚠️ הורדת XLSX החזירה HTML במקום קובץ. ייתכן redirect/חסימת גישה.')
            return pd.DataFrame()

        excel = pd.ExcelFile(io.BytesIO(r.content), engine='openpyxl')
        frames: list[pd.DataFrame] = []
        for sheet_name in excel.sheet_names:
            try:
                raw_sheet = pd.read_excel(excel, sheet_name=sheet_name, header=None, dtype=str)
                smart = _read_excel_smart(raw_sheet)
                norm = _normalise_sheet_df(smart, sheet_name, debug_warnings)
                if not norm.empty:
                    frames.append(norm)
            except Exception as e:
                debug_warnings.append(f'⚠️ גליון **{sheet_name}** ב-XLSX: {e}')
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        debug_warnings.append(f'⚠️ טעינת XLSX ציבורי נכשלה: {e}')
        return pd.DataFrame()


def _discover_sheet_gids(sheet_id: str, max_probe: int = 20) -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    # Keep only as fallback. Works often enough for public sheets.
    try:
        r = requests.get(f'https://docs.google.com/spreadsheets/d/{sheet_id}/edit', timeout=20)
        html = r.text
        patterns = [
            r'"sheetId":(\d+).*?"title":"([^"]+)"',
            r'"title":"([^"]+)".*?"sheetId":(\d+)',
        ]
        for pattern in patterns:
            for a, b in re.findall(pattern, html, flags=re.S):
                if pattern.startswith(r'"sheetId"'):
                    gid, title = a, b
                else:
                    title, gid = a, b
                pair = (_clean(title), int(gid))
                if pair not in found:
                    found.append(pair)
    except Exception as e:
        logger.warning('HTML gid discovery failed: %s', e)

    if found:
        return found

    for gid in range(max_probe):
        try:
            rr = requests.get(_csv_export_url(sheet_id, gid), timeout=15)
            ct = rr.headers.get('Content-Type', '').lower()
            if rr.status_code != 200 or 'html' in ct:
                continue
            if len(rr.text.strip()) > 20:
                found.append((f'גליון_{gid}', gid))
        except Exception:
            continue

    return found if found else [('גליון_0', 0)]


def _load_sheet_via_csv(sheet_id: str, gid: int, sheet_name: str, debug_warnings: list[str]) -> pd.DataFrame:
    try:
        r = requests.get(_csv_export_url(sheet_id, gid), timeout=25, allow_redirects=True)
        if r.status_code in (401, 403):
            debug_warnings.append(
                f'🔒 גליון **{sheet_name}** (gid={gid}): שגיאת הרשאה (HTTP {r.status_code}).'
            )
            return pd.DataFrame()
        if r.status_code != 200:
            debug_warnings.append(f'⚠️ גליון **{sheet_name}** (gid={gid}): HTTP {r.status_code}')
            return pd.DataFrame()
        ct = r.headers.get('Content-Type', '').lower()
        if 'html' in ct or r.text.strip().lower().startswith('<!doctype'):
            debug_warnings.append(
                f'⚠️ גליון **{sheet_name}** (gid={gid}): הייצוא החזיר HTML במקום CSV.'
            )
            return pd.DataFrame()
        raw = _read_csv_smart(r.text)
        return _normalise_sheet_df(raw, sheet_name, debug_warnings)
    except Exception as e:
        debug_warnings.append(f'⚠️ גליון **{sheet_name}** (gid={gid}): {e}')
        return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Optional private transport via gspread
# ──────────────────────────────────────────────────────────────────────────────
def _load_via_gspread(sheet_url: str, debug_warnings: list[str]) -> pd.DataFrame:
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_dict = dict(st.secrets['gcp_service_account'])
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly',
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(sheet_url)

        frames: list[pd.DataFrame] = []
        for ws in sh.worksheets():
            try:
                data = ws.get_all_values()
                if not data:
                    continue
                raw_df = pd.DataFrame(data, dtype=str)
                smart = _read_excel_smart(raw_df)
                norm = _normalise_sheet_df(smart, ws.title, debug_warnings)
                if not norm.empty:
                    frames.append(norm)
            except Exception as e:
                debug_warnings.append(f"gspread: גליון '{ws.title}' — {e}")
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception as e:
        debug_warnings.append(f'gspread נכשל: {e}')
        return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_allocation_history(sheet_url: str) -> tuple[pd.DataFrame, list[str]]:
    debug_warnings: list[str] = []

    if not sheet_url or not sheet_url.strip():
        return pd.DataFrame(), ['לא הוגדר קישור ל-Google Sheets']

    # Optional private access first if configured
    has_sa = hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets
    if has_sa:
        df_gs = _load_via_gspread(sheet_url, debug_warnings)
        if not df_gs.empty:
            df_gs = _collapse_monthly_duplicates(df_gs)
            return df_gs, debug_warnings
        debug_warnings.append('gspread לא הצליח — מנסה טעינה ציבורית')

    try:
        sheet_id = _extract_sheet_id(sheet_url)
    except ValueError as e:
        return pd.DataFrame(), [str(e)]

    # Preferred public path: whole workbook as XLSX
    df_xlsx = _load_via_public_xlsx(sheet_id, debug_warnings)
    if not df_xlsx.empty:
        df_xlsx = _collapse_monthly_duplicates(df_xlsx)
        return df_xlsx, debug_warnings

    # Fallback only
    frames: list[pd.DataFrame] = []
    for name, gid in _discover_sheet_gids(sheet_id):
        df_sheet = _load_sheet_via_csv(sheet_id, gid, name, debug_warnings)
        if not df_sheet.empty:
            frames.append(df_sheet)

    if not frames:
        return pd.DataFrame(), debug_warnings + ['לא נטענו נתונים מאף גליון']

    df = pd.concat(frames, ignore_index=True)
    df = _collapse_monthly_duplicates(df)
    return df, debug_warnings
