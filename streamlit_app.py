from __future__ import annotations
# -*- coding: utf-8 -*-
# Profit Mix Optimizer – v3.8
# שיפורים v3.8: תמיכה בפוליסות חיסכון, בורר סוג מוצר, חילוץ חכם של שם מנהל
# - פיצ׳ר 1: ייבוא דו"ח מסלקה (פורטפוליו קיים)
# - פיצ׳ר 2: כפתורי מסלול מהיר (מניות/אג"ח/חו"ל/ישראל/מט"ח)
# - פיצ׳ר 3: נעילת קרן עם סכום/משקל קבוע
# - פיצ׳ר 4: השוואת מצב מוצע מול מצב קיים בתוצאות

import itertools, math, os, re, html, io, traceback
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="Profit Mix Optimizer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

import streamlit as _st_check
_st_version = tuple(int(x) for x in _st_check.__version__.split(".")[:2])

def _safe_plotly(fig, key=None):
    try:
        st.plotly_chart(fig, use_container_width=True, key=key)
    except TypeError:
        try:
            st.plotly_chart(fig, key=key)
        except TypeError:
            st.plotly_chart(fig)

# ─────────────────────────────────────────────
# CSS – Premium Fintech (v3.6)
# ─────────────────────────────────────────────
st.markdown("""
<style>
/* ── Base RTL ── */
html, body, [class*="css"] {
  direction: rtl; text-align: right;
  font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
  background: #F5F7FA;
}
div[data-baseweb="slider"], div[data-baseweb="slider"] * { direction: ltr !important; }
section[data-testid="stSidebar"] { display: none !important; }
.block-container { padding: 3.8rem 1.2rem 3rem !important; max-width: 1180px; }
div[data-testid="stVerticalBlock"] { gap: 0.25rem; }
/* tighten metric spacing */
div[data-testid="stMetric"] { padding: 4px 0 !important; }
div[data-testid="stMetricLabel"] { font-size: 11px !important; color: #6B7280 !important; }
div[data-testid="stMetricValue"] { font-size: 16px !important; font-weight: 800 !important; }

/* ── Header ── */
.pmo-header {
  background: #1F3A5F; color: #fff;
  border-radius: 12px; padding: 14px 20px 12px;
  display: flex; align-items: center; justify-content: space-between;
  flex-wrap: wrap; gap: 10px; margin-bottom: 8px;
}
.pmo-header-left { display: flex; align-items: center; gap: 12px; }
.pmo-logo-box {
  width: 38px; height: 38px; border-radius: 9px;
  background: #3A7AFE; display: flex; align-items: center;
  justify-content: center; font-size: 19px; flex-shrink: 0;
}
.pmo-title   { font-size: 20px; font-weight: 800; margin: 0; letter-spacing: -0.3px; }
.pmo-sub     { font-size: 11px; color: #93b4e0; margin: 1px 0 0; }
.pmo-kpis    { display: flex; gap: 10px; }
.pmo-kpi     { text-align: center; }
.pmo-kpi-val { font-size: 17px; font-weight: 800; color: #7dd3fc; display: block; line-height: 1.1; }
.pmo-kpi-lbl { font-size: 10px; color: #93b4e0; white-space: nowrap; }

/* ── Quick filters ── */
.qf-wrap {
  display: flex; gap: 6px; flex-wrap: wrap;
  padding: 8px 0 6px; margin-bottom: 6px;
}
/* Streamlit radio styled as pills */
.nav-bar div[role="radiogroup"] {
  display: flex !important; gap: 6px !important; flex-wrap: wrap !important;
}
.nav-bar div[role="radiogroup"] label {
  display: inline-flex !important; align-items: center !important;
  padding: 5px 13px !important; border-radius: 999px !important;
  border: 1.5px solid #D1D5DB !important; background: #fff !important;
  font-size: 13px !important; font-weight: 600 !important;
  cursor: pointer !important; white-space: nowrap !important;
  color: #374151 !important; transition: all 0.15s !important; margin: 0 !important;
}
.nav-bar div[role="radiogroup"] label:hover {
  background: #EFF6FF !important; border-color: #3A7AFE !important; color: #1F3A5F !important;
}
.nav-bar div[role="radiogroup"] label:has(input:checked) {
  background: #1F3A5F !important; color: #ffffff !important;
  border-color: #1F3A5F !important; box-shadow: 0 2px 6px rgba(31,58,95,0.25) !important;
}
.nav-bar div[role="radiogroup"] input[type="radio"] { display: none !important; }
.nav-bar div[role="radiogroup"] p { margin: 0 !important; }

/* ── Cards ── */
.card {
  background: #fff; border: 1px solid #E5EAF2; border-radius: 12px;
  padding: 16px 18px 14px; margin-bottom: 10px;
}
.card-title {
  font-size: 14px; font-weight: 800; color: #111827;
  margin: 0 0 12px; display: flex; align-items: center; gap: 6px;
}
.card-sub { font-size: 11px; color: #6B7280; margin-bottom: 8px; }

/* ── Mix builder ── */
.mix-total {
  background: #F0F4FF; border: 1px solid #C7D7FF;
  border-radius: 8px; padding: 8px 12px; margin: 8px 0;
  font-size: 12px; display: flex; justify-content: space-between;
}
.mix-total .t-ok  { color: #15A46E; font-weight: 800; }
.mix-total .t-warn{ color: #B7791F; font-weight: 800; }
.mix-total .t-err { color: #DC2626; font-weight: 800; }

/* ── Best result card ── */
.br-score {
  font-size: 42px; font-weight: 900; color: #1F3A5F;
  line-height: 1; margin: 0;
}
.br-score-lbl { font-size: 11px; color: #6B7280; margin-top: 2px; }
.br-managers  { font-size: 12px; color: #374151; margin: 8px 0 4px; }
.br-tracks    { font-size: 11px; color: #6B7280; margin-bottom: 10px; }
.br-chips     { display: flex; gap: 5px; flex-wrap: wrap; margin-bottom: 10px; }
.br-chip {
  padding: 3px 9px; border-radius: 999px; font-size: 11px; font-weight: 700;
  background: #EFF6FF; color: #1F3A5F; border: 1px solid #C7D7FF;
}

/* ── Results strip ── */
.res-strip {
  background: #F0F4FF; border: 1px solid #C7D7FF;
  border-radius: 8px; padding: 7px 14px;
  font-size: 12px; color: #374151; margin: 6px 0;
  display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
}
.res-strip b { color: #1F3A5F; }

/* ── Results table ── */
.res-tbl {
  width: 100%; border-collapse: collapse;
  font-size: 12.5px; direction: rtl;
}
.res-tbl th {
  background: #1F3A5F; color: #F0F4FF;
  padding: 7px 10px; text-align: right;
  white-space: nowrap; font-weight: 700; font-size: 11.5px;
}
.res-tbl td {
  padding: 7px 10px; border-bottom: 1px solid #F1F5F9;
  text-align: right; vertical-align: middle;
}
.res-tbl tr.sel-row td { background: #EFF6FF !important; }
.res-tbl tr:hover td  { background: #F9FAFB; }
.res-tbl td.num  { text-align: center; }
.res-tbl td.name { font-weight: 700; color: #111827; }

/* ── Stats bars ── */
.stat-bar-row { display: flex; align-items: center; gap: 8px; margin: 4px 0; direction: rtl; }
.stat-bar     { height: 14px; border-radius: 3px; min-width: 3px; }

/* ── Misc ── */
.score-tip {
  background: #FFFBEB; border: 1px solid #FDE68A;
  border-radius: 8px; padding: 7px 11px; font-size: 11.5px; color: #78350F; margin: 5px 0;
}
.pw-wrap  { max-width: 300px; margin: 60px auto; text-align: center; }
.pw-title { font-size: 22px; font-weight: 800; margin-bottom: 4px; }
.pw-sub   { font-size: 12px; opacity: 0.7; margin-bottom: 14px; }
.pw-warn  { font-size: 11px; color: #B45309; background: #FEF3C7; border-radius: 6px; padding: 4px 9px; margin-top: 7px; }
.change-badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 700; }
.change-low   { background: #DCFCE7; color: #166534; }
.change-med   { background: #FEF3C7; color: #92400E; }
.change-high  { background: #FEE2E2; color: #991B1B; }

/* ── Mobile responsive ── */
@media (max-width: 768px) {
  .pmo-header { flex-direction: column; align-items: flex-start; }
  .pmo-kpis   { flex-wrap: wrap; }
}
/* dark mode */
@media (prefers-color-scheme: dark) {
  html, body, [class*="css"] { background: #0d1117; }
  .card { background: #161b22; border-color: #30363d; }
  .card-title { color: #f0f6ff; }
  .res-tbl td { border-color: #1e293b; color: #cbd5e1; }
  .res-tbl th { background: #1e3a8a; }
  .res-strip, .mix-total { background: #0f1e3d; border-color: #1e3a5f; color: #cbd5e1; }
  .br-chip { background: #0f1e3d; border-color: #1e3a5f; color: #93c5fd; }
  .score-tip { background: #451a03; border-color: #92400e; color: #fde68a; }
}
div[data-testid="stDataFrame"] * { direction: rtl; text-align: right; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _esc(x) -> str:
    try:
        return html.escape("" if x is None else str(x), quote=True)
    except Exception:
        return ""

def _to_float(x) -> float:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = re.sub(r"[^\d.\-]", "", str(x).replace(",", "").replace("−", "-"))
    if s in ("", "-", "."):
        return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan

def _fmt_pct(x, decimals=2) -> str:
    try:
        return f"{float(x):.{decimals}f}%"
    except Exception:
        return "—"

def _fmt_num(x, fmt="{:.2f}") -> str:
    try:
        return fmt.format(float(x))
    except Exception:
        return "—"


# ─────────────────────────────────────────────
# Password Gate
# ─────────────────────────────────────────────
def _check_password() -> bool:
    if st.session_state.get("auth_ok", False):
        return True
    is_default = True
    if hasattr(st, "secrets") and "APP_PASSWORD" in st.secrets:
        correct = str(st.secrets["APP_PASSWORD"])
        is_default = False
    else:
        correct = os.getenv("APP_PASSWORD", "1234")

    st.markdown("""
    <div class="pw-wrap">
      <div class="pw-title">🔒 כניסה</div>
      <div class="pw-sub">האפליקציה מוגנת בסיסמה</div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        pwd = st.text_input("סיסמה", type="password", placeholder="••••••••", label_visibility="collapsed")
        if st.button("כניסה", use_container_width=True, type="primary"):
            if pwd == correct:
                st.session_state["auth_ok"] = True
                st.rerun()
            else:
                st.error("סיסמה שגויה")
        if is_default:
            st.markdown(
                '<div class="pw-warn">⚠️ סיסמה ברירת מחדל (1234). הגדר APP_PASSWORD ב-Streamlit Secrets בייצור!</div>',
                unsafe_allow_html=True
            )
    st.stop()

_check_password()


# ─────────────────────────────────────────────
# Google Sheets – מקורות נתונים
# ─────────────────────────────────────────────
FUNDS_GSHEET_ID    = "1ty_tqcyGqmVI4pQZetHHKd-cC0O2HCpD2dbpNpYlPtY"
POLICIES_GSHEET_ID = "11C0gpE_ugoGkzuljRiDW4Zdyk11oYftm2OMwrT-tIII"
SERVICE_GSHEET_ID  = "1FSgvIG6VsJxB5QPY6fmwAwGc1TYLB0KXg-7ckkD_RJQ"

# ─────────────────────────────────────────────
# Voting – Google Sheets via Service Account
# ─────────────────────────────────────────────
VOTES_SHEET_NAME = "votes"

def _get_votes_worksheet():
    """Return the gspread worksheet for votes, or None if not configured."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ]
        # Credentials stored in Streamlit Secrets as [gcp_service_account]
        if not (hasattr(st, "secrets") and "gcp_service_account" in st.secrets):
            return None
        sa_info = dict(st.secrets["gcp_service_account"])
        creds   = Credentials.from_service_account_info(sa_info, scopes=scopes)
        client  = gspread.authorize(creds)
        sheet   = client.open_by_key(FUNDS_GSHEET_ID)
        try:
            ws = sheet.worksheet(VOTES_SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet(title=VOTES_SHEET_NAME, rows=2000, cols=8)
            ws.append_row(["timestamp","alternative","managers","tracks",
                           "n_funds","mix_policy","session_hash"], value_input_option="RAW")
        return ws
    except Exception as _e:
        return None


def _write_vote(alternative: str, managers: str, tracks: str) -> bool:
    """Write a single vote row. Returns True on success."""
    try:
        ws = _get_votes_worksheet()
        if ws is None:
            return False
        import hashlib, uuid
        session_id = st.session_state.get("_session_id")
        if not session_id:
            session_id = hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()[:10]
            st.session_state["_session_id"] = session_id
        ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            alternative,
            managers,
            tracks,
            str(st.session_state.get("n_funds", 2)),
            str(st.session_state.get("mix_policy", "")),
            session_id,
        ], value_input_option="RAW")
        return True
    except Exception:
        return False


@st.cache_data(ttl=300, show_spinner=False)
def _load_votes_cached() -> pd.DataFrame:
    """Load all votes from the sheet (cached 5 min)."""
    try:
        ws = _get_votes_worksheet()
        if ws is None:
            return pd.DataFrame()
        records = ws.get_all_records()
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def _render_votes_stats():
    """Render comprehensive voting statistics dashboard."""
    df = _load_votes_cached()

    if df.empty:
        st.info("עדיין אין נתוני הצבעות. היה הראשון לבחור חלופה!")
        return

    from datetime import timedelta
    cutoff_30 = datetime.now() - timedelta(days=30)
    cutoff_7  = datetime.now() - timedelta(days=7)
    df30 = df[df["timestamp"] >= cutoff_30].copy()
    df7  = df[df["timestamp"] >= cutoff_7].copy()

    total_all = len(df)
    total_30  = len(df30)
    total_7   = len(df7)

    # ── Summary KPIs ──
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("סה״כ בחירות", f"{total_all:,}")
    c2.metric("30 יום אחרונים", f"{total_30:,}")
    c3.metric("7 ימים אחרונים", f"{total_7:,}")
    # unique sessions
    if "session_hash" in df30.columns:
        unique_users = df30["session_hash"].nunique()
        c4.metric("משתמשים ייחודיים (30י׳)", f"{unique_users:,}")

    if df30.empty:
        st.caption("אין הצבעות ב-30 הימים האחרונים עדיין.")
        return

    st.markdown("---")
    tab1, tab2, tab3, tab4 = st.tabs(["🏆 חלופות", "🏢 מנהלים", "📋 מסלולים", "⏱️ טרנד"])

    bar_colors_alt = {
        "חלופה משוקללת": "#2563eb", "הכי מדויקת": "#16a34a",
        "שארפ מקסימלי": "#ea580c", "שירות מוביל": "#7c3aed"
    }

    # ── Tab 1: Alternatives ──
    with tab1:
        alt_counts = df30["alternative"].value_counts().reset_index()
        alt_counts.columns = ["חלופה", "הצבעות"]
        alt_counts["אחוז"] = (alt_counts["הצבעות"] / total_30 * 100).round(1)
        colors = [bar_colors_alt.get(a, "#64748b") for a in alt_counts["חלופה"]]
        fig = go.Figure(go.Bar(
            x=alt_counts["חלופה"], y=alt_counts["הצבעות"],
            marker_color=colors,
            text=alt_counts["אחוז"].apply(lambda v: f"{v:.1f}%"),
            textposition="outside",
        ))
        fig.update_layout(height=260, margin=dict(t=20,b=40,l=5,r=5),
            xaxis=dict(tickfont=dict(size=11)),
            yaxis=dict(title="בחירות", gridcolor="#F1F5F9"),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        _safe_plotly(fig, key="stats_alts")
        st.dataframe(alt_counts.set_index("חלופה"), use_container_width=True)

    # ── Tab 2: Managers ──
    with tab2:
        if "managers" in df30.columns:
            all_mgrs_voted = []
            for cell in df30["managers"].dropna():
                for m in str(cell).replace("،","|").split("|"):
                    m = m.strip()
                    if m:
                        all_mgrs_voted.append(m)
            if all_mgrs_voted:
                mc = pd.Series(all_mgrs_voted).value_counts().head(10).reset_index()
                mc.columns = ["מנהל", "ספירה"]
                fig2 = go.Figure(go.Bar(
                    x=mc["מנהל"], y=mc["ספירה"],
                    marker_color="#3A7AFE",
                    text=mc["ספירה"], textposition="outside",
                ))
                fig2.update_layout(height=280, margin=dict(t=20,b=60,l=5,r=5),
                    xaxis=dict(tickangle=-30, tickfont=dict(size=10)),
                    yaxis=dict(title="ספירה", gridcolor="#F1F5F9"),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                _safe_plotly(fig2, key="stats_mgrs")
                st.dataframe(mc.set_index("מנהל"), use_container_width=True)
            else:
                st.info("אין נתוני מנהלים בהצבעות")
        else:
            st.info("עמודת מנהלים חסרה בנתונים")

    # ── Tab 3: Tracks ──
    with tab3:
        if "tracks" in df30.columns:
            all_tracks_voted = []
            for cell in df30["tracks"].dropna():
                for t in str(cell).split("|"):
                    t = t.strip()
                    if t:
                        all_tracks_voted.append(t)
            if all_tracks_voted:
                tc = pd.Series(all_tracks_voted).value_counts().head(12).reset_index()
                tc.columns = ["מסלול", "ספירה"]
                fig3 = go.Figure(go.Bar(
                    x=tc["מסלול"], y=tc["ספירה"],
                    marker_color="#15A46E",
                    text=tc["ספירה"], textposition="outside",
                ))
                fig3.update_layout(height=300, margin=dict(t=20,b=80,l=5,r=5),
                    xaxis=dict(tickangle=-35, tickfont=dict(size=10)),
                    yaxis=dict(title="ספירה", gridcolor="#F1F5F9"),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                _safe_plotly(fig3, key="stats_tracks")
                st.dataframe(tc.set_index("מסלול"), use_container_width=True)
            else:
                st.info("אין נתוני מסלולים בהצבעות")
        else:
            st.info("עמודת מסלולים חסרה בנתונים")

    # ── Tab 4: Trend (daily votes last 30 days) ──
    with tab4:
        df30["date"] = df30["timestamp"].dt.date
        daily = df30.groupby("date").size().reset_index(name="הצבעות")
        if not daily.empty:
            fig4 = go.Figure(go.Scatter(
                x=daily["date"], y=daily["הצבעות"],
                mode="lines+markers",
                line=dict(color="#3A7AFE", width=2),
                marker=dict(size=5, color="#1F3A5F"),
                fill="tozeroy", fillcolor="rgba(58,122,254,0.08)",
            ))
            fig4.update_layout(height=220, margin=dict(t=10,b=40,l=5,r=5),
                xaxis=dict(title="תאריך", tickfont=dict(size=10)),
                yaxis=dict(title="בחירות ביום", gridcolor="#F1F5F9"),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            _safe_plotly(fig4, key="stats_trend")

    st.caption(f"נתונים מתעדכנים כל 5 דקות · עדכון אחרון: {datetime.now().strftime('%H:%M:%S')} · מציג 30 ימים אחרונים")


PARAM_ALIASES = {
    # קרנות השתלמות
    "stocks":   ["סך חשיפה למניות", "מניות", "חשיפה למניות"],
    "foreign":  ['סך חשיפה לנכסים המושקעים בחו"ל', "סך חשיפה לנכסים המושקעים בחו׳ל",
                 'חו"ל', "חו׳ל", "חשיפה לנכסים המושקעים בחו"],
    "fx":       ['חשיפה למט"ח', 'מט"ח', "מט׳׳ח"],
    "illiquid": ["נכסים לא סחירים", "לא סחירים", "לא-סחיר", "לא סחיר"],
    "sharpe":   ["מדד שארפ", "שארפ"],
}

# ── Policy: map known sub-managers (ordered – longer matches first) ──
_POLICY_SUB_MGR = [
    ("מיטב",    "מיטב"),
    ("אנליסט",  "אנליסט"),
    ("אקסלנס",  "אקסלנס"),
    ("מור",     "מור"),
]
_POLICY_INSURER_PREFIXES = ["הכשרה", "הפניקס"]
_POLICY_DIRECT = [
    ("הראל",     "הראל"),
    ("מגדל",     "מגדל"),
    ("כלל",      "כלל"),
    ("מנורה",    "מנורה"),
    ("איילון",   "איילון"),
    ("אי.די.אי", "IDI"),
    ("הפניקס",   "הפניקס"),
    ("הכשרה",    "הכשרה"),
]

def _extract_manager_policy(name: str) -> str:
    """Extract investment manager from a policy fund name.
    'הכשרה ... מנוהל באמצעות מיטב ...' → 'מיטב (הכשרה)'
    'הפניקס-אקסלנס ...'               → 'אקסלנס (הפניקס)'
    'הכשרה ... כללי' (no sub-manager) → 'הכשרה'
    """
    s = str(name).strip()
    found_sub = None
    for kw, sub in _POLICY_SUB_MGR:
        if kw in s:
            found_sub = sub
            break
    if found_sub:
        for ins in _POLICY_INSURER_PREFIXES:
            if s.startswith(ins) or f"-{ins}" in s:
                return f"{found_sub} ({ins})"
        return found_sub
    for prefix, mgr in _POLICY_DIRECT:
        if s.startswith(prefix):
            return mgr
    return _extract_manager(s)  # fallback

# ── פיצ׳ר 2: quick profiles ───────────────────
QUICK_PROFILES = {
    "📈 מניות":  {"stocks_min": 90},
    '🏦 אג"ח':   {"stocks_max": 10, "illiquid_max": 10},
    "🌍 חו״ל":   {"foreign_min": 90},
    "🇮🇱 ישראל": {"foreign_max": 10},
    '💱 מט"ח':   {"fx_min": 90},
}


# ─────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────
def _match_param(row_name: str, key: str) -> bool:
    rn = str(row_name).strip()
    return any(a in rn for a in PARAM_ALIASES[key])

def _extract_manager(fund_name: str) -> str:
    name = str(fund_name).strip()
    for splitter in [" קרן", " השתלמות", " -", "-", "  "]:
        if splitter in name:
            head = name.split(splitter)[0].strip()
            if head:
                return head
    return name.split()[0] if name.split() else name

def _gsheet_to_bytes(sheet_id: str) -> Tuple[bytes, str]:
    import requests as _req
    urls = [
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx",
        f"https://docs.google.com/feeds/download/spreadsheets/Export?key={sheet_id}&exportFormat=xlsx",
    ]
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    last_err = ""
    for url in urls:
        try:
            resp = _req.get(url, headers=headers, allow_redirects=True, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 500:
                if resp.content[:2] == b"PK":
                    return resp.content, ""
                else:
                    preview = resp.content[:120].decode("utf-8", errors="ignore").replace("\n"," ") if resp.content else ""
                    last_err = (
                        f"קוד 200 אבל התקבל HTML במקום XLSX (גיליון {sheet_id[:20]}). "
                        "בדוק ש-Share מוגדר 'Anyone with the link' כ-Viewer. "
                        f"Preview: {preview[:80]}"
                    )
            else:
                last_err = f"HTTP {resp.status_code} מ-{url[:60]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
    return b"", last_err

def _load_service_scores(xlsx_bytes: bytes) -> Tuple[Dict[str, float], str]:
    try:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), header=None)
    except Exception as e:
        return {}, f"שגיאה בטעינת ציוני שירות: {e}"
    if df.empty:
        return {}, "גיליון ציוני שירות ריק"

    try:
        df_hdr = pd.read_excel(io.BytesIO(xlsx_bytes))
        if not df_hdr.empty:
            cols = [str(c).lower().strip() for c in df_hdr.columns]
            df_hdr.columns = cols
            if "provider" in df_hdr.columns and "score" in df_hdr.columns:
                out = {}
                for _, r in df_hdr.iterrows():
                    p = _extract_manager(str(r["provider"]).strip())
                    sc = _to_float(r["score"])
                    if p and not math.isnan(sc):
                        out[p] = float(sc)
                if out:
                    return out, ""
    except Exception:
        pass

    df2 = df.copy().dropna(how="all").dropna(how="all", axis=1)
    if df2.shape[0] >= 2 and df2.shape[1] >= 2:
        first_col = df2.iloc[:, 0].astype(str).str.strip().str.lower()
        prov_rows = df2.index[first_col.eq("provider")].tolist()
        combo_cell = df2.iloc[:, 0].astype(str).str.strip().str.lower()
        combo_rows = df2.index[combo_cell.str.contains("provider") & combo_cell.str.contains("score")].tolist()
        for r0 in combo_rows:
            if r0 not in prov_rows:
                prov_rows.append(r0)
        for r0 in prov_rows[:3]:
            if r0 + 1 in df2.index:
                header = df2.loc[r0].tolist()
                values = df2.loc[r0 + 1].tolist()
                tag = str(values[0]).strip().lower()
                if tag in {"score", "ציון", "שירות ואיכות", "ציון שירות"} or tag in {"nan", "", "none"}:
                    out = {}
                    for name, val in zip(header[1:], values[1:]):
                        p = _extract_manager(str(name).strip())
                        sc = _to_float(val)
                        if p and not math.isnan(sc):
                            out[p] = float(sc)
                    if out:
                        return out, ""

    return {}, "מבנה גיליון שירות לא מזוהה"


# ─────────────────────────────────────────────
# פיצ׳ר 1: Parse clearing house report
# ─────────────────────────────────────────────
def parse_clearing_report(xlsx_bytes: bytes) -> Tuple[Optional[Dict], str]:
    """
    מנתח דו"ח מסלקה (Excel) ומחזיר dict עם:
      {
        "holdings": [{"fund": str, "manager": str, "track": str, "amount": float, "weight_pct": float}],
        "total_amount": float,
        "baseline": {"foreign": float, "stocks": float, "fx": float, "illiquid": float}
      }
    או None + הודעת שגיאה.
    """
    AMOUNT_ALIASES  = ["יתרה", "ערך", "סכום", "balance", "amount", "שווי"]
    FUND_ALIASES    = ["שם הקרן", "קרן", "שם מוצר", "fund", "product", "שם הקופה", "שם הגוף"]
    MANAGER_ALIASES = ["מנהל", "גוף מנהל", "בית השקעות", "manager", "provider", "מנהל ההשקעות"]
    TRACK_ALIASES   = ["מסלול", "track", "שם מסלול"]

    try:
        xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    except Exception as e:
        return None, f"לא ניתן לפתוח את הקובץ: {e}"

    all_records = []

    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet, header=None)
        except Exception:
            continue
        if df.empty or df.shape[0] < 2:
            continue

        # Find header row (contains at least one known alias)
        header_idx = None
        for i in range(min(10, len(df))):
            row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist()]
            matches = sum(
                1 for v in row_vals
                if any(a.lower() in v for a in AMOUNT_ALIASES + FUND_ALIASES + MANAGER_ALIASES)
            )
            if matches >= 2:
                header_idx = i
                break

        if header_idx is None:
            continue

        df_clean = df.iloc[header_idx:].copy().reset_index(drop=True)
        df_clean.columns = [str(c).strip() for c in df_clean.iloc[0].tolist()]
        df_clean = df_clean.iloc[1:].reset_index(drop=True)

        def _find_col(aliases):
            for col in df_clean.columns:
                col_l = col.lower()
                for a in aliases:
                    if a.lower() in col_l:
                        return col
            return None

        fund_col    = _find_col(FUND_ALIASES)
        manager_col = _find_col(MANAGER_ALIASES)
        amount_col  = _find_col(AMOUNT_ALIASES)
        track_col   = _find_col(TRACK_ALIASES)

        if not fund_col and not manager_col:
            continue
        if not amount_col:
            continue

        for _, row in df_clean.iterrows():
            fund_name    = str(row.get(fund_col, "") or "").strip() if fund_col else ""
            manager_name = str(row.get(manager_col, "") or "").strip() if manager_col else ""
            track_name   = str(row.get(track_col, "") or "").strip() if track_col else ""
            amount_val   = _to_float(row.get(amount_col, np.nan))

            if not fund_name and not manager_name:
                continue
            if math.isnan(amount_val) or amount_val <= 0:
                continue

            if not manager_name and fund_name:
                manager_name = _extract_manager(fund_name)

            all_records.append({
                "fund":    fund_name or manager_name,
                "manager": manager_name or _extract_manager(fund_name),
                "track":   track_name,
                "amount":  amount_val,
            })

    if not all_records:
        return None, (
            "לא נמצאו נתונים בקובץ. וודא שהקובץ הוא דו\"ח מסלקה תקני "
            "עם עמודות שם קרן/מנהל וסכום/יתרה."
        )

    total = sum(r["amount"] for r in all_records)
    for r in all_records:
        r["weight_pct"] = round(r["amount"] / total * 100, 2) if total > 0 else 0.0

    return {
        "holdings":     all_records,
        "total_amount": total,
        "baseline":     None,  # יחושב בהמשך מהנתונים של האפליקציה
    }, ""


def _compute_baseline_from_holdings(holdings: List[Dict], df_long: pd.DataFrame) -> Optional[Dict]:
    """מחשב פרמטרי חשיפה משוקללים לפורטפוליו הנוכחי."""
    if not holdings:
        return None
    total = sum(r["amount"] for r in holdings)
    if total <= 0:
        return None

    result = {"foreign": 0.0, "stocks": 0.0, "fx": 0.0, "illiquid": 0.0, "sharpe": 0.0, "service": 0.0}
    matched = 0

    for h in holdings:
        w = h["amount"] / total
        # חפש התאמה בנתוני האפליקציה (לפי שם מנהל או שם קרן)
        fund_match = df_long[df_long["fund"].str.lower().str.strip() == h["fund"].lower().strip()]
        if fund_match.empty:
            mgr_match = df_long[df_long["manager"].str.lower().str.strip() == h["manager"].lower().strip()]
            if not mgr_match.empty:
                fund_match = mgr_match.head(1)
        if fund_match.empty:
            # fuzzy: check if any word matches
            words = h["manager"].lower().split()
            for word in words:
                if len(word) > 2:
                    m = df_long[df_long["manager"].str.lower().str.contains(word, na=False)]
                    if not m.empty:
                        fund_match = m.head(1)
                        break

        if not fund_match.empty:
            r = fund_match.iloc[0]
            for key in ["foreign", "stocks", "fx", "illiquid", "sharpe", "service"]:
                val = _to_float(r.get(key, np.nan))
                if not math.isnan(val):
                    result[key] += val * w
            matched += 1

    return result if matched > 0 else None


@st.cache_data(show_spinner=False, ttl=900)
def load_funds_long(funds_id: str, service_id: str, product_type: str = "קרנות השתלמות") -> Tuple[pd.DataFrame, Dict[str, float], List[str]]:
    warnings: List[str] = []

    svc_bytes, svc_err = _gsheet_to_bytes(service_id)
    if svc_err:
        warnings.append(svc_err)
        svc = {}
    else:
        svc, parse_err = _load_service_scores(svc_bytes)
        if parse_err:
            warnings.append(parse_err)

    funds_bytes, funds_err = _gsheet_to_bytes(funds_id)
    if funds_err:
        return pd.DataFrame(), svc, warnings + [funds_err]

    try:
        xls = pd.ExcelFile(io.BytesIO(funds_bytes))
    except Exception as e:
        return pd.DataFrame(), svc, warnings + [f"שגיאה בפתיחת גיליון קרנות: {e}"]

    records: List[Dict] = []
    for sh in xls.sheet_names:
        sh_str = str(sh)
        if re.search(r"ניהול\s*אישי", sh_str) or re.search(r"(^|[^a-z])ira([^a-z]|$)", sh_str.lower()):
            continue
        try:
            df = pd.read_excel(xls, sheet_name=sh, header=None)
        except Exception as e:
            warnings.append(f"גיליון '{sh}': שגיאת קריאה – {e}")
            continue
        if df.empty:
            continue

        header_row = df.iloc[0].tolist()
        if not str(header_row[0]).strip().startswith("פרמטר"):
            idxs = df.index[df.iloc[:, 0].astype(str).str.contains("פרמטר", na=False)].tolist()
            if not idxs:
                continue
            df = df.iloc[idxs[0]:].reset_index(drop=True)
            header_row = df.iloc[0].tolist()

        fund_names = [c for c in header_row[1:] if str(c).strip() and str(c).strip() != "nan"]
        if not fund_names:
            continue

        param_col = df.iloc[1:, 0].astype(str).tolist()

        def row_for(key: str) -> Optional[int]:
            for i, rn in enumerate(param_col, start=1):
                if _match_param(rn, key):
                    return i
            return None

        ridx = {k: row_for(k) for k in ["stocks", "foreign", "fx", "illiquid", "sharpe"]}
        if ridx["foreign"] is None and ridx["stocks"] is None:
            continue

        for j, fname in enumerate(fund_names, start=1):
            _extract_fn = _extract_manager_policy if product_type == "פוליסות חיסכון" else _extract_manager
            manager = _extract_fn(fname)
            svc_score = svc.get(manager)
            # fuzzy fallback: try partial match if exact not found
            if svc_score is None:
                for svc_key in svc:
                    if svc_key in manager or manager in svc_key:
                        svc_score = svc[svc_key]
                        break
            rec = {
                "track":    sh_str,
                "fund":     str(fname).strip(),
                "manager":  manager,
                "stocks":   _to_float(df.iloc[ridx["stocks"],   j]) if ridx["stocks"]   is not None else np.nan,
                "foreign":  _to_float(df.iloc[ridx["foreign"],  j]) if ridx["foreign"]  is not None else np.nan,
                "fx":       _to_float(df.iloc[ridx["fx"],       j]) if ridx["fx"]       is not None else np.nan,
                "illiquid": _to_float(df.iloc[ridx["illiquid"], j]) if ridx["illiquid"] is not None else np.nan,
                "sharpe":   _to_float(df.iloc[ridx["sharpe"],   j]) if ridx["sharpe"]   is not None else np.nan,
                "service_missing": svc_score is None,
            }
            if all(math.isnan(rec[k]) for k in ["foreign", "stocks", "fx", "illiquid", "sharpe"]):
                continue
            rec["service"] = float(svc_score) if svc_score is not None else np.nan
            records.append(rec)

    df_long = pd.DataFrame.from_records(records)
    if not df_long.empty:
        for c in ["stocks", "foreign", "fx", "illiquid", "sharpe", "service"]:
            if c in df_long.columns:
                df_long[c] = pd.to_numeric(df_long[c], errors="coerce")
    return df_long, svc, warnings


# ─────────────────────────────────────────────
# Optimizer
# ─────────────────────────────────────────────
def _weights_for_n(n: int, step: int) -> np.ndarray:
    step = max(1, int(step))
    if n == 1:
        return np.array([[100]], dtype=float)
    if n == 2:
        ws = np.arange(0, 101, step)
        pairs = np.column_stack([ws, 100 - ws])
        return pairs.astype(float)
    out = []
    for w1 in range(0, 101, step):
        for w2 in range(0, 101 - w1, step):
            w3 = 100 - w1 - w2
            if w3 >= 0 and w3 % step == 0:
                out.append([w1, w2, w3])
    return np.array(out, dtype=float) if out else np.empty((0, 3), dtype=float)

def _prefilter_candidates(df, include, targets, cap, locked_fund):
    keys = [k for k, v in include.items() if v and k in ["foreign", "stocks", "fx", "illiquid"]]
    if not keys:
        keys = ["foreign", "stocks"]
    tmp = df.copy()
    score = np.zeros(len(tmp), dtype=float)
    for k in keys:
        score += np.abs(tmp[k].fillna(50.0).to_numpy() - float(targets.get(k, 0.0))) / 100.0
    tmp["_s"] = score
    if locked_fund:
        locked_mask = tmp["fund"].str.strip() == locked_fund.strip()
        locked_df = tmp[locked_mask]
        rest_df   = tmp[~locked_mask].sort_values("_s").head(max(cap - len(locked_df), 1))
        tmp = pd.concat([locked_df, rest_df])
    else:
        tmp = tmp.sort_values("_s").head(cap)
    return tmp.drop(columns=["_s"]).reset_index(drop=True)

def _hard_ok_vec(values, target, mode):
    if mode == "בדיוק":
        return np.abs(values - target) < 0.5
    if mode == "לפחות":
        return values >= target - 0.5
    if mode == "לכל היותר":
        return values <= target + 0.5
    return np.ones(len(values), dtype=bool)

def find_best_solutions(
    df, n_funds, step, mix_policy, include, constraint, targets, primary_rank,
    locked_fund="", locked_weight_pct: Optional[float] = None,
    max_solutions_scan=20000,
) -> Tuple[pd.DataFrame, str]:
    import gc
    targets = {k: float(v) for k, v in targets.items()}

    cap = 50 if n_funds == 2 else 35 if n_funds == 3 else 80
    df_scan = _prefilter_candidates(df, include, targets, cap=cap, locked_fund=locked_fund)

    weights_arr  = _weights_for_n(n_funds, step)
    if len(weights_arr) == 0:
        return pd.DataFrame(), "לא נמצאו שילובי משקלים. נסה צעד קטן יותר."
    weights_norm = weights_arr / 100.0

    metric_keys = ["foreign", "stocks", "fx", "illiquid"]
    active_soft = [k for k in metric_keys if include.get(k, False)] or ["foreign", "stocks"]
    soft_idx    = {k: i for i, k in enumerate(metric_keys)}
    hard_keys   = [(k, constraint[k][1]) for k in metric_keys
                   if constraint.get(k, ("רך", ""))[0] == "קשיח"]

    A       = df_scan[["foreign","stocks","fx","illiquid","sharpe","service"]].to_numpy(dtype=float)
    records = df_scan.reset_index(drop=True)

    locked_idx: Optional[int] = None
    if locked_fund:
        matches = records.index[records["fund"].str.strip() == locked_fund.strip()].tolist()
        if matches:
            locked_idx = matches[0]

    # ── פיצ׳ר 3: סינון משקלים לפי locked_weight_pct ──
    if locked_idx is not None and locked_weight_pct is not None:
        tol = max(step * 0.5, 0.5)
        # עמודה של הקרן הנעולה היא העמודה שמתאימה ל-locked_idx בקומבינציה
        # נסנן אחרי בחירת קומבינציה — שמור רק weights שבהם המשקל ב-locked_idx == locked_weight_pct
        # בשלב הלולאה נסנן ידנית
        pass  # handled in loop below

    if mix_policy == "אותו מנהל בלבד":
        groups = list(records.groupby("manager").groups.values())
        combo_source = itertools.chain.from_iterable(
            itertools.combinations(list(g), n_funds) for g in groups if len(g) >= n_funds
        )
    else:
        combo_source = itertools.combinations(range(len(records)), n_funds)

    solutions = []
    scanned   = 0
    MAX_STORED = 60000

    for combo in combo_source:
        if locked_idx is not None and locked_idx not in combo:
            continue
        scanned += 1
        if scanned > max_solutions_scan:
            break

        arr     = A[list(combo), :]
        w_arr   = weights_arr.copy()

        # ── פיצ׳ר 3: אם יש locked_weight_pct, סנן משקלים ──
        if locked_idx is not None and locked_weight_pct is not None:
            pos_in_combo = list(combo).index(locked_idx)
            # Snap to nearest weight step to guarantee a match
            snapped = round(locked_weight_pct / step) * step
            snapped = max(step, min(100 - step * (n_funds - 1), snapped))  # keep combo valid
            tol = step * 0.5 + 0.1
            mask_w = np.abs(w_arr[:, pos_in_combo] - snapped) <= tol
            w_arr = w_arr[mask_w]
            if len(w_arr) == 0:
                continue

        w_norm = w_arr / 100.0
        mix_all = np.einsum("wn,nm->wm", w_norm, np.nan_to_num(arr, nan=0.0))

        mask = np.ones(len(w_norm), dtype=bool)
        for k, mode in hard_keys:
            mask &= _hard_ok_vec(mix_all[:, soft_idx[k]], targets.get(k, 0.0), mode)
        if not mask.any():
            continue

        mix_ok    = mix_all[mask]
        w_ok      = w_arr[mask]
        score_arr = np.zeros(len(mix_ok))
        for k in active_soft:
            score_arr += np.abs(mix_ok[:, soft_idx[k]] - targets.get(k, 0.0)) / 100.0

        fund_labels  = [records.loc[i, "fund"]    for i in combo]
        track_labels = [records.loc[i, "track"]   for i in combo]
        managers     = [records.loc[i, "manager"] for i in combo]
        manager_set  = " | ".join(sorted(set(managers)))
        managers_per_fund = " | ".join(managers)  # ordered, one per fund

        # ── Feature 5: Sharpe validity check ──
        sharpe_vals = arr[:, 4]  # sharpe column
        sharpe_incomplete = bool(np.any(np.isnan(sharpe_vals) | (sharpe_vals == 0)))

        for wi in range(len(mix_ok)):
            solutions.append({
                "combo":             combo,
                "weights":           tuple(int(round(x)) for x in w_ok[wi]),
                "מנהלים":            manager_set,
                "מנהלים_רשימה":      managers_per_fund,
                "מסלולים":           " | ".join(track_labels),
                "קופות":             " | ".join(fund_labels),
                'חו"ל (%)'  :        float(mix_ok[wi, 0]),
                "ישראל (%)"  :        float(100.0 - mix_ok[wi, 0]),
                "מניות (%)"  :        float(mix_ok[wi, 1]),
                'מט"ח (%)'  :        float(mix_ok[wi, 2]),
                "לא־סחיר (%)" :       float(mix_ok[wi, 3]),
                "שארפ משוקלל":        np.nan if sharpe_incomplete else float(mix_ok[wi, 4]),
                "sharpe_incomplete":  sharpe_incomplete,
                "שירות משוקלל":       float(mix_ok[wi, 5]),
                "score"       :       float(score_arr[wi]),
            })

        if len(solutions) >= MAX_STORED:
            solutions.sort(key=lambda r: (r["score"], -r["שארפ משוקלל"], -r["שירות משוקלל"]))
            solutions = solutions[:10000]
            gc.collect()

    if not solutions:
        return pd.DataFrame(), "לא נמצאו פתרונות. נסה לרכך מגבלות, להגדיל צעד, או לשנות יעדים."

    df_sol = pd.DataFrame(solutions)
    del solutions
    gc.collect()

    note = f"נסרקו {min(scanned, max_solutions_scan):,} קומבינציות מתוך {len(df_scan)} קופות מסוננות."

    if primary_rank == "דיוק":
        df_sol = df_sol.sort_values(["score", "שארפ משוקלל", "שירות משוקלל"], ascending=[True, False, False])
    elif primary_rank == "שארפ":
        df_sol = df_sol.sort_values(["שארפ משוקלל", "score"], ascending=[False, True])
    elif primary_rank == "שירות ואיכות":
        df_sol = df_sol.sort_values(["שירות משוקלל", "score"], ascending=[False, True])

    return df_sol, note

def _pick_three_distinct(df_sol, primary_rank):
    if df_sol.empty:
        return df_sol

    def mgr(row): return str(row["מנהלים"]).strip()

    sorted_primary = df_sol.copy()
    sorted_sharpe  = df_sol.sort_values(["שארפ משוקלל",  "score"], ascending=[False, True])
    sorted_service = df_sol.sort_values(["שירות משוקלל", "score"], ascending=[False, True])

    def best_from(df_sorted, exclude_managers):
        for _, r in df_sorted.iterrows():
            if mgr(r) not in exclude_managers:
                return r
        return df_sorted.iloc[0]

    pick1 = best_from(sorted_primary, set())
    pick2 = best_from(sorted_sharpe,  set())
    pick3 = best_from(sorted_service, set())

    used_after_1 = {mgr(pick1)}
    if mgr(pick2) in used_after_1:
        pick2 = best_from(sorted_sharpe, used_after_1)

    used_after_2 = used_after_1 | {mgr(pick2)}
    if mgr(pick3) in used_after_2:
        pick3 = best_from(sorted_service, used_after_2)

    labels     = ["חלופה 1 – דירוג ראשי", "חלופה 2 – שארפ", "חלופה 3 – שירות ואיכות"]
    criterions = ["דיוק", "שארפ", "שירות ואיכות"]
    base = pick1.to_dict()
    rows = []
    for i, r in enumerate([pick1, pick2, pick3]):
        row = r.to_dict()
        row["חלופה"]        = labels[i]
        row["weights_items"] = _weights_items(row.get("weights"), row.get("קופות",""), row.get("מסלולים",""), row.get("מנהלים_רשימה",""))
        row["משקלים"]       = _weights_short(row.get("weights"))
        row["יתרון"]        = _make_advantage(criterions[i], row, base if i > 0 else None)
        rows.append(row)
    return pd.DataFrame(rows)


def _weights_items(weights, funds_str, tracks_str, managers_str=""):
    try:    ws = list(weights)
    except: ws = []
    funds    = [s.strip() for s in (funds_str    or "").split("|") if s.strip()]
    tracks   = [s.strip() for s in (tracks_str   or "").split("|") if s.strip()]
    managers = [s.strip() for s in (managers_str or "").split("|") if s.strip()]
    n = max(len(ws), len(funds))
    return [
        {
            "pct":     f"{int(round(float(ws[i])))}%" if i < len(ws) else "?",
            "fund":    funds[i]    if i < len(funds)    else "",
            "track":   tracks[i]  if i < len(tracks)   else "",
            "manager": managers[i] if i < len(managers) else "",
        }
        for i in range(n)
    ]

def _weights_short(weights):
    if weights is None: return ""
    try:    w = [float(x) for x in weights]
    except: return ""
    return " / ".join(f"{int(round(x))}%" for x in w)

def _make_advantage(primary, row, base=None):
    score = row.get("score", 0)
    if primary == "דיוק":
        return f"מדויק ביותר ליעד (סטייה {score:.4f})"
    if primary == "שארפ":
        sh = float(row.get("שארפ משוקלל", 0) or 0)
        delta = sh - float((base or {}).get("שארפ משוקלל", sh) or sh)
        return f"שארפ {sh:.2f} (+{delta:.2f} מחלופה 1)"
    sv = float(row.get("שירות משוקלל", 0) or 0)
    delta = sv - float((base or {}).get("שירות משוקלל", sv) or sv)
    return f"שירות ואיכות {sv:.1f} (+{delta:.1f} מחלופה 1)"


# ─────────────────────────────────────────────
# Render helpers
# ─────────────────────────────────────────────
def _normalize_series(s):
    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
    mn, mx = float(s.min()), float(s.max())
    if abs(mx - mn) < 1e-12:
        return pd.Series([0.5] * len(s), index=s.index)
    return (s - mn) / (mx - mn)

def _pick_recommendations(df_sol_head):
    if df_sol_head is None or df_sol_head.empty:
        return {}
    df = df_sol_head.copy()
    score_n   = _normalize_series(df["score"])
    acc_n     = 1.0 - score_n
    sharpe_n  = _normalize_series(df.get("שארפ משוקלל", pd.Series([0]*len(df))))
    service_n = _normalize_series(df.get("שירות משוקלל", pd.Series([0]*len(df))))
    df["_weighted_pref"] = 0.45*acc_n + 0.15*sharpe_n + 0.40*service_n
    weighted     = df.loc[df["_weighted_pref"].idxmax()].to_dict()
    accurate     = df.loc[df["score"].idxmin()].to_dict()
    best_sh      = df.loc[df["שארפ משוקלל"].idxmax()].to_dict() if "שארפ משוקלל" in df.columns else accurate
    best_service = df.loc[df["שירות משוקלל"].idxmax()].to_dict() if "שירות משוקלל" in df.columns else accurate
    return {"weighted": weighted, "accurate": accurate, "sharpe": best_sh, "service": best_service}

def _manager_weights_from_items(items, manager_names):
    if not items: return []
    names = sorted([m for m in manager_names if isinstance(m, str) and m.strip()], key=len, reverse=True)
    agg = {}
    for it in items:
        fund = str(it.get("fund",""))
        pct  = float(str(it.get("pct","0")).replace("%","") or 0)
        chosen = None
        for n in names:
            if fund.strip().startswith(n) or (n in fund.strip()):
                chosen = n
                break
        if chosen is None: chosen = "אחר"
        agg[chosen] = agg.get(chosen, 0.0) + pct
    return sorted(agg.items(), key=lambda x: -x[1])

def _change_type_badge(cur_mgrs, prop_mgrs):
    cur_set  = set(m.strip() for m in cur_mgrs  if m.strip())
    prop_set = set(m.strip() for m in prop_mgrs if m.strip())
    if not cur_set:
        return ""
    if cur_set == prop_set:
        return "<span class='change-badge change-low'>✅ שינוי מסלול – אותו מנהל</span>"
    elif cur_set & prop_set:
        return "<span class='change-badge change-med'>⚠️ ניוד חלקי</span>"
    else:
        return "<span class='change-badge change-high'>🔴 ניוד מלא</span>"

# ─────────────────────────────────────────────
# AI explanation (Claude API)
# ─────────────────────────────────────────────
BAR_COLORS = ["#4f46e5","#2563eb","#0891b2","#7c3aed","#059669","#d97706"]

@st.cache_data(show_spinner=False, ttl=3600)
def _ai_explain(title: str, managers: str, weights_str: str,
                foreign: float, stocks: float, fx: float, illiquid: float,
                sharpe: float, service: float,
                has_baseline: bool, bl_foreign: float, bl_stocks: float,
                bl_sharpe: float, bl_service: float) -> str:
    """Call Claude API to generate a Hebrew explanation of this alternative."""
    import requests as _req

    api_key = ""
    try:
        if hasattr(st, "secrets") and "ANTHROPIC_API_KEY" in st.secrets:
            api_key = str(st.secrets["ANTHROPIC_API_KEY"])
    except Exception:
        pass
    if not api_key:
        import os
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""

    baseline_block = ""
    if has_baseline:
        baseline_block = (
            f"\nהמצב הנוכחי של הלקוח: חו\"ל {bl_foreign:.1f}%, מניות {bl_stocks:.1f}%, "
            f"שארפ {bl_sharpe:.2f}, שירות {bl_service:.1f}."
        )

    prompt = (
        f"אתה יועץ פיננסי ישראלי מנוסה המתמחה בקרנות השתלמות. "
        f"תפקידך לנסח הסבר תמציתי ומשכנע — 2-3 משפטים בלבד — למה החלופה הזו מתאימה ללקוח."
        f"\n\nהחלופה: {title}"
        f"\nמנהלים: {managers}"
        f"\nחלוקת משקלים: {weights_str}"
        f"\nחשיפות: חו\"ל {foreign:.1f}%, מניות {stocks:.1f}%, מט\"ח {fx:.1f}%, לא-סחיר {illiquid:.1f}%"
        f"\nמדד שארפ: {sharpe:.2f} | ציון שירות ואיכות: {service:.1f}"
        f"{baseline_block}"
        f"\n\nכתוב בעברית תמציתית. התמקד בערך המוסף העיקרי. אל תציין ש'אתה AI'."
    )

    try:
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 180,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=12,
        )
        if resp.status_code == 200:
            data = resp.json()
            for blk in data.get("content", []):
                if blk.get("type") == "text":
                    return str(blk["text"]).strip()
    except Exception:
        pass
    return ""


def _mini_alloc_bar_html(items: List[Dict]) -> str:
    """Generate compact horizontal stacked bar HTML for fund weights."""
    if not items:
        return ""
    colors = BAR_COLORS
    rows = ""
    for i, it in enumerate(items[:4]):
        pct_str = str(it.get("pct", "0")).replace("%", "")
        try:
            pct = float(pct_str)
        except Exception:
            pct = 0.0
        color = colors[i % len(colors)]
        width = max(2, min(100, int(pct)))
        name  = str(it.get("fund", ""))[:28]
        rows += (
            f"<div class='rc-fund-row'>"
            f"<span class='rc-fund-pct'>{pct_str}%</span>"
            f"<div class='rc-fund-bar-wrap'><div class='rc-fund-bar' style='width:{width}%;background:{color}'></div></div>"
            f"<span class='rc-fund-name'>{_esc(name)}</span>"
            f"</div>"
        )
    return f"<div class='rc-funds'>{rows}</div>"


def _kpi_chip_html(label: str, val: float, baseline_val: Optional[float] = None,
                   is_lower_better: bool = False, fmt: str = "{:.1f}%") -> str:
    val_str = fmt.format(val)
    delta_html = ""
    if baseline_val is not None:
        diff = val - baseline_val
        if abs(diff) < 0.1:
            cls = "flat"; arrow = "→"
        elif (diff > 0 and not is_lower_better) or (diff < 0 and is_lower_better):
            cls = "up";   arrow = "▲"
        else:
            cls = "down"; arrow = "▼"
        diff_str = ("+" if diff >= 0 else "") + fmt.format(diff)
        delta_html = f"<span class='rc-kpi-delta {cls}'>{arrow}{diff_str}</span>"
    return (
        f"<div class='rc-kpi'>"
        f"<span class='rc-kpi-label'>{_esc(label)}</span>"
        f"<span class='rc-kpi-val'>{_esc(val_str)}</span>"
        f"{delta_html}"
        f"</div>"
    )


def _delta_grid_html(row: Dict, baseline: Dict) -> str:
    """Inline 3-col delta grid comparing baseline vs proposed."""
    items = [
        ('חו"ל',     float(baseline.get("foreign",0)),  float(row.get('חו"ל (%)',0)),   False, "{:.1f}%"),
        ("מניות",    float(baseline.get("stocks",0)),   float(row.get("מניות (%)",0)),  False, "{:.1f}%"),
        ('מט"ח',     float(baseline.get("fx",0)),       float(row.get('מט"ח (%)',0)),   False, "{:.1f}%"),
        ("לא-סחיר",  float(baseline.get("illiquid",0)), float(row.get("לא־סחיר (%)",0)), True, "{:.1f}%"),
        ("שארפ",     float(baseline.get("sharpe",0)),   float(row.get("שארפ משוקלל",0)), False, "{:.2f}"),
        ("שירות ואיכות",    float(baseline.get("service",0)),  float(row.get("שירות משוקלל",0)), False, "{:.0f}"),
    ]
    cells = ""
    for label, cur, new, lower_better, fmt in items:
        diff = new - cur
        if abs(diff) < 0.05:
            arrow_cls = "flat"; arrow = "→"
        elif (diff > 0 and not lower_better) or (diff < 0 and lower_better):
            arrow_cls = "up"; arrow = "▲"
        else:
            arrow_cls = "down"; arrow = "▼"
        cells += (
            f"<div class='rc-delta-item'>"
            f"<span class='rc-delta-item-label'>{_esc(label)}</span>"
            f"<div class='rc-delta-item-vals'>"
            f"<span class='rc-delta-item-cur'>{fmt.format(cur)}</span>"
            f"<span class='rc-delta-item-arrow {arrow_cls}'>{arrow}</span>"
            f"<span class='rc-delta-item-new'>{fmt.format(new)}</span>"
            f"</div></div>"
        )
    return f"<div class='rc-delta'><div class='rc-delta-title'>השוואה למצב קיים</div><div class='rc-delta-grid'>{cells}</div></div>"


def _render_compact_card(r: Dict, title: str, card_cls: str = "",
                         baseline: Optional[Dict] = None,
                         ai_text: str = "",
                         card_key: str = ""):
    """Render a compact Bloomberg-style result card fully in HTML."""
    items = r.get("weights_items") or _weights_items(r.get("weights"), r.get("קופות",""), r.get("מסלולים",""), r.get("מנהלים_רשימה",""))

    # Change type badge
    prop_mgrs = [m.strip() for m in str(r.get("מנהלים","")).split("|") if m.strip()]
    cur_mgrs  = list(st.session_state.get("portfolio_managers", []))
    change_badge = _change_type_badge(cur_mgrs, prop_mgrs) if baseline else ""

    # Head badge
    badge_text = {"rc-primary": "⭐ ממולץ", "rc-service": "🤝 שירות"}.get(card_cls, "📊")
    badge_cls  = {"rc-primary": "rc-badge-blue", "rc-service": "rc-badge-green"}.get(card_cls, "rc-badge-amber")

    mgr_str = str(r.get("מנהלים",""))[:60]
    sharpe  = _to_float(r.get("שארפ משוקלל"))
    service = _to_float(r.get("שירות משוקלל"))
    foreign = _to_float(r.get('חו"ל (%)'))
    stocks  = _to_float(r.get("מניות (%)"))
    fx      = _to_float(r.get('מט"ח (%)'))
    illiquid= _to_float(r.get("לא־סחיר (%)"))

    bl_f  = float(baseline.get("foreign",0))  if baseline else None
    bl_s  = float(baseline.get("stocks",0))   if baseline else None
    bl_fx = float(baseline.get("fx",0))       if baseline else None
    bl_il = float(baseline.get("illiquid",0)) if baseline else None
    bl_sh = float(baseline.get("sharpe",0))   if baseline else None
    bl_sv = float(baseline.get("service",0))  if baseline else None

    kpis_html = (
        _kpi_chip_html('חו"ל',     foreign,  bl_f,  False) +
        _kpi_chip_html("מניות",    stocks,   bl_s,  False) +
        _kpi_chip_html('מט"ח',     fx,       bl_fx, False) +
        _kpi_chip_html("לא-סחיר",  illiquid, bl_il, True)  +
        _kpi_chip_html("שארפ",     sharpe,   bl_sh, False, "{:.2f}")  +
        _kpi_chip_html("שירות ואיכות",    service,  bl_sv, False, "{:.0f}")
    )

    delta_section = _delta_grid_html(r, baseline) if baseline else ""

    ai_section = ""
    if ai_text:
        ai_section = f"<div class='rc-ai'><div class='rc-ai-label'>🤖 ניתוח AI</div><div class='rc-ai-text'>{_esc(ai_text)}</div></div>"
    elif ai_text == "":
        # placeholder while loading
        ai_section = "<div class='rc-ai'><div class='rc-ai-spinner'>טוען ניתוח AI...</div></div>"

    alloc_html = _mini_alloc_bar_html(items)

    st.markdown(f"""
    <div class='rc {card_cls}'>
      <div class='rc-head'>
        <span class='rc-title'>{_esc(title)}</span>
        <div style='display:flex;gap:6px;align-items:center;flex-wrap:wrap'>
          {change_badge}
          <span class='rc-badge {badge_cls}'>{badge_text}</span>
        </div>
      </div>
      <div style='padding:4px 14px 2px;font-size:11px;color:#64748b'>{_esc(mgr_str)}</div>
      {alloc_html}
      <div class='rc-kpis'>{kpis_html}</div>
      {delta_section}
      {ai_section}
    </div>
    """, unsafe_allow_html=True)

# Keep old helpers for comparison tab
def _alloc_plot(r):
    labels = ["מניות", 'חו"ל', 'מט"ח', "לא־סחיר"]
    vals = []
    for k in ["מניות (%)", 'חו"ל (%)', 'מט"ח (%)', "לא־סחיר (%)"]:
        try: vals.append(float(r.get(k) or 0))
        except: vals.append(0.0)
    text_labels = [f"{lbl} · {v:.1f}%" for lbl, v in zip(labels, vals)]
    fig = go.Figure(go.Bar(x=vals, y=labels, orientation='h', text=text_labels, textposition='outside',
        cliponaxis=False, marker=dict(color=['#6366f1','#8b5cf6','#a78bfa','#c4b5fd'])))
    fig.update_layout(height=200, margin=dict(l=10,r=120,t=0,b=0),
        xaxis=dict(range=[0,100], showgrid=False, zeroline=False, visible=False),
        yaxis=dict(autorange='reversed', tickfont=dict(size=12), showgrid=False, title=None),
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', showlegend=False)
    return fig

def _manager_donut(mgr_break):
    labels=[m for m,_ in mgr_break] or ["ללא"]
    values=[float(p) for _,p in mgr_break] or [100.0]
    fig = go.Figure(go.Pie(labels=labels, values=values, hole=0.62, textinfo='percent', sort=False))
    fig.update_traces(marker=dict(colors=['#4f46e5','#7c3aed','#06b6d4','#22c55e','#f59e0b','#ef4444']))
    fig.update_layout(height=180, margin=dict(l=0,r=0,t=0,b=0), showlegend=False, paper_bgcolor='rgba(0,0,0,0)')
    return fig


def _radar_chart(top_df, targets):
    categories = ['חו"ל', "מניות", 'מט"ח', "לא־סחיר", "שארפ×10", "שירות ואיכות÷10"]
    fig = go.Figure()
    colors = ["#2563eb", "#16a34a", "#ea580c", "#7c3aed"]
    for i, row in top_df.iterrows():
        vals = [
            float(row.get('חו"ל (%)', 0) or 0),
            float(row.get("מניות (%)", 0) or 0),
            float(row.get('מט"ח (%)', 0) or 0),
            float(row.get("לא־סחיר (%)", 0) or 0),
            float(row.get("שארפ משוקלל", 0) or 0) * 10,
            float(row.get("שירות משוקלל", 0) or 0) / 10,
        ]
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=categories + [categories[0]],
            fill="toself", opacity=0.25,
            line=dict(color=colors[i % len(colors)], width=2),
            name=str(row.get("חלופה", f"חלופה {i+1}")),
        ))
    tgt_vals = [targets.get("foreign",0), targets.get("stocks",0), targets.get("fx",0), targets.get("illiquid",0), 0, 0]
    fig.add_trace(go.Scatterpolar(
        r=tgt_vals + [tgt_vals[0]], theta=categories + [categories[0]],
        mode="lines", line=dict(color="rgba(239,68,68,0.7)", width=1.5, dash="dot"), name="יעד",
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=9)),
            angularaxis=dict(direction="clockwise"),
        ),
        showlegend=True, height=420,
        margin=dict(t=30, b=10, l=20, r=20),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=-0.12),
        font=dict(family="sans-serif", size=11),
    )
    return fig

def _export_excel(top3, baseline=None):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        display_cols = [
            "חלופה", "יתרון", "קופות", "מסלולים", "משקלים",
            'חו"ל (%)', "מניות (%)", 'מט"ח (%)', "לא־סחיר (%)",
            "שארפ משוקלל", "שירות משוקלל", "score",
        ]
        cols_exist = [c for c in display_cols if c in top3.columns]
        sheet_df = top3[cols_exist].copy()

        # ── פיצ׳ר 4: הוסף עמודות מצב קיים ──
        if baseline:
            for key, col_name in [("foreign",'חו"ל קיים (%)'),("stocks","מניות קיים (%)"),
                                   ("fx",'מט"ח קיים (%)'),("illiquid","לא-סחיר קיים (%)")]:
                sheet_df[col_name] = baseline.get(key, "—")

        sheet_df.to_excel(writer, sheet_name="חלופות", index=False)

        for i, row in top3.iterrows():
            items = row.get("weights_items") or []
            if items:
                detail_df = pd.DataFrame(items)
                # _weights_items returns: pct, fund, track, manager
                expected = ["אחוז", "קרן", "מסלול", "מנהל"]
                if len(detail_df.columns) == len(expected):
                    detail_df.columns = expected
                elif len(detail_df.columns) == 3:
                    detail_df.columns = ["אחוז", "קרן", "מסלול"]
                sheet_name = f"חלופה {i+1}"[:31]
                detail_df.to_excel(writer, sheet_name=sheet_name, index=False)

    return output.getvalue()


# ─────────────────────────────────────────────
# Load data (product-type aware)
# ─────────────────────────────────────────────
_product_type_now = st.session_state.get("product_type", "קרנות השתלמות")
_active_sheet_id  = POLICIES_GSHEET_ID if _product_type_now == "פוליסות חיסכון" else FUNDS_GSHEET_ID

with st.spinner("🔄 טוען נתונים מ-Google Sheets..."):
    df_long, service_map, load_warnings = load_funds_long(
        _active_sheet_id, SERVICE_GSHEET_ID, _product_type_now
    )
    if load_warnings:
        with st.expander('אזהרות טעינת נתונים', expanded=False):
            for w in load_warnings:
                st.warning(w)

if load_warnings:
    for w in load_warnings:
        st.warning(f"⚠️ {w}")

if df_long.empty:
    err_details = " | ".join(load_warnings) if load_warnings else "סיבה לא ידועה"
    st.error(
        f"❌ לא הצלחתי לטעון נתונים מ-Google Sheets.\n\n"
        f"**פרטי השגיאה:** {err_details}\n\n"
        "ודא שהגיליונות פתוחים לשיתוף ('Anyone with the link') ושמבנה הגיליון תקין."
    )
    st.stop()

n_tracks  = df_long["track"].nunique()
n_records = len(df_long)
all_funds = sorted(df_long["fund"].unique().tolist())


# ─────────────────────────────────────────────
# Session state defaults
# ─────────────────────────────────────────────
def _init_state():
    st.session_state.setdefault("n_funds",           2)
    st.session_state.setdefault("mix_policy",        "מותר לערבב מנהלים")
    st.session_state.setdefault("step",              5)
    st.session_state.setdefault("primary_rank",      "דיוק")
    st.session_state.setdefault("locked_fund",       "")
    st.session_state.setdefault("locked_amount",     0.0)   # פיצ׳ר 3
    st.session_state.setdefault("total_amount",      0.0)   # פיצ׳ר 3
    st.session_state.setdefault("selected_managers", None)
    st.session_state.setdefault("targets",      {"foreign": 30.0, "stocks": 40.0, "fx": 25.0, "illiquid": 20.0})
    st.session_state.setdefault("include",      {"foreign": True, "stocks": True, "fx": True, "illiquid": True})
    st.session_state.setdefault("constraint",   {
        "foreign":  ("רך", "בדיוק"),
        "stocks":   ("רך", "בדיוק"),
        "fx":       ("רך", "בדיוק"),
        "illiquid": ("רך", "בדיוק"),
    })
    st.session_state.setdefault("last_results",      None)
    st.session_state.setdefault("last_note",         "")
    st.session_state.setdefault("run_history",       [])
    # פיצ׳ר 1
    st.session_state.setdefault("portfolio_holdings",  None)
    st.session_state.setdefault("portfolio_baseline",  None)
    st.session_state.setdefault("portfolio_total",     0.0)
    st.session_state.setdefault("portfolio_managers",  [])
    # פיצ׳ר 2
    st.session_state.setdefault("quick_profile_active", None)
    st.session_state.setdefault("selected_alt", None)
    st.session_state.setdefault("voted_this_session", None)
    st.session_state.setdefault("show_vote_stats", False)
    # product type switcher
    st.session_state.setdefault("product_type", "קרנות השתלמות")
    # internal flags (ephemeral – popped when used)
    st.session_state.setdefault("_qf_scroll_to_cmp", False)
    st.session_state.setdefault("_mgr_clear_flag", False)

_init_state()


# ═══════════════════════════════════════════════════════════════════
# UI  – Single-page Premium Fintech Layout (v3.6)
# ═══════════════════════════════════════════════════════════════════

# ── Dynamic terminology helper ───────────────────────────────────────
def _lbl(key: str) -> str:
    """Return UI label based on active product type."""
    is_policy = st.session_state.get("product_type") == "פוליסות חיסכון"
    _labels = {
        "product_plural":   "פוליסות חיסכון" if is_policy else "קרנות",
        "product_singular": "פוליסת חיסכון"  if is_policy else "קרן",
        "fund_count_lbl":   "פוליסות"         if is_policy else "קופות",
        "manager_lbl":      "מנהל ההשקעות"    if is_policy else "מנהל",
        "n_funds_lbl":      "מספר פוליסות לשלב" if is_policy else "מספר קרנות לשלב",
        "subtitle":         "אופטימיזציה לתמהיל פוליסות חיסכון" if is_policy
                            else "אופטימיזציה לתמהיל מסלולי קרנות השתלמות",
    }
    return _labels.get(key, key)


# ── RENDER PRODUCT SELECTOR ─────────────────────────────────────────
def render_product_selector():
    pt = st.session_state.get("product_type", "קרנות השתלמות")
    st.markdown("""
<div style='display:flex;gap:0;border:1.5px solid #D1D5DB;border-radius:10px;
overflow:hidden;width:fit-content;margin-bottom:8px'>
""", unsafe_allow_html=True)
    c1, c2, _ = st.columns([1.4, 1.6, 5])
    with c1:
        if st.button("🏦 קרנות השתלמות",
                     type="primary" if pt == "קרנות השתלמות" else "secondary",
                     use_container_width=True, key="pt_funds"):
            if pt != "קרנות השתלמות":
                for k in ["last_results","selected_alt","run_history","quick_profile_active"]:
                    st.session_state[k] = [] if k == "run_history" else None
                st.session_state["product_type"] = "קרנות השתלמות"
                st.rerun()
    with c2:
        if st.button("📋 פוליסות חיסכון",
                     type="primary" if pt == "פוליסות חיסכון" else "secondary",
                     use_container_width=True, key="pt_policies"):
            if pt != "פוליסות חיסכון":
                for k in ["last_results","selected_alt","run_history","quick_profile_active"]:
                    st.session_state[k] = [] if k == "run_history" else None
                st.session_state["product_type"] = "פוליסות חיסכון"
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)


# ── helpers shared across renders ──────────────────────────────────
def _pct(v, dec=1):
    try: return f"{float(v):.{dec}f}%"
    except: return "—"

def _num(v, dec=2):
    try: return f"{float(v):.{dec}f}"
    except: return "—"

def _chip(label, val, fmt="{:.1f}%", lower_better=False, baseline_val=None, dark=False):
    val_s = fmt.format(float(val)) if val not in (None, "—") else "—"
    arrow = ""
    if baseline_val is not None:
        try:
            d = float(val) - float(baseline_val)
            if abs(d) < 0.05: arrow = ""
            elif (d > 0 and not lower_better) or (d < 0 and lower_better):
                arrow = f" <span style='color:#34d399;font-size:9px'>▲</span>" if dark else f" <span style='color:#15A46E;font-size:9px'>▲</span>"
            else:
                arrow = f" <span style='color:#f87171;font-size:9px'>▼</span>" if dark else f" <span style='color:#DC2626;font-size:9px'>▼</span>"
        except: pass
    if dark:
        return f"<div style='padding:4px 11px;border-radius:999px;font-size:12px;font-weight:700;background:#2a4a70;color:#bfdbfe;border:1px solid #3a5f8a'>{_esc(label)} {_esc(val_s)}{arrow}</div>"
    return f"<div class='br-chip'>{_esc(label)} {_esc(val_s)}{arrow}</div>"


# ── RENDER HEADER ───────────────────────────────────────────────────
def render_header(n_records, n_managers, n_alts):
    alts_display = str(n_alts) if n_alts else "—"
    subtitle  = _lbl("subtitle")
    cnt_label = _lbl("fund_count_lbl")
    st.markdown(f"""
<div class="pmo-header">
  <div class="pmo-header-left">
    <div class="pmo-logo-box">📊</div>
    <div>
      <div class="pmo-title">Profit Mix Optimizer</div>
      <div class="pmo-sub">{_esc(subtitle)}</div>
    </div>
  </div>
  <div class="pmo-kpis">
    <div class="pmo-kpi"><span class="pmo-kpi-val">{n_records:,}</span><span class="pmo-kpi-lbl">{cnt_label}</span></div>
    <div class="pmo-kpi"><span class="pmo-kpi-val">{n_managers}</span><span class="pmo-kpi-lbl">מנהלים</span></div>
    <div class="pmo-kpi"><span class="pmo-kpi-val">{alts_display}</span><span class="pmo-kpi-lbl">חלופות</span></div>
  </div>
</div>
""", unsafe_allow_html=True)


# ── RENDER QUICK FILTERS ────────────────────────────────────────────
QUICK_PROFILES_NEW = {
    "📈 מניות":  {"stocks_min": 90},
    '🏦 אג"ח':   {"stocks_max": 10, "illiquid_max": 10},
    "🌍 חו״ל":   {"foreign_min": 90},
    "🇮🇱 ישראל": {"foreign_max": 10},
    '💱 מט"ח':   {"fx_min": 90},
    "🔵 כללי":   {"track_contains": "כלל"},  # matches "כלל", "כללי", "באפיק כלל" etc.
}

def render_quick_filters(df_active):
    st.markdown("<div class='nav-bar'>", unsafe_allow_html=True)
    st.markdown("""
<div style='font-size:11px;color:#6B7280;margin-bottom:4px;display:flex;align-items:center;gap:6px'>
  <span style='font-weight:700;color:#374151'>⚡ סינון לפי מסלול טהור</span>
  <span style='background:#F3F4F6;border:1px solid #D1D5DB;border-radius:6px;padding:2px 7px;font-size:10px;cursor:default'
        title='מסלול טהור = קרן שהשקעתה ממוקדת בנכס בודד (כגון: מניות 90%+, אג&quot;ח 90%+ וכד&apos;). מנוגד למסלול כללי שמאזן בין כמה נכסים.'>
    💡 מה זה מסלול טהור?
  </span>
</div>""", unsafe_allow_html=True)
    current_qp = st.session_state.get("quick_profile_active")
    options = ["— הכל —"] + list(QUICK_PROFILES_NEW.keys())
    default_idx = 0
    if current_qp in QUICK_PROFILES_NEW:
        default_idx = list(QUICK_PROFILES_NEW.keys()).index(current_qp) + 1
    sel = st.radio(
        "מסלול מהיר", options=options, horizontal=True,
        index=default_idx, label_visibility="collapsed", key="qf_radio"
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if sel == "— הכל —":
        if st.session_state.get("quick_profile_active"):
            st.session_state["quick_profile_active"] = None
            st.rerun()
    else:
        new_qp = sel
        if new_qp != st.session_state.get("quick_profile_active"):
            st.session_state["quick_profile_active"] = new_qp
            st.session_state["_qf_scroll_to_cmp"] = True  # trigger scroll + open
            # auto-fill targets from profile
            pf = QUICK_PROFILES_NEW[new_qp]
            if "stocks_min"  in pf: st.session_state["targets"]["stocks"]  = float(pf["stocks_min"])
            if "foreign_min" in pf: st.session_state["targets"]["foreign"] = float(pf["foreign_min"])
            if "fx_min"      in pf: st.session_state["targets"]["fx"]      = float(pf["fx_min"])
            st.rerun()


# ── RENDER MIX BUILDER (left card) ─────────────────────────────────
def render_mix_builder(df_active, all_funds_list):

    params = [
        ("stocks",   "📈 מניות",   True),
        ("foreign",  '🌍 חו"ל',    True),
        ("fx",       '💱 מט"ח',    True),
        ("illiquid", "🔒 לא-סחיר", False),
    ]

    # ── Advanced settings expander (above the card) ──
    with st.expander("⚙️ הגדרות מתקדמות", expanded=False):
        ca, cb = st.columns(2)
        with ca:
            st.session_state["n_funds"] = st.selectbox(
                _lbl("n_funds_lbl"), options=[1, 2, 3],
                index=[1,2,3].index(st.session_state["n_funds"]),
                key="adv_nfunds"
            )
        with cb:
            st.session_state["mix_policy"] = st.selectbox(
                "מדיניות מנהלים",
                options=["מותר לערבב מנהלים", "אותו מנהל בלבד"],
                index=0 if st.session_state["mix_policy"] == "מותר לערבב מנהלים" else 1,
                key="adv_policy"
            )

        with st.expander("🎯 מגבלות יעד (קשיח/רך)", expanded=False):
            st.caption("ברירת מחדל: כל המגבלות רכות (משפיעות רק על הדירוג)")
            for key, label, _ in params:
                constraint = st.session_state["constraint"]
                cc1, cc2 = st.columns(2)
                with cc1:
                    h = st.selectbox(f"{label} – קשיחות", ["רך", "קשיח"],
                        index=0 if constraint.get(key, ("רך",))[0] == "רך" else 1,
                        key=f"hard_{key}")
                with cc2:
                    m = st.selectbox(f"{label} – כיוון", ["בדיוק", "לפחות", "לכל היותר"],
                        index=["בדיוק","לפחות","לכל היותר"].index(constraint.get(key, ("רך","בדיוק"))[1]),
                        key=f"mode_{key}")
                constraint[key] = (h, m)
            st.session_state["constraint"] = constraint

        # Locked fund
        st.markdown("**🔒 נעילת קרן (אופציונלי)**")
        lock_opts = ["ללא"] + all_funds_list
        lock_idx = 0
        if st.session_state["locked_fund"] in all_funds_list:
            lock_idx = all_funds_list.index(st.session_state["locked_fund"]) + 1
        locked = st.selectbox("קרן נעולה", options=lock_opts, index=lock_idx, key="adv_lock",
                               label_visibility="collapsed")
        st.session_state["locked_fund"] = "" if locked == "ללא" else locked

        if st.session_state["locked_fund"]:
            la1, la2 = st.columns(2)
            with la1:
                total_amt = st.number_input("סך לחלוקה (₪)", min_value=0.0,
                    value=float(st.session_state["total_amount"] or st.session_state.get("portfolio_total", 0.0)),
                    step=10000.0, format="%.0f", key="adv_total_amt")
                st.session_state["total_amount"] = total_amt
            with la2:
                max_l = float(total_amt) if total_amt > 0 else 1e9
                locked_amt = st.number_input("סכום לקרן הנעולה (₪)", min_value=0.0, max_value=max_l,
                    value=float(min(st.session_state["locked_amount"], max_l)),
                    step=10000.0, format="%.0f", key="adv_locked_amt")
                st.session_state["locked_amount"] = locked_amt
            if total_amt > 0 and locked_amt > 0:
                raw_pct = locked_amt / total_amt * 100
                step_val = st.session_state.get("step", 5)
                snapped_pct = round(raw_pct / step_val) * step_val
                snapped_pct = max(step_val, min(100 - step_val, snapped_pct))
                if abs(raw_pct - snapped_pct) > 0.1:
                    st.caption(f"💡 {raw_pct:.1f}% → **{snapped_pct:.0f}%** (מעוגל לצעד הקרוב)")
                else:
                    st.caption(f"✅ משקל נעול: **{snapped_pct:.0f}%**")

        # Manager filter – uses df_long so deselected managers remain visible
        all_mgrs_full = sorted(df_long["manager"].unique().tolist())
        real_sel_mgrs = st.session_state.get("selected_managers") or all_mgrs_full
        sel_mgrs = [m for m in real_sel_mgrs if m in all_mgrs_full and m != "__none__"]
        if not sel_mgrs and "__none__" not in (real_sel_mgrs or []):
            sel_mgrs = all_mgrs_full

        st.markdown("**🏢 סינון מנהלים**")
        mb1, mb2, _ = st.columns([1,1,3])
        with mb1:
            if st.button("בחר הכל", key="mgr_all2", use_container_width=True):
                st.session_state["selected_managers"] = all_mgrs_full.copy()
                st.rerun()
        with mb2:
            if st.button("נקה הכל", key="mgr_none2", use_container_width=True):
                st.session_state["selected_managers"] = ["__none__"]
                st.session_state["_mgr_clear_flag"] = True
                st.rerun()

        cleared = st.session_state.pop("_mgr_clear_flag", False)
        mgr_cols = st.columns(3)
        new_sel = []
        for i, mgr in enumerate(all_mgrs_full):
            with mgr_cols[i % 3]:
                fc = df_long[df_long["manager"] == mgr]["fund"].nunique()
                is_checked = (not cleared) and (mgr in sel_mgrs)
                if st.checkbox(f"{mgr} ({fc})", value=is_checked, key=f"mgr_cb2_{mgr}"):
                    new_sel.append(mgr)
        new_sel_set = set(new_sel)
        old_sel_set = set(m for m in sel_mgrs if m != "__none__")
        if new_sel_set != old_sel_set:
            st.session_state["selected_managers"] = new_sel if new_sel else ["__none__"]
            st.rerun()

        # Portfolio import
        st.markdown("**📂 ייבוא פורטפוליו נוכחי (אופציונלי)**")
        uploaded = st.file_uploader("דו\"ח מסלקה (XLSX)", type=["xlsx","xls"], key="adv_upload",
                                    label_visibility="collapsed")
        if uploaded:
            raw = uploaded.read()
            parsed, err = parse_clearing_report(raw)
            if err:
                st.error(f"שגיאה: {err}")
            elif parsed:
                holdings = parsed["holdings"]
                st.session_state["portfolio_holdings"] = holdings
                st.session_state["portfolio_total"]    = parsed["total_amount"]
                st.session_state["portfolio_managers"] = list({h["manager"] for h in holdings})
                baseline = _compute_baseline_from_holdings(holdings, df_active)
                st.session_state["portfolio_baseline"] = baseline
                st.success(f"✅ טעון: {len(holdings)} קרנות, ₪{parsed['total_amount']:,.0f}")

        if st.session_state.get("portfolio_baseline"):
            if st.button("🗑️ נקה פורטפוליו", key="clear_portfolio"):
                for k in ["portfolio_holdings","portfolio_baseline","portfolio_total","portfolio_managers"]:
                    st.session_state[k] = None if k != "portfolio_total" else 0.0
                st.rerun()

    # ── תמהיל card ──
    st.markdown("<div class='card'>\n<div class='card-title' style='font-size:16px'>⚙️ בניית תמהיל מסלולים אישי</div>", unsafe_allow_html=True)

    tgts = st.session_state["targets"]
    inc  = st.session_state["include"]

    for key, label, default_on in params:
        c1, c2 = st.columns([3, 1])
        with c1:
            inc[key] = True
            val = st.slider(
                label, 0.0, 100.0,
                float(tgts.get(key, 0.0)),
                step=1.0, key=f"sl_{key}", label_visibility="visible"
            )
            tgts[key] = val
        with c2:
            st.markdown(f"<div style='padding-top:28px;font-size:14px;font-weight:800;color:#1F3A5F;text-align:center'>{val:.0f}%</div>", unsafe_allow_html=True)

    st.session_state["targets"] = tgts
    st.session_state["include"] = inc

    st.markdown("</div>", unsafe_allow_html=True)  # close card

    # ── Calc button ──
    run = st.button("🔍 חשב חלופות מיטביות", type="primary", use_container_width=True, key="btn_calc")

    return run


# ── RENDER BEST SOLUTION (right card) ──────────────────────────────
def render_best_solution(recs, baseline):
    if not recs:
        st.markdown("""
<div class='card' style='text-align:center;padding:30px 18px'>
  <div style='font-size:28px;margin-bottom:8px'>⚡</div>
  <div style='font-size:13px;color:#6B7280'>הגדר יעדים ולחץ <b>חשב חלופות</b></div>
</div>""", unsafe_allow_html=True)
        return

    best = recs.get("weighted") or recs.get("accurate", {})
    if not best:
        return

    items = best.get("weights_items") or _weights_items(
        best.get("weights"), best.get("קופות",""), best.get("מסלולים",""), best.get("מנהלים_רשימה",""))

    score    = best.get("score", 0.0)
    score_pct = max(0.0, 100.0 - float(score) * 100.0)
    managers = str(best.get("מנהלים",""))[:70]
    tracks   = " | ".join(str(it.get("track",""))[:20] for it in items)[:80]
    foreign  = best.get('חו"ל (%)', 0.0)
    stocks   = best.get("מניות (%)", 0.0)
    fx       = best.get('מט"ח (%)', 0.0)
    illiquid = best.get("לא־סחיר (%)", 0.0)
    sharpe   = best.get("שארפ משוקלל", 0.0)
    service  = best.get("שירות משוקלל", 0.0)

    bl = baseline or {}
    chips_html = (
        _chip("מניות",   stocks,   "{:.1f}%", False, bl.get("stocks"),   dark=True) +
        _chip('חו"ל',    foreign,  "{:.1f}%", False, bl.get("foreign"),  dark=True) +
        _chip('מט"ח',    fx,       "{:.1f}%", False, bl.get("fx"),       dark=True) +
        _chip("לא-סחיר", illiquid, "{:.1f}%", True,  bl.get("illiquid"), dark=True) +
        _chip("שארפ",    sharpe,   "{:.2f}",  False, bl.get("sharpe"),   dark=True) +
        _chip("שירות ואיכות", service, "{:.0f}", False, bl.get("service"), dark=True)
    )

    weights_disp = " | ".join(
        f"{it.get('pct','')} {str(it.get('fund',''))[:18]}" for it in items
    )

    sharpe_inc = best.get("sharpe_incomplete", False)
    sharpe_note = ""
    if sharpe_inc or (isinstance(sharpe, float) and math.isnan(sharpe)):
        sharpe_note = "<div style='font-size:10px;color:#92400e;margin-top:2px'>⚠️ לא כל המסלולים פרסמו שארפ</div>"

    # Build per-fund lines (no truncation)
    fund_lines_html = ""
    for it in items:
        pct     = it.get("pct","")
        fund    = str(it.get("fund",""))
        track   = str(it.get("track",""))
        manager = str(it.get("manager",""))
        label   = track if track else fund
        mgr_bit = f" <span style='color:#93b4e0;font-size:11px'>({manager})</span>" if manager else ""
        fund_lines_html += (
            f"<div style='margin:3px 0;font-size:13px;color:#e2eeff'>"
            f"<b style='color:#7dd3fc;font-size:14px'>{_esc(pct)}</b> "
            f"{_esc(label)}{mgr_bit}</div>"
        )

    st.markdown(f"""
<div class='card' style='background:#1F3A5F;border-color:#2a4a70'>
  <div class='card-title' style='color:#fff;font-size:15px'>🏆 החלופה הטובה ביותר</div>
  <div style='display:flex;align-items:flex-start;gap:16px;margin-bottom:10px'>
    <div style='text-align:center;min-width:70px'>
      <div style='font-size:52px;font-weight:900;color:#7dd3fc;line-height:1'>{score_pct:.0f}</div>
      <div style='font-size:11px;color:#93b4e0;margin-top:2px'>ציון התאמה</div>
    </div>
    <div style='flex:1'>{fund_lines_html}</div>
  </div>
  {sharpe_note}
  <div style='display:flex;gap:6px;flex-wrap:wrap;margin-top:8px'>{chips_html}</div>
</div>
""", unsafe_allow_html=True)


# ── RENDER RESULTS STRIP ────────────────────────────────────────────
def render_results_strip(n_solutions, elapsed_note, qp_name):
    filter_txt = f"פילטר: {_esc(qp_name)}" if qp_name else "ללא פילטר"
    st.markdown(f"""
<div class='res-strip'>
  <span>🔢 <b>{n_solutions}</b> חלופות נמצאו</span>
  <span>⏱ {_esc(elapsed_note)}</span>
  <span>🎯 {filter_txt}</span>
</div>
""", unsafe_allow_html=True)


# ── RENDER RESULTS TABLE ────────────────────────────────────────────
def render_results_table(rows, baseline, voting_configured):
    if not rows:
        return

    selected_alt = st.session_state.get("selected_alt")

    def fmt_val(d, key, is_pct=True, dec=1):
        v = d.get(key, "—")
        if v == "—": return "—"
        try:
            f = float(v)
            return f"{f:.{dec}f}%" if is_pct else f"{f:.{dec}f}"
        except: return "—"

    def tracks_with_weights_html(rrow):
        """Build multi-line cell: 40% מסלול כללי — מנהל, per fund"""
        items = rrow.get("weights_items") or []
        parts = []
        for it in items:
            pct     = str(it.get("pct","")).replace("%","").strip()
            track   = str(it.get("track","")).strip()
            fund    = str(it.get("fund","")).strip()
            manager = str(it.get("manager","")).strip()
            label   = track if track else fund
            if label:
                mgr_html = f" <span style='color:#6B7280;font-size:10px'>— {_esc(manager)}</span>" if manager else ""
                parts.append(f"<b>{_esc(pct)}%</b>&nbsp;{_esc(label)}{mgr_html}")
        return "<br>".join(parts) if parts else "—"

    # Build table HTML – manager now inline in tracks cell
    header_cells = "".join(f"<th>{h}</th>" for h in [
        "לחץ לבחירה ▼", "מסלולים / מנהל / משקל", "מניות", 'חו"ל', 'מט"ח', "לא-סחיר", "שארפ", "שירות ואיכות"
    ])

    # ── Target row: shows user's requested mix at top of table ──
    target_map = st.session_state.get("targets", {}) or {}
    def _tpct(k): return f"{float(target_map.get(k, 0) or 0):.1f}%"
    rows_html = f"""<tr style='background:#F0F9FF;font-size:11px;color:#374151'>
  <td style='font-weight:800;color:#1F3A5F;white-space:nowrap'>🎯 יעד מבוקש</td>
  <td style='color:#6B7280;font-size:10px'>הגדרות הבקשה</td>
  <td class='num' style='color:#1F3A5F;font-weight:700'>{_tpct("stocks")}</td>
  <td class='num' style='color:#1F3A5F;font-weight:700'>{_tpct("foreign")}</td>
  <td class='num' style='color:#1F3A5F;font-weight:700'>{_tpct("fx")}</td>
  <td class='num' style='color:#1F3A5F;font-weight:700'>{_tpct("illiquid")}</td>
  <td class='num'>—</td>
  <td class='num'>—</td>
</tr>"""

    for rrow in rows:
        label = rrow["חלופה"]
        is_sel = (label == selected_alt)
        sel_cls = " class='sel-row'" if is_sel else ""
        icon = "✔ " if is_sel else ""
        name_style = "font-weight:800;color:#1F3A5F;cursor:pointer;text-decoration:underline dotted #93C5FD"

        tracks_cell = tracks_with_weights_html(rrow)

        sharpe_v = rrow.get("שארפ משוקלל","—")
        if rrow.get("sharpe_incomplete") or (isinstance(sharpe_v, float) and math.isnan(sharpe_v)):
            sharpe_cell = "<td class='num' style='font-size:9px;color:#92400e'>—*</td>"
        else:
            sharpe_cell = f"<td class='num'>{fmt_val(rrow, 'שארפ משוקלל', False, 2)}</td>"

        rows_html += f"""<tr{sel_cls}>
  <td style='{name_style}'>{icon}{_esc(label)}</td>
  <td style='font-size:11px;line-height:1.9'>{tracks_cell}</td>
  <td class='num'>{fmt_val(rrow,'מניות (%)')}</td>
  <td class='num'>{fmt_val(rrow,'חו"ל (%)')}</td>
  <td class='num'>{fmt_val(rrow,'מט"ח (%)')}</td>
  <td class='num'>{fmt_val(rrow,'לא־סחיר (%)')}</td>
  {sharpe_cell}
  <td class='num'>{fmt_val(rrow,'שירות משוקלל',False,0) if not rrow.get("service_missing") else "<span style='font-size:9px;color:#92400e'>—*</span>"}</td>
</tr>"""

    st.markdown(f"""
<div style='overflow-x:auto;border:1px solid #E5EAF2;border-radius:10px;overflow:hidden;margin-bottom:4px'>
  <table class='res-tbl'>
    <thead><tr>{header_cells}</tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
<p style='font-size:10px;color:#9CA3AF;margin:2px 0 6px'>
  לחץ על שם החלופה לפרטים וסטטיסטיקה &nbsp;|&nbsp; * שארפ חסר — לא כל המסלולים פרסמו שארפ &nbsp;|&nbsp; * שירות — ציון לא נמצא לגוף
</p>
""", unsafe_allow_html=True)

    # Fix 7: Alt NAME as Streamlit button → opens detail panel (replaces separate row of buttons)
    sel_cols = st.columns(len(rows))
    for col, rrow in zip(sel_cols, rows):
        with col:
            label = rrow["חלופה"]
            is_sel = (label == selected_alt)
            if st.button(
                f"{'✔ ' if is_sel else '📊 '}{label}",
                use_container_width=True,
                type="primary" if is_sel else "secondary",
                key=f"sel2_{label}"
            ):
                if is_sel:
                    st.session_state["selected_alt"] = None
                else:
                    st.session_state["selected_alt"] = label
                    st.session_state["show_vote_stats"] = False  # close stats panel on new selection
                    if voting_configured:
                        ok = _write_vote(label, str(rrow.get("מנהלים","")), str(rrow.get("מסלולים","")))
                        if ok:
                            try: _load_votes_cached.clear()
                            except: pass
                st.rerun()

    # Inline detail + lazy stats for selected row
    if selected_alt:
        sel_row = next((r for r in rows if r["חלופה"] == selected_alt), None)
        if sel_row:
            items = sel_row.get("weights_items") or _weights_items(
                sel_row.get("weights"), sel_row.get("קופות",""), sel_row.get("מסלולים",""), sel_row.get("מנהלים_רשימה",""))
            st.markdown(f"""
<div style='background:#EFF6FF;border:1.5px solid #C7D7FF;border-radius:10px;
padding:12px 16px;margin:6px 0'>
<div style='font-size:13px;font-weight:800;color:#1F3A5F;margin-bottom:8px'>
  📊 {_esc(selected_alt)} — פרטים
</div>""", unsafe_allow_html=True)

            d1, d2, d3 = st.columns(3)
            with d1:
                st.metric("מניות",  _pct(sel_row.get("מניות (%)","—")))
                st.metric('חו"ל',   _pct(sel_row.get('חו"ל (%)',"—")))
            with d2:
                st.metric('מט"ח',   _pct(sel_row.get('מט"ח (%)',"—")))
                sv = sel_row.get("שארפ משוקלל","—")
                if sel_row.get("sharpe_incomplete") or (isinstance(sv,float) and math.isnan(sv)):
                    st.metric("שארפ", "—")
                    st.caption("⚠️ לא כל המסלולים פרסמו שארפ")
                else:
                    st.metric("שארפ", _num(sv, 2))
            with d3:
                st.metric("לא-סחיר", _pct(sel_row.get("לא־סחיר (%)","—")))
                st.metric("שירות ואיכות",   _num(sel_row.get("שירות משוקלל","—"), 0))

            if items:
                st.caption("חלוקת משקלים: " + " | ".join(f"{it.get('pct','')} {it.get('fund','')}" for it in items))

            if baseline:
                delta_parts = []
                for lbl, bl_key, row_key, lower in [
                    ("מניות","stocks","מניות (%)",False), ('חו"ל',"foreign",'חו"ל (%)',False),
                    ("שארפ","sharpe","שארפ משוקלל",False), ("שירות ואיכות","service","שירות משוקלל",False),
                ]:
                    try:
                        cur = float(baseline.get(bl_key, 0))
                        new = float(sel_row.get(row_key, 0))
                        diff = new - cur
                        if abs(diff) < 0.05: continue
                        good = (diff > 0 and not lower) or (diff < 0 and lower)
                        clr  = "#15A46E" if good else "#DC2626"
                        sign = "+" if diff >= 0 else ""
                        delta_parts.append(f"<span style='color:{clr}'>{lbl}: {sign}{diff:.1f}</span>")
                    except: pass
                if delta_parts:
                    st.markdown("**Δ** מול מצב קיים: " + " &nbsp;|&nbsp; ".join(delta_parts), unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

            # Voting stats – always show button; session_state flag persists across reruns
            show_key = "show_vote_stats"
            if st.button("📊 סטטיסטיקות בחירה קהילתית", key="show_stats_btn"):
                st.session_state[show_key] = not st.session_state.get(show_key, False)
                st.rerun()
            if st.session_state.get(show_key, False):
                if voting_configured:
                    _render_votes_stats()
                else:
                    st.info(
                        "📋 **הסטטיסטיקה זמינה רק לאחר הגדרת חיבור ל-Google Sheets.**\n\n"
                        "הוסף את פרטי ה-GCP Service Account לסודות האפליקציה ב-Streamlit Cloud:\n"
                        "`Settings → Secrets → הדבק את תוכן קובץ ה-JSON תחת [gcp_service_account]`\n\n"
                        "ראה קובץ `SETUP_VOTING.md` בפרויקט להוראות מלאות."
                    )


# ── FUND COMPARISON SECTION ─────────────────────────────────────────
def render_fund_comparison(df_active, all_funds_list):
    # If quick filter was just selected, auto-open and inject scroll anchor
    scroll_flag = st.session_state.pop("_qf_scroll_to_cmp", False)
    if scroll_flag:
        st.markdown("""
<div id="cmp-anchor" style="height:0;overflow:hidden"></div>
<script>
  setTimeout(function(){
    var el = window.parent.document.getElementById('cmp-anchor');
    if(el){ el.scrollIntoView({behavior:'smooth', block:'start'}); }
  }, 200);
</script>""", unsafe_allow_html=True)

    # auto-expand when quick filter active
    qp = st.session_state.get("quick_profile_active")
    auto_open = bool(qp)

    with st.expander("⚖️ השוואת מסלולי השקעה", expanded=auto_open):
        st.caption("בחר עד 10 מסלולים להשוואה – נתוני חשיפות, שארפ וציון שירות.")

        qp = st.session_state.get("quick_profile_active")
        if qp and qp in QUICK_PROFILES_NEW:
            pf = QUICK_PROFILES_NEW[qp]
            filt = df_active.copy()
            if "stocks_min"  in pf: filt = filt[filt["stocks"]   >= pf["stocks_min"]]
            if "stocks_max"  in pf: filt = filt[filt["stocks"]   <= pf["stocks_max"]]
            if "foreign_min" in pf: filt = filt[filt["foreign"]  >= pf["foreign_min"]]
            if "foreign_max" in pf: filt = filt[filt["foreign"]  <= pf["foreign_max"]]
            if "fx_min"      in pf: filt = filt[filt["fx"]       >= pf["fx_min"]]
            if "illiquid_max"in pf: filt = filt[filt["illiquid"] <= pf["illiquid_max"]]
            if "track_contains" in pf:
                filt = filt[filt["track"].astype(str).str.contains(pf["track_contains"], na=False)]
            if "track_exact" in pf:
                filt = filt[filt["track"].astype(str).str.strip() == pf["track_exact"]]
            all_tracks = sorted(filt["track"].unique().tolist())
            st.info(f"פילטר פעיל: **{qp}** — {len(filt)} קרנות")
        else:
            filt = df_active
            all_tracks = sorted(df_active["track"].unique().tolist())

        ca, cb = st.columns(2)
        with ca:
            compare_tracks = st.multiselect("🔍 לפי מסלול", all_tracks, placeholder="הקלד שם...", key="cmp_tracks")
        with cb:
            compare_funds = st.multiselect("🔍 לפי קרן", all_funds_list, placeholder="הקלד שם...", max_selections=10, key="cmp_funds")

        selected_rows = []
        for track in (compare_tracks or []):
            for _, row in filt[filt["track"]==track].sort_values("sharpe",ascending=False).iterrows():
                selected_rows.append(row)
        for fn in (compare_funds or []):
            rows = filt[filt["fund"]==fn]
            if not rows.empty: selected_rows.append(rows.iloc[0])

        if qp and not compare_tracks and not compare_funds:
            for _, row in filt.sort_values("sharpe",ascending=False).head(15).iterrows():
                selected_rows.append(row)

        seen = set(); unique_rows = []
        for r in selected_rows:
            k = str(r["fund"])
            if k not in seen: seen.add(k); unique_rows.append(r)

        if not unique_rows:
            st.info("בחר מסלול או קרן להצגה")
            return

        comp_data = []
        for r in unique_rows:
            comp_data.append({
                "קרן":         str(r.get("fund","")),
                "מסלול":       str(r.get("track","")),
                "מנהל":        str(r.get("manager","")),
                'חו"ל (%)':    r.get("foreign",  float("nan")),
                "מניות (%)":   r.get("stocks",   float("nan")),
                'מט"ח (%)':    r.get("fx",       float("nan")),
                "לא־סחיר (%)": r.get("illiquid", float("nan")),
                "שארפ":        r.get("sharpe",   float("nan")),
                "שירות ואיכות":       r.get("service",  float("nan")),
            })
        comp_df = pd.DataFrame(comp_data)
        num_cols = ['חו"ל (%)','מניות (%)', 'מט"ח (%)','לא־סחיר (%)','שארפ','שירות']

        header_h = "".join(f"<th>{c}</th>" for c in comp_df.columns)
        rows_h = ""
        for _, row in comp_df.iterrows():
            cells = ""
            for col in comp_df.columns:
                v = row[col]
                if col in num_cols:
                    try:
                        dec = 2 if col=="שארפ" else (0 if col=="שירות ואיכות" else 1)
                        unit = "%" if "%" in col else ""
                        cells += f"<td style='text-align:center'>{float(v):.{dec}f}{unit}</td>"
                    except: cells += "<td style='text-align:center'>—</td>"
                else:
                    cells += f"<td>{_esc(str(v))}</td>"
            rows_h += f"<tr>{cells}</tr>"

        st.markdown(f"""
<div style='overflow-x:auto;border:1px solid #E5EAF2;border-radius:10px;overflow:hidden'>
  <table class='res-tbl'>
    <thead><tr>{header_h}</tr></thead>
    <tbody>{rows_h}</tbody>
  </table>
</div>""", unsafe_allow_html=True)

        # Compact bar chart for one metric
        bar_metric = st.selectbox("הצג גרף:", num_cols, key="cmp_bar2")
        bar_df = comp_df[["קרן", bar_metric]].dropna().sort_values(bar_metric, ascending=False)
        if not bar_df.empty:
            colors = ["#3A7AFE"] * len(bar_df)
            if len(bar_df) > 1:
                colors[0] = "#15A46E"; colors[-1] = "#EF4444"
            unit = "%" if "%" in bar_metric else ""
            fig = go.Figure(go.Bar(
                x=bar_df["קרן"], y=bar_df[bar_metric],
                marker_color=colors,
                text=bar_df[bar_metric].apply(lambda v: f"{v:.1f}{unit}"),
                textposition="outside",
            ))
            fig.update_layout(
                height=240, margin=dict(t=15,b=60,l=5,r=5),
                xaxis=dict(tickangle=-30, tickfont=dict(size=10)),
                yaxis=dict(showgrid=True, gridcolor="#F1F5F9"),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            )
            _safe_plotly(fig, key="cmp_bar_fig")

        cmp_out = io.BytesIO()
        with pd.ExcelWriter(cmp_out, engine="openpyxl") as writer:
            comp_df.to_excel(writer, sheet_name="השוואה", index=False)
        st.download_button("⬇️ ייצוא לאקסל", data=cmp_out.getvalue(),
            file_name="fund_comparison.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="cmp_dl")


# ── HISTORY SECTION ─────────────────────────────────────────────────
def render_history():
    history = st.session_state.get("run_history", [])
    if not history:
        return
    with st.expander("🕓 היסטוריית חישובים", expanded=False):
        for i, h in enumerate(history):
            ts = h.get("ts","")
            df_h = h.get("solutions_all")
            if df_h is None or df_h.empty: continue
            n = len(df_h)
            tgts = h.get("targets", {})
            tgt_str = " | ".join(f"{k}: {v:.0f}%" for k,v in tgts.items() if v)
            st.markdown(f"**{ts}** — {n:,} פתרונות | {tgt_str}")
            if i < len(history) - 1: st.markdown("---")


# ═══════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═══════════════════════════════════════════════════════════════════

# ── Derive active df based on manager filter ──
all_managers = sorted(df_long["manager"].unique().tolist())
sel_m = st.session_state.get("selected_managers") or all_managers
real_sel = [m for m in sel_m if m in all_managers and m != "__none__"]
if real_sel and len(real_sel) < len(all_managers):
    df_active = df_long[df_long["manager"].isin(real_sel)].copy()
else:
    df_active = df_long

all_funds = sorted(df_active["fund"].unique().tolist())

# ── Derive results ──
res          = st.session_state.get("last_results")
recs         = _pick_recommendations(res.get("solutions_all") if res else None) if res else {}
baseline     = st.session_state.get("portfolio_baseline")
voting_conf  = hasattr(st, "secrets") and "gcp_service_account" in st.secrets
n_solutions  = len(res["solutions_all"]) if res and res.get("solutions_all") is not None else 0

# ── HEADER ──
render_header(len(df_active), len(all_managers), len(recs))

# ── PRODUCT TYPE SELECTOR ──
render_product_selector()

# ── QUICK FILTERS ──
render_quick_filters(df_active)

# ── FUND COMPARISON (above main layout, below quick filters) ──
render_fund_comparison(df_active, all_funds)

# ── MAIN 2-COL LAYOUT ──
col_left, col_right = st.columns([1.1, 0.9], gap="medium")

with col_left:
    run_clicked = render_mix_builder(df_active, all_funds)

with col_right:
    render_best_solution(recs, baseline)

# ── Handle calculation ──
if run_clicked:
    # Derive locked_pct
    locked_pct = None
    if st.session_state["locked_fund"] and st.session_state["total_amount"] > 0 and st.session_state["locked_amount"] > 0:
        locked_pct = round(st.session_state["locked_amount"] / st.session_state["total_amount"] * 100, 1)

    with st.spinner("⚡ מחשב... (חיפוש מואץ עם NumPy)"):
        try:
            sols, note = find_best_solutions(
                df=df_active,
                n_funds=st.session_state["n_funds"],
                step=st.session_state["step"],
                mix_policy=st.session_state["mix_policy"],
                include=st.session_state["include"],
                constraint=st.session_state["constraint"],
                targets=st.session_state["targets"],
                primary_rank=st.session_state["primary_rank"],
                locked_fund=st.session_state["locked_fund"],
                locked_weight_pct=locked_pct,
            )
            if sols is not None and not sols.empty:
                result = {
                    "solutions_all": sols,
                    "targets":       dict(st.session_state["targets"]),
                    "ts":            datetime.now().strftime("%H:%M:%S"),
                }
                st.session_state["last_results"]  = result
                st.session_state["last_note"]     = note
                st.session_state["selected_alt"]  = None
                hist = st.session_state.get("run_history", [])
                hist.insert(0, result)
                st.session_state["run_history"] = hist[:3]
            else:
                st.warning(f"לא נמצאו תוצאות. {note}")
        except Exception as _e:
            st.error(f"שגיאה: {_e}")
    st.rerun()

# ── RESULTS (shown after calculation) ──
if res and recs:
    # Build rows list
    rows_list = []
    for _key, rrow, title in [
        ("weighted", recs.get("weighted"), "חלופה משוקללת"),
        ("accurate", recs.get("accurate"), "הכי מדויקת"),
        ("sharpe",   recs.get("sharpe"),   "שארפ מקסימלי"),
        ("service",  recs.get("service"),  "שירות מוביל"),
    ]:
        if rrow is None: continue
        r = dict(rrow)
        r["חלופה"]        = title
        r["weights_items"] = _weights_items(r.get("weights"), r.get("קופות",""), r.get("מסלולים",""), r.get("מנהלים_רשימה",""))
        r["משקלים"]       = _weights_short(r.get("weights"))
        rows_list.append(r)

    # Results strip
    render_results_strip(
        n_solutions=n_solutions,
        elapsed_note=st.session_state.get("last_note",""),
        qp_name=st.session_state.get("quick_profile_active")
    )

    # Results table
    render_results_table(rows_list, baseline, voting_conf)

    # Export
    top_df = pd.DataFrame(rows_list)
    exc, _ = st.columns([1, 6])
    with exc:
        st.download_button(
            "⬇️ ייצוא לאקסל",
            data=_export_excel(top_df, baseline),
            file_name="profit_mix_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_main"
        )

# ── HISTORY (expander) ──
render_history()

# ── ALLOCATION HISTORY (new feature – isolated module) ──
from allocation_history_ui import render_allocation_history
render_allocation_history()
