# -*- coding: utf-8 -*-
"""
allocation_history_ui.py
────────────────────────
Self-contained UI for the "היסטוריית אלוקציה" feature.
Renders as an st.expander — zero interference with the rest of the app.

Entry point called from streamlit_app.py (one line):
    from allocation_history_ui import render_allocation_history
    render_allocation_history()

Configuration
─────────────
Set ALLOCATION_SHEET_URL to your Google Sheets URL.
If the sheet is public (published to web → CSV), no credentials needed.
Otherwise add a [gcp_service_account] block to .streamlit/secrets.toml.
"""

from __future__ import annotations

from datetime import date, timedelta
import io

import numpy as np
import pandas as pd
import streamlit as st

from allocation_history_loader import load_allocation_history
from allocation_history_charts import (
    build_timeseries_chart,
    build_snapshot_chart,
    build_delta_chart,
    build_heatmap,
    build_summary_stats,
    build_ranking_chart,
    build_track_gap_chart,
    build_vs_benchmark_chart,
)

# ══════════════════════════════════════════════════════════════════════════════
# ▶▶▶  PASTE YOUR GOOGLE SHEETS URL HERE  ◀◀◀
# ══════════════════════════════════════════════════════════════════════════════
ALLOCATION_SHEET_URL: str = (
    "https://docs.google.com/spreadsheets/d/1XuUz5--HhUmcG1YTgktkmmTypicFIaND2dltR-dA0FE/edit?gid=0#gid=0"
)
# ══════════════════════════════════════════════════════════════════════════════


def _safe_plotly(fig, key=None):
    """Wrapper identical to the one in streamlit_app.py — avoids import."""
    try:
        st.plotly_chart(fig, use_container_width=True, key=key)
    except TypeError:
        try:
            st.plotly_chart(fig, key=key)
        except TypeError:
            st.plotly_chart(fig)


def _filter_by_range(df: pd.DataFrame, range_label: str, custom_start: date | None) -> pd.DataFrame:
    if df.empty:
        return df
    max_date = df["date"].max()
    if range_label == "הכל":
        return df
    elif range_label == "YTD":
        start = pd.Timestamp(max_date.year, 1, 1)
    elif range_label == "1Y":
        start = max_date - pd.DateOffset(years=1)
    elif range_label == "3Y":
        start = max_date - pd.DateOffset(years=3)
    elif range_label == "5Y":
        start = max_date - pd.DateOffset(years=5)
    elif range_label == "מותאם אישית" and custom_start:
        start = pd.Timestamp(custom_start)
    else:
        return df
    return df[df["date"] >= start]


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _clamp_date_value(value: date, min_value: date, max_value: date) -> date:
    if value < min_value:
        return min_value
    if value > max_value:
        return max_value
    return value


def _date_bounds(df: pd.DataFrame) -> tuple[date, date]:
    min_d = pd.to_datetime(df["date"], errors="coerce").min()
    max_d = pd.to_datetime(df["date"], errors="coerce").max()
    return min_d.date(), max_d.date()


def render_allocation_history():
    """
    Renders the full Allocation History feature inside an st.expander.
    This function is the only public API of this module.
    """

    with st.expander("📊 היסטוריית אלוקציה", expanded=False):

        # ── Configuration notice ──────────────────────────────────────────────
        if not ALLOCATION_SHEET_URL.strip():
            st.info(
                "💡 **הגדרה נדרשת:** "
                "יש להגדיר את קישור ה-Google Sheets ב-`allocation_history_ui.py` "
                "(משתנה `ALLOCATION_SHEET_URL`).",
                icon="⚙️",
            )
            return

        # ── Load data ─────────────────────────────────────────────────────────
        with st.spinner("טוען נתוני אלוקציה..."):
            df_all, load_errors = load_allocation_history(ALLOCATION_SHEET_URL)

        if load_errors:
            with st.expander("⚠️ הערות טעינה", expanded=False):
                for e in load_errors:
                    st.warning(e)

        if df_all.empty:
            st.error("לא נטענו נתונים. ייתכן שמבנה הגיליון לא זוהה נכון או שהגישה לקובץ נכשלה.")
            return

        # ── Sidebar filters ───────────────────────────────────────────────────
        st.markdown("#### 🎛️ סינון נתונים")
        f1, f2, f3 = st.columns([1, 1, 1])

        with f1:
            all_managers = sorted(df_all["manager"].unique().tolist())
            sel_managers = st.multiselect(
                "מנהל השקעות",
                options=all_managers,
                help="בחירת גוף מוסדי אחד או יותר להשוואה לאורך זמן.",
                default=all_managers,
                key="ah_managers",
            )

        with f2:
            avail_tracks = sorted(
                df_all[df_all["manager"].isin(sel_managers)]["track"].unique().tolist()
            ) if sel_managers else []
            sel_tracks = st.multiselect(
                "מסלול",
                options=avail_tracks,
                help="בחירת מסלול השקעה. למשל כללי או מנייתי.",
                default=avail_tracks,
                key="ah_tracks",
            )

        with f3:
            avail_allocs = sorted(
                df_all[
                    df_all["manager"].isin(sel_managers) &
                    df_all["track"].isin(sel_tracks)
                ]["allocation_name"].unique().tolist()
            ) if sel_managers and sel_tracks else []
            sel_allocs = st.multiselect(
                "רכיב אלוקציה",
                options=avail_allocs,
                help="בחירת רכיבי החשיפה שיופיעו בגרפים ובטבלאות.",
                default=avail_allocs[:4] if len(avail_allocs) > 4 else avail_allocs,
                key="ah_allocs",
            )

        # Time range
        rng_col, cust_col = st.columns([2, 2])
        with rng_col:
            range_label = st.radio(
                "טווח זמן",
                options=["הכל", "YTD", "1Y", "3Y", "5Y", "מותאם אישית"],
                help="סינון לפי חלון זמן יחסי או תאריך התחלה מותאם אישית.",
                index=0,
                horizontal=True,
                key="ah_range",
                label_visibility="collapsed",
            )
            st.caption("⏱️ בחירת טווח זמן — עמוד עם הסמן על האפשרויות להסבר.")
        with cust_col:
            custom_start = None
            if range_label == "מותאם אישית":
                min_date_all, max_date_all = _date_bounds(df_all)
                custom_start = st.date_input(
                    "מתאריך",
                    value=min_date_all,
                    min_value=min_date_all,
                    max_value=max_date_all,
                    help="בחר תאריך התחלה ידני מתוך טווח התאריכים שקיים בנתונים.",
                    key="ah_custom_start",
                )

        # ── Apply filters ──────────────────────────────────────────────────────
        if not sel_managers or not sel_tracks or not sel_allocs:
            st.info("יש לבחור לפחות מנהל, מסלול ורכיב אחד.")
            return

        df = df_all[
            df_all["manager"].isin(sel_managers) &
            df_all["track"].isin(sel_tracks) &
            df_all["allocation_name"].isin(sel_allocs)
        ].copy()

        df = _filter_by_range(df, range_label, custom_start)

        if not df.empty:
            uniq_dates = sorted(pd.to_datetime(df["date"], errors="coerce").dropna().dt.to_period("M").astype(str).unique().tolist())
            with st.expander("🛠️ בדיקות טעינה וסינון", expanded=False):
                min_dbg, max_dbg = _date_bounds(df)
                st.write(f"מספר חודשי תצפית: **{len(uniq_dates)}**")
                st.write(f"טווח תאריכים: **{min_dbg} → {max_dbg}**")
                st.write(f"שורות אחרי סינון: **{len(df):,}**")
                st.write("דוגמת חודשים:", uniq_dates[:10], "...", uniq_dates[-10:] if len(uniq_dates) > 10 else "")
                if len(uniq_dates) <= 3:
                    st.warning("זוהו מעט מאוד חודשי תצפית. זה בדרך כלל מצביע על בעיית parsing במבנה הגיליון, לא על בעיית טווח זמן.")

        if df.empty:
            st.warning("אין נתונים לסינון הנוכחי.")
            return

        # ── View tabs ──────────────────────────────────────────────────────────
        tab_ts, tab_snap, tab_delta, tab_heat, tab_stats, tab_rank, tab_adv = st.tabs([
            "📈 סדרת זמן",
            "📍 Snapshot",
            "🔄 שינוי / Delta",
            "🌡️ Heatmap",
            "📊 סטטיסטיקות",
            "🏆 דירוג",
            "🔬 ניתוח מתקדם",
        ])

        # ── Tab 1: Time series ────────────────────────────────────────────────
        with tab_ts:
            fig = build_timeseries_chart(df, title="חשיפה לאורך זמן")
            _safe_plotly(fig, key="ah_ts")

            # Export filtered data
            dl_col, _ = st.columns([1, 5])
            with dl_col:
                st.download_button(
                    "⬇️ ייצוא CSV",
                    data=_to_csv_bytes(df),
                    file_name="allocation_history_filtered.csv",
                    mime="text/csv",
                    key="ah_dl_ts",
                )

        # ── Tab 2: Snapshot ───────────────────────────────────────────────────
        with tab_snap:
            max_d = df["date"].max().date()
            min_d = df["date"].min().date()
            snap_date = st.date_input(
                "בחר תאריך Snapshot",
                value=max_d,
                min_value=min_d,
                max_value=max_d,
                help="מציג את הערך האחרון הידוע עד לתאריך שנבחר.",
                key="ah_snap_date",
            )
            fig_snap = build_snapshot_chart(df, pd.Timestamp(snap_date))
            _safe_plotly(fig_snap, key="ah_snap")

            # Table view
            snap_df = df[df["date"] <= pd.Timestamp(snap_date)]
            if not snap_df.empty:
                idx = snap_df.groupby(["manager", "track", "allocation_name"])["date"].idxmax()
                snap_table = snap_df.loc[idx][
                    ["manager", "track", "allocation_name", "allocation_value", "date"]
                ].sort_values("allocation_value", ascending=False).reset_index(drop=True)
                snap_table.columns = ["מנהל", "מסלול", "רכיב", "ערך (%)", "תאריך"]
                st.dataframe(snap_table, use_container_width=True, hide_index=True)

        # ── Tab 3: Delta ──────────────────────────────────────────────────────
        with tab_delta:
            d_col1, d_col2 = st.columns(2)
            min_d = df["date"].min().date()
            max_d = df["date"].max().date()

            default_a = _clamp_date_value(max_d - timedelta(days=365), min_d, max_d)
            default_b = _clamp_date_value(max_d, min_d, max_d)

            with d_col1:
                date_a = st.date_input(
                    "תאריך A (מוצא)",
                    value=default_a,
                    min_value=min_d,
                    max_value=max_d,
                    help="תאריך ההשוואה ההתחלתי. אם אין מספיק היסטוריה, ברירת המחדל תיצמד לטווח הזמין.",
                    key="ah_date_a",
                )
            with d_col2:
                date_b = st.date_input(
                    "תאריך B (יעד)",
                    value=default_b,
                    min_value=min_d,
                    max_value=max_d,
                    help="תאריך ההשוואה הסופי.",
                    key="ah_date_b",
                )

            if date_a >= date_b:
                st.warning("תאריך A חייב להיות לפני תאריך B.")
            else:
                fig_delta, delta_df = build_delta_chart(
                    df, pd.Timestamp(date_a), pd.Timestamp(date_b)
                )
                _safe_plotly(fig_delta, key="ah_delta")

                if not delta_df.empty:
                    st.subheader("טבלת שינויים")
                    fmt = {c: "{:.2f}" for c in delta_df.columns if delta_df[c].dtype == float}
                    st.dataframe(delta_df.reset_index(drop=True), use_container_width=True, hide_index=True)

                    dl_col, _ = st.columns([1, 5])
                    with dl_col:
                        st.download_button(
                            "⬇️ ייצוא Delta CSV",
                            data=_to_csv_bytes(delta_df),
                            file_name="allocation_delta.csv",
                            mime="text/csv",
                            key="ah_dl_delta",
                        )

        # ── Tab 4: Heatmap ────────────────────────────────────────────────────
        with tab_heat:
            # Limit to recent 36 months by default for readability
            heat_df = df.copy()
            if len(df["date"].dt.to_period("M").unique()) > 36:
                cutoff = df["date"].max() - pd.DateOffset(months=36)
                heat_df = df[df["date"] >= cutoff]
                st.caption("הוצגו 36 חודשים אחרונים. בחר 'הכל' בטווח הזמן לתצוגה מלאה.")

            fig_heat = build_heatmap(heat_df, title="Heatmap — חשיפה חודשית")
            _safe_plotly(fig_heat, key="ah_heat")

        # ── Tab 5: Summary stats ──────────────────────────────────────────────
        with tab_stats:
            stats_df = build_summary_stats(df)
            if stats_df.empty:
                st.info("אין נתוני סטטיסטיקה.")
            else:
                st.dataframe(
                    stats_df.reset_index(drop=True),
                    use_container_width=True,
                    hide_index=True,
                )
                dl_col2, _ = st.columns([1, 5])
                with dl_col2:
                    st.download_button(
                        "⬇️ ייצוא סטטיסטיקות",
                        data=_to_csv_bytes(stats_df),
                        file_name="allocation_stats.csv",
                        mime="text/csv",
                        key="ah_dl_stats",
                    )

        # ── Tab 6: Ranking ────────────────────────────────────────────────────
        with tab_rank:
            if df["allocation_name"].nunique() > 1:
                rank_alloc = st.selectbox(
                    "רכיב לדירוג",
                    options=sorted(df["allocation_name"].unique()),
                    help="בחירת רכיב שלפיו יוצג הדירוג החודשי בין המנהלים.",
                    key="ah_rank_alloc",
                )
                rank_df = df[df["allocation_name"] == rank_alloc]
            else:
                rank_df = df

            if rank_df.empty:
                st.info("אין נתונים לדירוג.")
            else:
                fig_rank = build_ranking_chart(
                    rank_df,
                    title=f"דירוג מנהלים לאורך זמן — {rank_df['allocation_name'].iloc[0]}",
                )
                _safe_plotly(fig_rank, key="ah_rank")

                # Volatility table: std of monthly changes
                vol_rows = []
                for key, gdf in rank_df.groupby(["manager", "track"]):
                    gdf = gdf.sort_values("date")
                    monthly_chg = gdf["allocation_value"].diff().dropna()
                    vol_rows.append({
                        "מנהל":   key[0],
                        "מסלול":  key[1],
                        "תנודתיות (STD שינוי חודשי)": round(monthly_chg.std(), 3),
                        "שינוי מקסימלי (abs)": round(monthly_chg.abs().max(), 3),
                    })
                if vol_rows:
                    vol_df = pd.DataFrame(vol_rows).sort_values(
                        "תנודתיות (STD שינוי חודשי)", ascending=False
                    )
                    st.caption("מי הכי תנודתי?")
                    st.dataframe(vol_df.reset_index(drop=True), use_container_width=True, hide_index=True)

        # ── Tab 7: Advanced analysis ──────────────────────────────────────────
        with tab_adv:
            adv_tab1, adv_tab2 = st.tabs(["📐 פער בין מסלולים", "📏 פער מול ממוצע קבוצה"])

            with adv_tab1:
                st.markdown("#### פער בין מסלול כללי למנייתי (אותו גוף)")
                managers_with_multi_track = [
                    m for m in sel_managers
                    if df[df["manager"] == m]["track"].nunique() >= 2
                ]
                if not managers_with_multi_track:
                    st.info("אין גוף עם יותר ממסלול אחד בסינון הנוכחי.")
                else:
                    adv_mgr = st.selectbox(
                        "מנהל",
                        options=managers_with_multi_track,
                        key="ah_adv_mgr",
                    )
                    mgr_tracks = sorted(df[df["manager"] == adv_mgr]["track"].unique())
                    adv_alloc_options = sorted(
                        df[df["manager"] == adv_mgr]["allocation_name"].unique()
                    )

                    tc1, tc2, tc3 = st.columns(3)
                    with tc1:
                        t_a = st.selectbox("מסלול A", options=mgr_tracks,
                                           index=0, key="ah_ta")
                    with tc2:
                        t_b = st.selectbox("מסלול B", options=mgr_tracks,
                                           index=min(1, len(mgr_tracks) - 1),
                                           key="ah_tb")
                    with tc3:
                        gap_alloc = st.selectbox("רכיב", options=adv_alloc_options,
                                                 key="ah_gap_alloc")

                    if t_a != t_b:
                        fig_gap = build_track_gap_chart(df, adv_mgr, t_a, t_b, gap_alloc)
                        _safe_plotly(fig_gap, key="ah_track_gap")
                    else:
                        st.info("יש לבחור שני מסלולים שונים.")

            with adv_tab2:
                st.markdown("#### פער מול ממוצע הקבוצה")
                if not sel_managers:
                    st.info("יש לבחור מנהל.")
                else:
                    bench_mgr = st.selectbox(
                        "מנהל להשוואה",
                        options=sel_managers,
                        key="ah_bench_mgr",
                    )
                    bench_track_options = sorted(
                        df[df["manager"] == bench_mgr]["track"].unique()
                    )
                    bench_track = st.selectbox(
                        "מסלול",
                        options=bench_track_options,
                        key="ah_bench_track",
                    )
                    fig_bench = build_vs_benchmark_chart(df, bench_mgr, bench_track)
                    _safe_plotly(fig_bench, key="ah_vs_bench")

        # ── Raw data table ────────────────────────────────────────────────────
        with st.expander("📋 נתונים גולמיים מסוננים", expanded=False):
            disp = df[[
                "manager", "track", "date", "year", "month",
                "allocation_name", "allocation_value", "source_sheet",
            ]].copy()
            disp["date"] = disp["date"].dt.strftime("%Y-%m-%d")
            disp.columns = [
                "מנהל", "מסלול", "תאריך", "שנה", "חודש",
                "רכיב", "ערך (%)", "גליון מקור",
            ]
            st.dataframe(disp.reset_index(drop=True), use_container_width=True, hide_index=True)
            st.download_button(
                "⬇️ ייצוא כל הנתונים המסוננים",
                data=_to_csv_bytes(df),
                file_name="allocation_history_all.csv",
                mime="text/csv",
                key="ah_dl_all",
            )
