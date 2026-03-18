# -*- coding: utf-8 -*-
"""
allocation_history_charts.py
────────────────────────────
Plotly chart builders for the Allocation History feature.

All charts use real datetime X-axes to guarantee correct chronological ordering.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from typing import Optional

# ─── Colour palette ───────────────────────────────────────────────────────────
_PALETTE = [
    "#3A7AFE", "#10B981", "#F59E0B", "#EF4444",
    "#8B5CF6", "#EC4899", "#06B6D4", "#84CC16",
    "#F97316", "#6366F1", "#14B8A6", "#FB7185",
]

_LAYOUT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(248,249,252,1)",
    font=dict(family="Segoe UI, -apple-system, sans-serif", size=12, color="#374151"),
    margin=dict(l=10, r=10, t=40, b=10),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=-0.35,
        xanchor="center",
        x=0.5,
        font=dict(size=11),
        bgcolor="rgba(0,0,0,0)",
    ),
    hovermode="x unified",
)


def _apply_base(fig: go.Figure, title: str = "", height: int = 420) -> go.Figure:
    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=title, font=dict(size=14, color="#1F3A5F"), x=0.5),
        height=height,
    )
    fig.update_xaxes(
        type="date",
        tickformat="%b %Y",
        gridcolor="#E5E7EB",
        tickfont=dict(size=11),
    )
    fig.update_yaxes(
        ticksuffix="%",
        gridcolor="#E5E7EB",
        zeroline=False,
        tickfont=dict(size=11),
    )
    return fig


# ─── 1. Time-series chart ─────────────────────────────────────────────────────

def build_timeseries_chart(
    df: pd.DataFrame,
    title: str = "סדרת זמן — חשיפה לאורך זמן",
    height: int = 450,
) -> go.Figure:
    """
    Line chart with one trace per (manager, track, allocation_name).
    X-axis is a real datetime axis.
    df must contain: date, allocation_value, manager, track, allocation_name
    """
    fig = go.Figure()

    groups = df.groupby(["manager", "track", "allocation_name"])
    for idx, (key, gdf) in enumerate(groups):
        manager, track, alloc = key
        label = f"{manager} {track} — {alloc}"
        color = _PALETTE[idx % len(_PALETTE)]

        gdf = gdf.sort_values("date")
        fig.add_trace(go.Scatter(
            x=gdf["date"],
            y=gdf["allocation_value"],
            mode="lines",
            name=label,
            line=dict(color=color, width=2),
            hovertemplate=f"<b>{label}</b><br>תאריך: %{{x|%b %Y}}<br>ערך: %{{y:.1f}}%<extra></extra>",
        ))

    return _apply_base(fig, title, height)


# ─── 2. Multi-manager comparison chart ───────────────────────────────────────

def build_comparison_chart(
    df: pd.DataFrame,
    title: str = "השוואה בין גופים",
    height: int = 450,
) -> go.Figure:
    """
    One line per (manager + track).  Useful for comparing the same allocation
    component across several investment managers.
    """
    return build_timeseries_chart(df, title=title, height=height)


# ─── 3. Snapshot bar chart ───────────────────────────────────────────────────

def build_snapshot_chart(
    df: pd.DataFrame,
    snapshot_date: pd.Timestamp,
    title: Optional[str] = None,
    height: int = 380,
) -> go.Figure:
    """
    Horizontal bar chart showing all allocation components at a given date.
    Uses the closest available date ≤ snapshot_date for each series.
    """
    # For each (manager, track, allocation_name) pick the closest date ≤ snapshot_date
    df_past = df[df["date"] <= snapshot_date].copy()
    idx = (
        df_past.groupby(["manager", "track", "allocation_name"])["date"]
        .idxmax()
    )
    snap = df_past.loc[idx].reset_index(drop=True)

    if snap.empty:
        return go.Figure().update_layout(**_LAYOUT_BASE, title="אין נתונים לתאריך זה")

    snap["label"] = snap["manager"] + " " + snap["track"] + " — " + snap["allocation_name"]
    snap = snap.sort_values("allocation_value", ascending=True)

    title = title or f"Snapshot — {snapshot_date.strftime('%b %Y')}"

    fig = go.Figure(go.Bar(
        x=snap["allocation_value"],
        y=snap["label"],
        orientation="h",
        marker_color=_PALETTE[0],
        text=snap["allocation_value"].map(lambda v: f"{v:.1f}%"),
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=title, font=dict(size=14, color="#1F3A5F"), x=0.5),
        height=max(height, 50 + 30 * len(snap)),
        xaxis=dict(ticksuffix="%", gridcolor="#E5E7EB"),
        yaxis=dict(gridcolor="#E5E7EB", tickfont=dict(size=10)),
        showlegend=False,
    )
    return fig


# ─── 4. Delta / change analysis chart ────────────────────────────────────────

def build_delta_chart(
    df: pd.DataFrame,
    date_a: pd.Timestamp,
    date_b: pd.Timestamp,
    title: Optional[str] = None,
    height: int = 380,
) -> tuple[go.Figure, pd.DataFrame]:
    """
    Bar chart of change (delta) between two dates per (manager, track, allocation).
    Returns (figure, delta_df).
    """
    def snap_at(df_: pd.DataFrame, dt: pd.Timestamp) -> pd.DataFrame:
        past = df_[df_["date"] <= dt]
        idx = past.groupby(["manager", "track", "allocation_name"])["date"].idxmax()
        s = past.loc[idx].copy()
        s["snap_date"] = dt
        return s

    s_a = snap_at(df, date_a)
    s_b = snap_at(df, date_b)

    merged = s_a.merge(
        s_b,
        on=["manager", "track", "allocation_name"],
        suffixes=("_a", "_b"),
    )
    if merged.empty:
        return go.Figure().update_layout(**_LAYOUT_BASE, title="אין נתונים להשוואה"), pd.DataFrame()

    merged["delta"] = merged["allocation_value_b"] - merged["allocation_value_a"]
    merged["delta_pct"] = np.where(
        merged["allocation_value_a"] != 0,
        merged["delta"] / merged["allocation_value_a"] * 100,
        np.nan,
    )
    merged["label"] = (
        merged["manager"] + " " + merged["track"] + " — " + merged["allocation_name"]
    )
    merged = merged.sort_values("delta")

    colors = ["#EF4444" if d < 0 else "#10B981" for d in merged["delta"]]

    date_a_str = date_a.strftime("%b %Y")
    date_b_str = date_b.strftime("%b %Y")
    title = title or f"שינוי: {date_a_str} → {date_b_str}"

    fig = go.Figure(go.Bar(
        x=merged["delta"],
        y=merged["label"],
        orientation="h",
        marker_color=colors,
        text=merged["delta"].map(lambda v: f"{v:+.1f}pp"),
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            f"מ-{date_a_str}: %{{customdata[0]:.1f}}%<br>"
            f"ל-{date_b_str}: %{{customdata[1]:.1f}}%<br>"
            "שינוי: %{x:+.1f} נקודות אחוז<extra></extra>"
        ),
        customdata=merged[["allocation_value_a", "allocation_value_b"]].values,
    ))
    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=title, font=dict(size=14, color="#1F3A5F"), x=0.5),
        height=max(height, 50 + 30 * len(merged)),
        xaxis=dict(ticksuffix="pp", gridcolor="#E5E7EB", zeroline=True,
                   zerolinecolor="#9CA3AF", zerolinewidth=1.5),
        yaxis=dict(gridcolor="#E5E7EB", tickfont=dict(size=10)),
        showlegend=False,
    )

    delta_df = merged[[
        "manager", "track", "allocation_name",
        "allocation_value_a", "allocation_value_b",
        "delta", "delta_pct",
    ]].rename(columns={
        "allocation_value_a": f"ערך ב-{date_a_str}",
        "allocation_value_b": f"ערך ב-{date_b_str}",
        "delta": "שינוי (pp)",
        "delta_pct": "שינוי יחסי (%)",
    })

    return fig, delta_df


# ─── 5. Monthly heatmap ───────────────────────────────────────────────────────

def build_heatmap(
    df: pd.DataFrame,
    title: str = "Heatmap — חשיפה חודשית",
    height: int = 400,
) -> go.Figure:
    """
    Rows = (manager + track + allocation_name), Columns = year-month.
    Cell colour = allocation_value.
    """
    df = df.copy()
    df["ym"] = df["date"].dt.to_period("M").dt.to_timestamp()
    df["row"] = df["manager"] + " " + df["track"] + " — " + df["allocation_name"]

    pivot = (
        df.groupby(["row", "ym"])["allocation_value"]
        .mean()
        .unstack("ym")
    )
    pivot = pivot[sorted(pivot.columns)]  # ensure chronological order

    col_labels = [c.strftime("%b %Y") for c in pivot.columns]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=col_labels,
        y=pivot.index.tolist(),
        colorscale="Blues",
        text=np.round(pivot.values, 1),
        texttemplate="%{text:.1f}%",
        hoverongaps=False,
        hovertemplate="<b>%{y}</b><br>%{x}<br>%{z:.1f}%<extra></extra>",
        colorbar=dict(ticksuffix="%", len=0.8),
    ))

    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=title, font=dict(size=14, color="#1F3A5F"), x=0.5),
        height=max(height, 80 + 35 * len(pivot)),
        xaxis=dict(type="category", tickfont=dict(size=10), tickangle=-45),
        yaxis=dict(tickfont=dict(size=10)),
    )
    return fig


# ─── 6. Summary statistics table ─────────────────────────────────────────────

def build_summary_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a summary DataFrame with one row per (manager, track, allocation_name).
    Columns: mean, min, max, std, last_value, mom_avg (avg monthly change),
             mom_max (max abs monthly change), ytd_change.
    """
    rows = []
    for key, gdf in df.groupby(["manager", "track", "allocation_name"]):
        gdf = gdf.sort_values("date")
        vals = gdf["allocation_value"].dropna()
        if vals.empty:
            continue
        diffs = vals.diff().dropna()

        last_val = vals.iloc[-1]
        # YTD: compare to Jan 1 of current year
        this_year = gdf[gdf["year"] == gdf["date"].dt.year.max()]
        jan_val = this_year[this_year["month"] == 1]["allocation_value"]
        ytd = last_val - jan_val.iloc[0] if not jan_val.empty else np.nan

        # 12-month change
        latest_date = gdf["date"].max()
        one_yr_ago = latest_date - pd.DateOffset(months=12)
        yr_ago_df = gdf[gdf["date"] <= one_yr_ago]
        yr_ago_val = yr_ago_df.iloc[-1]["allocation_value"] if not yr_ago_df.empty else np.nan
        ch_12m = last_val - yr_ago_val if not np.isnan(yr_ago_val) else np.nan

        rows.append({
            "מנהל":             key[0],
            "מסלול":            key[1],
            "רכיב":             key[2],
            "ממוצע (%)":        round(vals.mean(), 2),
            "מינימום (%)":      round(vals.min(), 2),
            "מקסימום (%)":      round(vals.max(), 2),
            "סטיית תקן":        round(vals.std(), 2),
            "שינוי חודשי ממוצע": round(diffs.mean(), 2) if not diffs.empty else np.nan,
            "שינוי חודשי מקסימלי": round(diffs.abs().max(), 2) if not diffs.empty else np.nan,
            "שינוי 12 חודש (pp)": round(ch_12m, 2) if not np.isnan(ch_12m) else np.nan,
            "שינוי YTD (pp)":   round(ytd, 2) if not np.isnan(ytd) else np.nan,
            "ערך אחרון (%)":    round(last_val, 2),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ─── 7. Ranking chart ─────────────────────────────────────────────────────────

def build_ranking_chart(
    df: pd.DataFrame,
    title: str = "דירוג מנהלים לאורך זמן",
    height: int = 400,
) -> go.Figure:
    """
    For each month, ranks managers by allocation_value (descending).
    Shows rank=1 = highest exposure.
    """
    df = df.copy()
    df["ym"] = df["date"].dt.to_period("M").dt.to_timestamp()
    df["row"] = df["manager"] + " " + df["track"]

    pivot = (
        df.groupby(["row", "ym"])["allocation_value"]
        .mean()
        .unstack("ym")
    )
    pivot = pivot[sorted(pivot.columns)]
    rank_df = pivot.rank(axis=0, ascending=False, method="min")

    fig = go.Figure()
    for idx, row in enumerate(rank_df.index):
        fig.add_trace(go.Scatter(
            x=rank_df.columns,
            y=rank_df.loc[row],
            mode="lines+markers",
            name=row,
            line=dict(color=_PALETTE[idx % len(_PALETTE)], width=2),
            marker=dict(size=5),
            hovertemplate=f"<b>{row}</b><br>%{{x|%b %Y}}<br>דירוג: %{{y:.0f}}<extra></extra>",
        ))

    fig.update_layout(
        **_LAYOUT_BASE,
        title=dict(text=title, font=dict(size=14, color="#1F3A5F"), x=0.5),
        height=height,
    )
    fig.update_xaxes(type="date", tickformat="%b %Y", gridcolor="#E5E7EB")
    fig.update_yaxes(
        autorange="reversed",
        title="דירוג (1 = הגבוה ביותר)",
        gridcolor="#E5E7EB",
        dtick=1,
        tickfont=dict(size=11),
    )
    return fig


# ─── 8. Gap between tracks of same manager ────────────────────────────────────

def build_track_gap_chart(
    df: pd.DataFrame,
    manager: str,
    track_a: str,
    track_b: str,
    allocation_name: str,
    title: Optional[str] = None,
    height: int = 420,
) -> go.Figure:
    """
    Shows two lines (track_a vs track_b) and a shaded gap between them.
    """
    def _get(track: str) -> pd.DataFrame:
        return (
            df[
                (df["manager"] == manager)
                & (df["track"] == track)
                & (df["allocation_name"] == allocation_name)
            ]
            .sort_values("date")
        )

    da = _get(track_a)
    db = _get(track_b)

    if da.empty or db.empty:
        return go.Figure().update_layout(**_LAYOUT_BASE, title="נתונים חסרים")

    title = title or f"{manager}: {track_a} vs {track_b} — {allocation_name}"
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=da["date"], y=da["allocation_value"],
        name=f"{manager} {track_a}",
        line=dict(color=_PALETTE[0], width=2.5),
        hovertemplate="<b>" + track_a + "</b><br>%{x|%b %Y}<br>%{y:.1f}%<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=db["date"], y=db["allocation_value"],
        name=f"{manager} {track_b}",
        line=dict(color=_PALETTE[1], width=2.5),
        fill="tonexty",
        fillcolor="rgba(58,122,254,0.10)",
        hovertemplate="<b>" + track_b + "</b><br>%{x|%b %Y}<br>%{y:.1f}%<extra></extra>",
    ))

    return _apply_base(fig, title, height)


# ─── 9. Gap vs group benchmark ────────────────────────────────────────────────

def build_vs_benchmark_chart(
    df: pd.DataFrame,
    selected_manager: str,
    selected_track: str,
    title: Optional[str] = None,
    height: int = 420,
) -> go.Figure:
    """
    Plots selected manager/track vs. the median of all managers for the same
    allocation_name and track (or all tracks if desired).
    """
    df = df.copy()
    df["ym"] = df["date"].dt.to_period("M").dt.to_timestamp()

    # Group benchmark: median per allocation_name per month across all managers
    bench = (
        df.groupby(["allocation_name", "ym"])["allocation_value"]
        .median()
        .reset_index()
        .rename(columns={"ym": "date", "allocation_value": "benchmark"})
    )

    sel = df[
        (df["manager"] == selected_manager) & (df["track"] == selected_track)
    ].copy()
    sel["ym"] = sel["date"].dt.to_period("M").dt.to_timestamp()
    sel = sel.groupby(["allocation_name", "ym"])["allocation_value"].mean().reset_index()
    sel = sel.rename(columns={"ym": "date"})

    merged = sel.merge(bench, on=["allocation_name", "date"])
    if merged.empty:
        return go.Figure().update_layout(**_LAYOUT_BASE, title="אין נתוני בנצ'מרק")

    merged["gap"] = merged["allocation_value"] - merged["benchmark"]
    title = title or f"פער מול ממוצע קבוצה — {selected_manager} {selected_track}"

    fig = go.Figure()
    for idx, alloc in enumerate(merged["allocation_name"].unique()):
        sub = merged[merged["allocation_name"] == alloc].sort_values("date")
        color = _PALETTE[idx % len(_PALETTE)]
        fig.add_trace(go.Scatter(
            x=sub["date"], y=sub["gap"],
            mode="lines",
            name=alloc,
            line=dict(color=color, width=2),
            hovertemplate=f"<b>{alloc}</b><br>%{{x|%b %Y}}<br>פער: %{{y:+.1f}}pp<extra></extra>",
        ))

    fig.add_hline(y=0, line_dash="dash", line_color="#9CA3AF")
    return _apply_base(fig, title, height)
