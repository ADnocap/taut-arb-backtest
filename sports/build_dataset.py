"""Sports prediction dataset builder — CLI orchestrator + CSV export.

Usage:
    python -m sports.build_dataset --sports NBA NFL Soccer --step 1
    python -m sports.build_dataset --sports NBA --output sports_dataset.csv
"""

import argparse
import asyncio
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from scipy.optimize import brentq

from sports.config import SPORTS
from sports.database import SportsDatabase
from sports.matcher import match_events

console = Console()

STEPS = {
    1: "Discover sports markets",
    2: "Collect Polymarket price histories",
    3: "Collect Odds API historical odds",
    4: "Cross-reference events",
    5: "Generate CSV dataset",
    6: "Generate charts",
}

DEFAULT_OUTPUT = "sports_dataset.csv"


def _make_progress(*, indeterminate=False):
    if indeterminate:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TextColumn("{task.completed} found"),
            TimeElapsedColumn(),
            console=console,
        )
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("({task.completed}/{task.total})"),
        TimeElapsedColumn(),
        console=console,
    )


async def run_pipeline(
    sports: list[str],
    step: int | None = None,
    output: str = DEFAULT_OUTPUT,
):
    async with SportsDatabase() as db:
        steps_to_run = [step] if step else list(STEPS.keys())

        for s in steps_to_run:
            console.rule(f"[bold]Step {s}: {STEPS[s]}")

            if s == 1:
                from sports.collectors.polymarket_markets import SportsMarketsCollector

                with _make_progress(indeterminate=True) as progress:
                    task = progress.add_task("Discovering markets...", total=None)
                    async with SportsMarketsCollector() as collector:
                        count = await collector.collect(
                            db, sports, progress=progress, task_id=task,
                        )
                console.print(f"  Saved {count} sports markets")

                # Print per-sport breakdown
                for sport in sports:
                    markets = await db.get_sports_markets(sport)
                    console.print(f"    {sport}: {len(markets)} markets")

            elif s == 2:
                from sports.collectors.polymarket_prices import SportsPricesCollector

                async with SportsPricesCollector() as collector:
                    for sport in sports:
                        missing = await db.get_markets_missing_prices(sport)
                        with _make_progress() as progress:
                            task = progress.add_task(
                                f"{sport} prices", total=len(missing),
                            )
                            stats = await collector.collect(
                                db, sport, progress=progress, task_id=task,
                            )
                        console.print(
                            f"  {sport}: Goldsky={stats['goldsky_success']}, "
                            f"CLOB={stats['clob_fallback']}, "
                            f"no data={stats['no_data']}"
                        )

            elif s == 3:
                from sports.collectors.odds_api import OddsApiCollector

                with _make_progress() as progress:
                    task = progress.add_task("Fetching odds...", total=None)
                    async with OddsApiCollector() as collector:
                        stats = await collector.collect(
                            db, sports, progress=progress, task_id=task,
                        )
                for sport, count in stats.items():
                    console.print(f"  {sport}: {count} odds snapshots")
                total_credits = await db.get_total_credits_used()
                console.print(f"  Total credits used: {total_credits:,}")

            elif s == 4:
                total_mkts = 0
                for sp in sports:
                    total_mkts += len(await db.get_sports_markets(sp))
                with _make_progress() as progress:
                    task = progress.add_task("Matching events...", total=total_mkts)
                    stats = await match_events(
                        db, sports, progress=progress, task_id=task,
                    )
                table = Table(title="Match Report")
                table.add_column("Sport")
                table.add_column("Matched", justify="right")
                table.add_column("Unmatched", justify="right")
                table.add_column("Avg Score", justify="right")
                for sport, s_stats in stats.items():
                    table.add_row(
                        sport,
                        str(s_stats["matched"]),
                        str(s_stats["unmatched"]),
                        f"{s_stats['avg_score']:.3f}",
                    )
                console.print(table)

            elif s == 5:
                matched_count = await db.count_matched_events()
                with _make_progress() as progress:
                    task = progress.add_task("Generating CSV...", total=matched_count)
                    rows_written = await generate_csv(
                        db, output, progress=progress, task_id=task,
                    )
                console.print(f"  Wrote {rows_written} rows to {output}")

            elif s == 6:
                build_charts(output)

        # Summary
        console.rule("[bold]Summary")
        counts = await db.get_table_counts()
        table = Table(title="Table Counts")
        table.add_column("Table")
        table.add_column("Rows", justify="right")
        for t, c in counts.items():
            table.add_row(t, f"{c:,}")
        console.print(table)


def _devig_additive(inv_odds: list[float]) -> list[float]:
    """Additive vig removal — best for n=2 (equivalent to Shin)."""
    booksum = sum(inv_odds)
    if booksum <= 1.0:
        return [p / booksum for p in inv_odds]  # no vig
    margin_per = (booksum - 1.0) / len(inv_odds)
    probs = [p - margin_per for p in inv_odds]
    if any(p <= 0 for p in probs):
        return [p / booksum for p in inv_odds]  # fallback
    return probs


def _devig_power(inv_odds: list[float]) -> list[float]:
    """Power method vig removal — best for n>=3."""
    booksum = sum(inv_odds)
    if booksum <= 1.0:
        return [p / booksum for p in inv_odds]  # no vig
    def objective(k):
        return sum(p ** k for p in inv_odds) - 1.0
    try:
        k = brentq(objective, 1.0, 100.0)
        return [p ** k for p in inv_odds]
    except ValueError:
        return [p / booksum for p in inv_odds]  # fallback


async def generate_csv(
    db: SportsDatabase, output_path: str,
    *, progress=None, task_id=None,
) -> int:
    """Generate the final CSV dataset.

    For each matched event, align Polymarket price timestamps with nearest
    Odds API snapshot and compute vig-removed implied probability.
    """
    matched = await db.get_matched_events()
    if not matched:
        console.print("[yellow]  No matched events — cannot generate CSV")
        return 0

    rows = []

    for me in matched:
        cid = me["condition_id"]
        odds_eid = me["odds_event_id"]
        a_is_home = bool(me["poly_team_a_is_home"])
        sport = me["sport"]
        question = me.get("question", "")
        team_a = me.get("team_a", "")
        game_date = me.get("game_date", "")

        # Build event name
        event_name = f"{sport}: {question}" if question else f"{sport}: {team_a} {game_date}"

        # Get price history
        prices = await db.get_price_history(cid)
        if not prices:
            continue

        # Forward-fill price history on 15-min grid
        BUCKET = 15 * 60  # 15 minutes in seconds
        timestamps = [p["timestamp"] for p in prices]
        ts_min = min(timestamps)
        ts_max = max(timestamps)

        # Index original prices by timestamp for O(1) lookup
        price_by_ts: dict[int, dict] = {}
        for p in prices:
            price_by_ts[p["timestamp"]] = p

        # Build forward-filled grid
        filled_prices = []
        last_a: float | None = None
        last_b: float | None = None
        ts = ts_min
        while ts <= ts_max:
            if ts in price_by_ts:
                orig = price_by_ts[ts]
                cur_a = orig["team_a_price"] if orig["team_a_price"] is not None else last_a
                cur_b = orig["team_b_price"] if orig["team_b_price"] is not None else last_b
                filled_prices.append({
                    "timestamp": ts,
                    "team_a_price": cur_a,
                    "team_b_price": cur_b,
                    "forward_filled": 0,
                })
            else:
                filled_prices.append({
                    "timestamp": ts,
                    "team_a_price": last_a,
                    "team_b_price": last_b,
                    "forward_filled": 1,
                })
            # Update carry-forward values
            if filled_prices[-1]["team_a_price"] is not None:
                last_a = filled_prices[-1]["team_a_price"]
            if filled_prices[-1]["team_b_price"] is not None:
                last_b = filled_prices[-1]["team_b_price"]
            ts += BUCKET

        # Get odds snapshots (use consensus/average across bookmakers per timestamp)
        raw_odds = await db.get_odds_for_event(odds_eid)
        if not raw_odds:
            continue

        # Build odds timeline: average across bookmakers at each snapshot_ts
        odds_by_ts: dict[int, dict] = {}
        for o in raw_odds:
            ts = o["snapshot_ts"]
            if ts not in odds_by_ts:
                odds_by_ts[ts] = {
                    "home_sum": 0.0, "away_sum": 0.0,
                    "draw_sum": 0.0, "draw_count": 0, "count": 0,
                }
            odds_by_ts[ts]["home_sum"] += o["home_odds"]
            odds_by_ts[ts]["away_sum"] += o["away_odds"]
            odds_by_ts[ts]["count"] += 1
            if o["draw_odds"]:
                odds_by_ts[ts]["draw_sum"] += o["draw_odds"]
                odds_by_ts[ts]["draw_count"] += 1

        odds_timeline = []
        for ts in sorted(odds_by_ts):
            d = odds_by_ts[ts]
            n = d["count"]
            home_avg = d["home_sum"] / n
            away_avg = d["away_sum"] / n
            draw_avg = (d["draw_sum"] / d["draw_count"]) if d["draw_count"] > 0 else None
            odds_timeline.append((ts, home_avg, away_avg, draw_avg))

        if not odds_timeline:
            continue

        # For each price timestamp, find nearest odds snapshot
        for p in filled_prices:
            price_ts = p["timestamp"]
            poly_price = p["team_a_price"]
            poly_price_b = p["team_b_price"]
            ff = p["forward_filled"]
            if poly_price is None:
                continue

            # Find nearest odds snapshot within +/-15 min (900 seconds)
            best_odds = None
            best_dist = float("inf")
            for ots, h_odds, a_odds, d_odds in odds_timeline:
                dist = abs(price_ts - ots)
                if dist < best_dist:
                    best_dist = dist
                    best_odds = (h_odds, a_odds, d_odds)

            if best_odds is None or best_dist > 900:
                continue

            h_odds, a_odds, d_odds = best_odds

            # Which odds correspond to team_a?
            if a_is_home:
                tracked_odds = h_odds
                other_odds = a_odds
            else:
                tracked_odds = a_odds
                other_odds = h_odds

            # Vig-removed implied probability
            inv_tracked = 1.0 / tracked_odds if tracked_odds > 0 else 0
            inv_other = 1.0 / other_odds if other_odds > 0 else 0
            inv_draw = (1.0 / d_odds) if d_odds and d_odds > 0 else 0

            is_draw = "draw" in question.lower()

            if inv_draw > 0:
                # 3-way market (soccer): power method
                probs = _devig_power([inv_tracked, inv_other, inv_draw])
                if probs[0] + probs[1] + probs[2] <= 0:
                    continue
                implied_prob = probs[2] if is_draw else probs[0]
            else:
                # Binary market: additive method
                if inv_tracked <= 0 or inv_other <= 0:
                    continue
                probs = _devig_additive([inv_tracked, inv_other])
                implied_prob = probs[0]

            # Format datetime
            dt_str = datetime.fromtimestamp(price_ts, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

            rows.append({
                "datetime": dt_str,
                "event_name": event_name,
                "poly_price": round(poly_price, 4),
                "poly_price_b": round(poly_price_b, 4) if poly_price_b is not None else "",
                "odds": round(implied_prob, 4),
                "forward_filled": ff,
            })

        if progress is not None and task_id is not None:
            progress.update(task_id, advance=1)

    # Sort by datetime
    rows.sort(key=lambda r: r["datetime"])

    # Write CSV
    if rows:
        fieldnames = ["datetime", "event_name", "poly_price", "poly_price_b", "odds", "forward_filled"]
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        # Print forward-fill stats
        ff_count = sum(1 for r in rows if r["forward_filled"] == 1)
        total = len(rows)
        pct = (ff_count / total * 100) if total > 0 else 0
        console.print(f"    {ff_count} of {total} rows forward-filled ({pct:.1f}%)")

    return len(rows)


SPORT_COLORS = {
    "Soccer": "#f4a261",
    "MLB": "#2a9d8f",
    "NFL": "#e76f51",
    "NHL": "#264653",
    "NBA": "#e9c46a",
    "Tennis": "#606c38",
}

DEFAULT_CHART_DIR = Path("sports/charts")


def build_charts(
    csv_path: str = DEFAULT_OUTPUT,
    chart_dir: Path | str = DEFAULT_CHART_DIR,
):
    """Generate 8 diagnostic charts from the sports CSV dataset."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import numpy as np
    import pandas as pd

    chart_dir = Path(chart_dir)
    chart_dir.mkdir(parents=True, exist_ok=True)
    charts_made = 0

    def save(fig, name):
        nonlocal charts_made
        path = chart_dir / name
        fig.savefig(str(path), dpi=120, bbox_inches="tight")
        plt.close(fig)
        charts_made += 1
        console.print(f"    Saved {name}")

    console.print(f"  Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    df["sport"] = df["event_name"].str.split(":").str[0]
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    sport_order = ["NBA", "NFL", "NHL", "MLB", "Soccer", "Tennis"]
    sports_present = [s for s in sport_order if s in df["sport"].unique()]
    colors = [SPORT_COLORS.get(s, "#888888") for s in sports_present]

    # ---- 1. Rows per sport (horizontal bar) ----
    counts = df.groupby("sport").size().reindex(sports_present)
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(counts.index, counts.values, color=colors)
    ax.set_xlabel("Rows")
    ax.set_title("Data Volume per Sport")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    for bar, val in zip(bars, counts.values):
        ax.text(val + counts.max() * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=9)
    ax.invert_yaxis()
    save(fig, "rows_per_sport.png")

    # ---- 2. Events per sport (vertical bar) ----
    events = df.groupby("sport")["event_name"].nunique().reindex(sports_present)
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(events.index, events.values, color=colors)
    ax.set_ylabel("Unique Events")
    ax.set_title("Events per Sport")
    for bar, val in zip(bars, events.values):
        ax.text(bar.get_x() + bar.get_width() / 2, val + events.max() * 0.01,
                str(val), ha="center", va="bottom", fontsize=9)
    save(fig, "events_per_sport.png")

    # ---- 3. Polymarket vs Odds (hexbin scatter) ----
    valid = df.dropna(subset=["poly_price", "odds"])
    fig, ax = plt.subplots(figsize=(6, 6))
    hb = ax.hexbin(valid["odds"], valid["poly_price"], gridsize=50,
                    cmap="YlOrRd", mincnt=1)
    ax.plot([0, 1], [0, 1], "k--", linewidth=1, alpha=0.6, label="45° reference")
    ax.set_xlabel("Bookmaker Implied Probability (vig-removed)")
    ax.set_ylabel("Polymarket Price")
    ax.set_title("Polymarket Price vs Bookmaker Implied Probability")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect("equal")
    ax.legend(loc="upper left")
    fig.colorbar(hb, ax=ax, label="Count")
    save(fig, "poly_vs_odds_scatter.png")

    # ---- 4. Edge distribution (histogram) ----
    edge = valid["poly_price"] - valid["odds"]
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(edge, bins=100, color="#2a9d8f", alpha=0.8, edgecolor="white",
            linewidth=0.3)
    mean_edge = edge.mean()
    median_edge = edge.median()
    ax.axvline(mean_edge, color="#e76f51", linewidth=1.5, linestyle="--",
               label=f"Mean = {mean_edge:.4f}")
    ax.axvline(median_edge, color="#264653", linewidth=1.5, linestyle="-.",
               label=f"Median = {median_edge:.4f}")
    ax.set_xlabel("Edge (poly_price − odds)")
    ax.set_ylabel("Frequency")
    ax.set_title("Distribution of Edge (Polymarket − Bookmaker)")
    ax.legend()
    save(fig, "edge_distribution.png")

    # ---- 5. Coverage over time (multi-line) ----
    df["month"] = df["datetime"].dt.tz_localize(None).dt.to_period("M")
    monthly = df.groupby(["month", "sport"])["event_name"].nunique().unstack(fill_value=0)
    monthly = monthly.reindex(columns=sports_present, fill_value=0)
    fig, ax = plt.subplots(figsize=(10, 5))
    for sport in sports_present:
        if sport in monthly.columns:
            vals = monthly[sport]
            ax.plot(vals.index.astype(str), vals.values,
                    marker="o", markersize=3, linewidth=1.5,
                    color=SPORT_COLORS.get(sport), label=sport)
    ax.set_xlabel("Month")
    ax.set_ylabel("Unique Events")
    ax.set_title("Monthly Event Coverage by Sport")
    ax.legend(loc="upper left")
    plt.xticks(rotation=45, ha="right")
    save(fig, "coverage_over_time.png")

    # ---- 6. Event trajectories (2×3 grid) ----
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    axes_flat = axes.flatten()
    for idx, sport in enumerate(sports_present[:6]):
        ax = axes_flat[idx]
        sport_df = df[df["sport"] == sport]
        # Pick the event with the most data points
        event_counts = sport_df.groupby("event_name").size()
        top_event = event_counts.idxmax()
        ev_df = sport_df[sport_df["event_name"] == top_event].sort_values("datetime")
        ax.plot(ev_df["datetime"], ev_df["poly_price"],
                color=SPORT_COLORS.get(sport), linewidth=1, label="Poly price")
        ax.plot(ev_df["datetime"], ev_df["odds"],
                color="#333333", linewidth=1, linestyle="--", alpha=0.7, label="Odds")
        # Truncate title
        short_title = top_event if len(top_event) <= 40 else top_event[:37] + "..."
        ax.set_title(short_title, fontsize=9)
        ax.set_ylim(0, 1)
        ax.tick_params(labelsize=7)
        if idx == 0:
            ax.legend(fontsize=7)
    # Hide unused axes
    for idx in range(len(sports_present), 6):
        axes_flat[idx].set_visible(False)
    fig.suptitle("Example Event Trajectories (one per sport)", fontsize=12)
    fig.tight_layout()
    save(fig, "event_trajectories.png")

    # ---- 7. Calibration curve ----
    cal_df = valid[["odds", "poly_price"]].copy()
    cal_df["odds_bin"] = pd.cut(cal_df["odds"], bins=20, labels=False)
    cal = cal_df.groupby("odds_bin").agg(
        odds_mean=("odds", "mean"),
        poly_mean=("poly_price", "mean"),
        count=("odds", "size"),
    )
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(cal["odds_mean"], cal["poly_mean"], s=cal["count"] / cal["count"].max() * 200,
               color="#2a9d8f", alpha=0.8, edgecolors="white", zorder=3)
    ax.plot(cal["odds_mean"], cal["poly_mean"], color="#2a9d8f", linewidth=1, alpha=0.5)
    ax.plot([0, 1], [0, 1], "k--", linewidth=1, alpha=0.6, label="Perfect calibration")
    ax.set_xlabel("Mean Bookmaker Implied Probability (binned)")
    ax.set_ylabel("Mean Polymarket Price")
    ax.set_title("Calibration: Bookmaker vs Polymarket")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect("equal")
    ax.legend(loc="upper left")
    save(fig, "calibration_curve.png")

    # ---- 8. Data summary table ----
    summary_rows = []
    for sport in sports_present:
        sdf = df[df["sport"] == sport]
        n_rows = len(sdf)
        n_events = sdf["event_name"].nunique()
        date_min = sdf["datetime"].min().strftime("%Y-%m-%d")
        date_max = sdf["datetime"].max().strftime("%Y-%m-%d")
        ff_pct = (sdf["forward_filled"].sum() / n_rows * 100) if n_rows else 0
        summary_rows.append([sport, f"{n_rows:,}", str(n_events), date_min, date_max, f"{ff_pct:.1f}%"])

    # Total row
    total_rows = len(df)
    total_events = df["event_name"].nunique()
    total_ff = df["forward_filled"].sum() / total_rows * 100 if total_rows else 0
    summary_rows.append([
        "TOTAL", f"{total_rows:,}", str(total_events),
        df["datetime"].min().strftime("%Y-%m-%d"),
        df["datetime"].max().strftime("%Y-%m-%d"),
        f"{total_ff:.1f}%",
    ])

    col_labels = ["Sport", "Rows", "Events", "From", "To", "% Forward-filled"]
    fig, ax = plt.subplots(figsize=(9, 3))
    ax.axis("off")
    table = ax.table(
        cellText=summary_rows,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    # Style header
    for j in range(len(col_labels)):
        table[(0, j)].set_facecolor("#264653")
        table[(0, j)].set_text_props(color="white", fontweight="bold")
    # Style total row
    total_idx = len(summary_rows)
    for j in range(len(col_labels)):
        table[(total_idx, j)].set_facecolor("#e9c46a")
        table[(total_idx, j)].set_text_props(fontweight="bold")
    fig.suptitle("Dataset Summary", fontsize=13, fontweight="bold")
    save(fig, "data_summary.png")

    console.print(f"  Generated {charts_made} charts in {chart_dir}/")


def main():
    parser = argparse.ArgumentParser(
        description="Sports prediction dataset: Polymarket vs sportsbook odds"
    )
    parser.add_argument(
        "--sports",
        nargs="+",
        default=["NBA"],
        choices=list(SPORTS.keys()),
        help="Sports to process (default: NBA)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_sports",
        help="Process all sports (overrides --sports)",
    )
    parser.add_argument(
        "--step",
        type=int,
        choices=list(STEPS.keys()),
        help="Run a single step",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    sports = list(SPORTS.keys()) if args.all_sports else args.sports
    console.print(f"[bold green]Sports dataset builder — sports: {sports}")
    asyncio.run(run_pipeline(sports, args.step, args.output))
    console.print("[bold green]Done!")


if __name__ == "__main__":
    main()
