#!/usr/bin/env python3
"""
multi_day_intraday_hourly_test.py
=================================
Run a multi-day test plan that captures both morning and intraday recommendations,
then performs hourly performance checks for each generated prediction file.

Example:
  python3 scripts/multi_day_intraday_hourly_test.py --days 5 --hours 6 --watchlist NIFTY500
"""

import argparse
import datetime as dt
import time
from pathlib import Path

from track_trades import run_scan_and_track
from check_performance import check_trade_performance


def now_str():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_one_day(day_index: int, total_days: int, watchlist: str, workers: int, hourly_checks: int, sleep_minutes: int):
    print("\n" + "=" * 110)
    print(f"DAY {day_index}/{total_days} START — {now_str()}")
    print("=" * 110)

    generated_files = []

    # Morning scan
    _, morning_file = run_scan_and_track(watchlist_name=watchlist, run_type="morning", workers=workers)
    if morning_file:
        generated_files.append(morning_file)

    # Intraday scan
    _, intraday_file = run_scan_and_track(watchlist_name=watchlist, run_type="intraday", workers=workers)
    if intraday_file:
        generated_files.append(intraday_file)

    if not generated_files:
        print("No prediction files generated today. Skipping hourly checks.")
        return []

    summaries = []
    for hour_idx in range(1, hourly_checks + 1):
        print("\n" + "-" * 110)
        print(f"HOURLY CHECK {hour_idx}/{hourly_checks} — {now_str()}")
        print("-" * 110)

        for pred_file in generated_files:
            summary = check_trade_performance(pred_file)
            if summary:
                summary["day"] = day_index
                summary["hour_check"] = hour_idx
                summaries.append(summary)

        if hour_idx < hourly_checks:
            print(f"Waiting {sleep_minutes} minute(s) for next hourly check...")
            time.sleep(max(1, sleep_minutes) * 60)

    return summaries


def write_summary_csv(rows, out_file: Path):
    import csv

    out_file.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp", "day", "hour_check", "csv_file", "wins", "losses", "running", "pending",
        "evaluated", "win_rate", "result_file"
    ]
    with out_file.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "timestamp": now_str(),
                "day": r.get("day"),
                "hour_check": r.get("hour_check"),
                "csv_file": r.get("csv_file"),
                "wins": r.get("wins", 0),
                "losses": r.get("losses", 0),
                "running": r.get("running", 0),
                "pending": r.get("pending", 0),
                "evaluated": r.get("evaluated", 0),
                "win_rate": f"{r.get('win_rate', 0):.2f}",
                "result_file": r.get("result_file") or "",
            })


def print_final_summary(rows):
    if not rows:
        print("\nNo evaluation rows captured.")
        return

    wins = sum(int(r.get("wins", 0)) for r in rows)
    losses = sum(int(r.get("losses", 0)) for r in rows)
    evaluated = sum(int(r.get("evaluated", 0)) for r in rows)
    pending = sum(int(r.get("pending", 0)) for r in rows)

    overall = (wins / evaluated * 100.0) if evaluated > 0 else 0.0

    print("\n" + "=" * 110)
    print("MULTI-DAY TEST SUMMARY")
    print("=" * 110)
    print(f"Total checked rows: {len(rows)}")
    print(f"Evaluated outcomes: {evaluated} | Wins: {wins} | Losses: {losses} | Pending: {pending}")
    print(f"Current computed win rate: {overall:.2f}%")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=5, help="How many test days to run")
    p.add_argument("--hours", type=int, default=6, help="Hourly checks per day")
    p.add_argument("--sleep-minutes", type=int, default=60, help="Minutes between hourly checks")
    p.add_argument("--watchlist", default="NIFTY500", help="Watchlist to scan")
    p.add_argument("--workers", type=int, default=12, help="Scanner worker count")
    p.add_argument("--out", default="output/multi_day_test_summary.csv", help="Summary CSV output")
    args = p.parse_args()

    all_rows = []
    for day in range(1, max(1, args.days) + 1):
        day_rows = run_one_day(
            day_index=day,
            total_days=args.days,
            watchlist=args.watchlist,
            workers=args.workers,
            hourly_checks=max(1, args.hours),
            sleep_minutes=max(1, args.sleep_minutes),
        )
        all_rows.extend(day_rows)

        if day < args.days:
            print("\nEnd of day cycle. Waiting until next day window is managed externally.")
            print("Tip: run this script once daily, or keep --days 1 for manual day-by-day control.")

    out_file = Path(args.out)
    write_summary_csv(all_rows, out_file)
    print(f"\nSaved aggregated test summary: {out_file}")
    print_final_summary(all_rows)


if __name__ == "__main__":
    main()
