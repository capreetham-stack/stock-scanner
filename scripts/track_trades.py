#!/usr/bin/env python3
"""
track_trades.py
================
Track buy signal predictions over time and calculate win rate.
Saves scan results to CSV with entry/target/SL, then checks 1h later.
"""

import os
import sys
import csv
import json
import pandas as pd
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config as cfg
from src.scanner import PreMarketScanner
from main import get_watchlist


def save_buy_list(buy_list, filename=None):
    """Save buy candidates with entry/target/SL to CSV."""
    if not buy_list:
        print("No buy candidates to track.")
        return None

    if filename is None:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"output/trade_predictions_{ts}.csv"

    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "symbol", "score", "price", "entry", "sl", "target", 
            "rr", "rsi", "vol_ratio", "rvol_desc", "reasons", "warnings"
        ])
        writer.writeheader()
        for sig in buy_list:
            writer.writerow({
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": sig.symbol,
                "score": sig.score,
                "price": f"{sig.current_price:.2f}",
                "entry": f"{sig.entry:.2f}",
                "sl": f"{sig.stop_loss:.2f}",
                "target": f"{sig.target:.2f}",
                "rr": f"{sig.reward_risk:.2f}x",
                "rsi": f"{sig.rsi:.1f}",
                "vol_ratio": f"{sig.vol_ratio:.2f}x",
                "rvol_desc": "Strong" if sig.vol_ratio >= 1.2 else "Moderate",
                "reasons": " | ".join(sig.reasons[:3]),
                "warnings": " | ".join(sig.warnings[:2]),
            })

    print(f"\n✓ Saved {len(buy_list)} buy candidates to: {filename}")
    return filename


def print_buy_list(buy_list):
    """Print buy list to console."""
    if not buy_list:
        print("No buy candidates.")
        return

    print("\n" + "=" * 130)
    print(f"  BUY CANDIDATES — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 130)
    print(f"  {'Rank':<4} {'Symbol':<10} {'Score':>5} {'Price':>8} "
          f"{'Entry':>8} {'SL':>8} {'Target':>8} {'R:R':>5} {'RSI':>6} {'RVOL':>6} "
          f"{'Status':<20}")
    print("  " + "─" * 126)

    for i, sig in enumerate(buy_list, 1):
        status = "High Conviction ⭐" if sig.high_conviction else "Trade Signal"
        print(f"  {i:<4} {sig.symbol:<10} {sig.score:>5} {sig.current_price:>8.2f} "
              f"{sig.entry:>8.2f} {sig.stop_loss:>8.2f} {sig.target:>8.2f} {sig.reward_risk:>5.2f}x "
              f"{sig.rsi:>6.1f} {sig.vol_ratio:>6.2f}x {status:<20}")

    print("=" * 130)
    print(f"  Total candidates: {len(buy_list)} | Market time: {datetime.datetime.now().strftime('%H:%M IST')}")
    print()


def run_scan_and_track(watchlist_name="NIFTY500", run_type="morning", workers=12):
    """Run scan and save/print buy list."""
    print(f"\n📊 Starting {run_type.upper()} scan on {watchlist_name}...")
    
    watchlist = get_watchlist(watchlist_name)
    scanner = PreMarketScanner(watchlist=watchlist, max_workers=workers, run_type=run_type)
    result = scanner.run()
    
    buy_list = result.get("buy_list", [])
    stats = result.get("stats", {})
    
    print(f"\nScan complete: Scanned {stats['scanned']} | "
          f"Qualified {len(buy_list)} | Skipped {stats['skipped']}")
    
    if buy_list:
        print_buy_list(buy_list)
        csv_file = save_buy_list(buy_list)
        return buy_list, csv_file
    else:
        print("⚠  No qualified candidates from scan.")
        return [], None


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--watchlist", default="NIFTY500", help="Watchlist to scan")
    p.add_argument("--run-type", default="morning", choices=["morning", "intraday"])
    p.add_argument("--workers", type=int, default=12)
    args = p.parse_args()

    buy_list, csv_file = run_scan_and_track(
        watchlist_name=args.watchlist,
        run_type=args.run_type,
        workers=args.workers
    )
