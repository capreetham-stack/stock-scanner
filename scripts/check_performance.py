#!/usr/bin/env python3
"""
check_performance.py
====================
Check actual prices 1h after scan and calculate win rate.
Run this every hour to track trade performance.
"""

import os
import sys
import csv
import json
import pandas as pd
import datetime
import yfinance as yf
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config as cfg
from src.nse_fetcher import NSEFetcher


def nse_to_yf(symbol):
    return f"{symbol}.NS"


def get_current_price(symbol):
    """Fetch current price from yfinance."""
    try:
        tk = yf.Ticker(nse_to_yf(symbol))
        data = tk.history(period="1d")
        if not data.empty:
            # yfinance columns are usually title-cased (Close), but keep a lowercase fallback.
            if "Close" in data.columns:
                return float(data["Close"].iloc[-1])
            if "close" in data.columns:
                return float(data["close"].iloc[-1])
    except Exception as e:
        print(f"  ⚠ {symbol}: price fetch failed ({e})")
    return None


def check_trade_performance(csv_file):
    """Compare predictions vs actual prices and calculate win/loss."""
    if not os.path.exists(csv_file):
        print(f"❌ File not found: {csv_file}")
        return

    df = pd.read_csv(csv_file)
    print(f"\n{'=' * 100}")
    print(f"  TRADE PERFORMANCE CHECK — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 100}")
    print(f"  {'Symbol':<10} {'Entry':>8} {'Current':>8} {'SL':>8} {'Target':>8} "
          f"{'P&L%':>7} {'Result':>15} {'Decision':<30}")
    print(f"  {'-' * 96}")

    wins = 0
    losses = 0
    breakeven = 0
    results = []
    pending = 0

    for _, row in df.iterrows():
        symbol = row['symbol']
        entry = float(row['entry'])
        sl = float(row['sl'])
        target = float(row['target'])

        current = get_current_price(symbol)
        if current is None:
            print(f"  {symbol:<10} {entry:>8.2f} {'N/A':>8} {sl:>8.2f} {target:>8.2f} {'N/A':>7} {'PENDING':>15}")
            pending += 1
            continue

        pnl_pct = ((current - entry) / entry) * 100 if entry else 0
        rows_data = {
            "check_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "entry": entry,
            "current": current,
            "sl": sl,
            "target": target,
            "pnl_pct": pnl_pct,
        }

        # Determine result
        if current <= sl:
            result = "STOPPED OUT 🛑"
            decision = "HIT SL — Close position"
            losses += 1
            rows_data["result"] = "loss"
        elif current >= target:
            result = "TARGET HIT ✅"
            decision = "Book profit — Exit"
            wins += 1
            rows_data["result"] = "win"
        elif current > entry:
            result = f"PROFIT 📈 (+{pnl_pct:.1f}%)"
            decision = f"Hold / Trailing SL to {entry:.2f}"
            breakeven += 1
            rows_data["result"] = "running_profit"
        elif current < entry:
            result = f"LOSS 📉 ({pnl_pct:.1f}%)"
            if current > sl:
                decision = f"Hold / SL at {sl:.2f}"
                breakeven += 1
                rows_data["result"] = "running_loss"
            else:
                decision = "Exit — Close"
        else:
            result = "BREAKEVEN ➡"
            decision = "Hold position"
            breakeven += 1
            rows_data["result"] = "breakeven"

        print(f"  {symbol:<10} {entry:>8.2f} {current:>8.2f} {sl:>8.2f} {target:>8.2f} "
              f"{pnl_pct:>6.1f}% {result:>15} {decision:<30}")
        results.append(rows_data)

    total = wins + losses + breakeven
    win_rate = (wins / total * 100) if total > 0 else 0

    print(f"  {'-' * 96}")
    print(f"\n  📊 SUMMARY:")
    print(f"     Wins: {wins}  | Losses: {losses}  | Running: {breakeven}  | Win Rate: {win_rate:.1f}%")
    print(f"{'=' * 100}\n")

    # Save results
    result_file = None
    if results:
        result_file = csv_file.replace("predictions", "results").replace(".csv", f"_{datetime.datetime.now().strftime('%H%M')}.csv")
        with open(result_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "check_time", "symbol", "entry", "current", "sl", "target", "pnl_pct", "result"
            ])
            writer.writeheader()
            writer.writerows(results)
        print(f"  ✓ Results saved to: {result_file}")

    return {
        "csv_file": csv_file,
        "wins": wins,
        "losses": losses,
        "running": breakeven,
        "pending": pending,
        "evaluated": total,
        "win_rate": win_rate,
        "result_file": result_file,
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=str, help="Path to predictions CSV", 
                   default=None)
    args = p.parse_args()

    if args.csv:
        check_trade_performance(args.csv)
    else:
        # Find the latest predictions file
        output_dir = "output"
        files = sorted(Path(output_dir).glob("trade_predictions_*.csv"), reverse=True)
        if files:
            check_trade_performance(str(files[0]))
        else:
            print("❌ No prediction files found in output/")
