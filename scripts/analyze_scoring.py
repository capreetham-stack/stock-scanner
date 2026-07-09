#!/usr/bin/env python3
"""
analyze_scoring.py
===================
Analyze winner vs loser trades to refine scoring approach.
Extract all signals and compare characteristics of winners vs losers.
"""

import os
import sys
import json
import pandas as pd
import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config as cfg
from src.scanner import PreMarketScanner
from main import get_watchlist


def analyze_all_signals_vs_performance(performance_data):
    """
    Compare all signals scored by scanner to actual performance.
    Identify patterns in winning vs losing predictions.
    """
    print("\n" + "=" * 120)
    print("  SCORING ANALYSIS: Winner vs Loser Comparison")
    print("=" * 120)

    perf_map = {p["symbol"]: p for p in performance_data}

    watchlist = get_watchlist("NIFTY500")
    scanner = PreMarketScanner(watchlist=watchlist, max_workers=12, run_type="morning")
    result = scanner.run()

    all_signals = result.get("all_signals", [])
    buy_list = result.get("buy_list", [])

    # Find the signals for our prediction targets
    target_signals = {s.symbol: s for s in all_signals if s.symbol in perf_map}

    if not target_signals:
        print("  ⚠ No signals found for comparison targets")
        return

    analysis = []

    for symbol, sig in target_signals.items():
        perf = perf_map.get(symbol, {})
        current_price = perf.get("current", sig.current_price)
        entry = perf.get("entry", sig.entry)
        target_price = perf.get("target", sig.target)
        sl = perf.get("sl", sig.stop_loss)

        pnl = ((current_price - entry) / entry * 100) if entry else 0
        if current_price >= target_price:
            result_label = "WIN ✅"
            result_weight = 1.0
        elif current_price <= sl:
            result_label = "LOSS 🛑"
            result_weight = -1.0
        elif current_price > entry:
            result_label = "RUNNING_PROFIT 📈"
            result_weight = 0.5
        else:
            result_label = "RUNNING_LOSS 📉"
            result_weight = -0.5

        analysis.append({
            "symbol": symbol,
            "score": sig.score,
            "rsi": sig.rsi,
            "vol_ratio": sig.vol_ratio,
            "adx": sig.adx,
            "macd_hist": sig.macd_hist,
            "vwap_pullback": sig.vwap_pullback_ok,
            "high_conviction": sig.high_conviction,
            "supertrend": sig.supertrend_dir,
            "ema_aligned": "EMA stack bullish" in " ".join(sig.reasons),
            "current_price": current_price,
            "pnl_pct": pnl,
            "result": result_label,
            "result_weight": result_weight,
            "warnings_count": len(sig.warnings),
            "reasons_count": len(sig.reasons),
            "top_warnings": " | ".join(sig.warnings[:2]),
            "top_reasons": " | ".join(sig.reasons[:2]),
        })

    df = pd.DataFrame(analysis)

    # Print comparison
    print("\n  DETAILED COMPARISON:")
    print("  " + "─" * 116)
    print(f"  {'Symbol':<12} {'Score':>6} {'RSI':>6} {'RVOL':>6} {'ADX':>6} {'VWAP':>6} {'HC':>4} {'ST':>3} "
          f"{'EMA':>4} {'P&L%':>7} {'Result':>15} {'Warnings':>10}")
    print("  " + "─" * 116)

    for _, row in df.iterrows():
        print(f"  {row['symbol']:<12} {row['score']:>6.0f} {row['rsi']:>6.1f} {row['vol_ratio']:>6.2f}x {row['adx']:>6.1f} "
              f"{'Y' if row['vwap_pullback'] else 'N':>6} {'Y' if row['high_conviction'] else 'N':>4} "
              f"{row['supertrend']:>3.0f} {'Y' if row['ema_aligned'] else 'N':>4} {row['pnl_pct']:>6.1f}% "
              f"{row['result']:>15} {row['warnings_count']:>10}")

    print("\n  KEY INSIGHTS:")
    print("  " + "─" * 116)

    # Analyze winners vs losers
    winners = df[df["result_weight"] > 0]
    losers = df[df["result_weight"] < 0]

    if not winners.empty and not losers.empty:
        print(f"\n  WINNERS (n={len(winners)}):")
        print(f"    Avg Score: {winners['score'].mean():.1f} | Avg RSI: {winners['rsi'].mean():.1f} | "
              f"Avg RVOL: {winners['vol_ratio'].mean():.2f}x | Avg ADX: {winners['adx'].mean():.1f} | "
              f"Avg Warnings: {winners['warnings_count'].mean():.1f}")
        print(f"    VWAP Pullback %: {(winners['vwap_pullback'].sum() / len(winners) * 100):.0f}%")
        print(f"    High Conviction %: {(winners['high_conviction'].sum() / len(winners) * 100):.0f}%")
        print(f"    EMA Aligned %: {(winners['ema_aligned'].sum() / len(winners) * 100):.0f}%")

        print(f"\n  LOSERS (n={len(losers)}):")
        print(f"    Avg Score: {losers['score'].mean():.1f} | Avg RSI: {losers['rsi'].mean():.1f} | "
              f"Avg RVOL: {losers['vol_ratio'].mean():.2f}x | Avg ADX: {losers['adx'].mean():.1f} | "
              f"    Avg Warnings: {losers['warnings_count'].mean():.1f}")
        print(f"    VWAP Pullback %: {(losers['vwap_pullback'].sum() / len(losers) * 100):.0f}%")
        print(f"    High Conviction %: {(losers['high_conviction'].sum() / len(losers) * 100):.0f}%")
        print(f"    EMA Aligned %: {(losers['ema_aligned'].sum() / len(losers) * 100):.0f}%")

        print(f"\n  DISCRIMINATORS (Winner traits vs Loser traits):")
        score_diff = winners['score'].mean() - losers['score'].mean()
        rsi_diff = winners['rsi'].mean() - losers['rsi'].mean()
        rvol_diff = winners['vol_ratio'].mean() - losers['vol_ratio'].mean()
        adx_diff = winners['adx'].mean() - losers['adx'].mean()

        print(f"    Score: {score_diff:+.1f} (winners higher)")
        print(f"    RSI: {rsi_diff:+.1f} (winners higher means more overbought risk)")
        print(f"    RVOL: {rvol_diff:+.2f}x (winners higher volume)")
        print(f"    ADX: {adx_diff:+.1f} (winners have stronger trend)")
        print(f"    Warnings: {losers['warnings_count'].mean() - winners['warnings_count'].mean():+.1f} "
              f"(losers have more warnings)")

    print("\n" + "=" * 120)

    # Save analysis
    csv_file = f"logs/scoring_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_file, index=False)
    print(f"\n  ✓ Analysis saved to: {csv_file}\n")

    return df, winners, losers


if __name__ == "__main__":
    # Sample performance data from previous run
    performance_data = [
        {"symbol": "ANANTRAJ", "current": 542.65, "entry": 551.35, "target": 609.69, "sl": 522.18},
        {"symbol": "ATHERENERG", "current": 1194.40, "entry": 1164.20, "target": 1299.38, "sl": 1096.61},
    ]

    df, winners, losers = analyze_all_signals_vs_performance(performance_data)
