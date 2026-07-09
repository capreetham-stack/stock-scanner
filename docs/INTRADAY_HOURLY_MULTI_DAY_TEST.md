# Intraday + Hourly Multi-Day Test Guide

This project now includes an automated test flow for:
- Morning scan
- Intraday scan
- Hourly performance checks
- Multi-day aggregation

## What was added

- `scripts/multi_day_intraday_hourly_test.py`
  - Runs morning and intraday scans
  - Runs hourly checks against generated prediction files
  - Writes aggregate summary CSV

- `scripts/check_performance.py` improvement
  - Fixed live price read for yfinance by using `Close` (with `close` fallback)
  - Returns structured summary usable by automation

- `src/signals.py` refinement
  - RVOL live-success band bonus
  - Very-low-RVOL penalty
  - RSI-overheat + weak-volume penalty
  - Hard rank gating for low RVOL quality and weak R:R

## Run a quick smoke test

```bash
python3 scripts/multi_day_intraday_hourly_test.py --days 1 --hours 1 --sleep-minutes 1 --watchlist NIFTY500 --workers 12
```

## Run for next few days (manual day-by-day)

Recommended to avoid long-running local process interruption:

```bash
# Repeat once per day (morning)
python3 scripts/multi_day_intraday_hourly_test.py --days 1 --hours 6 --sleep-minutes 60 --watchlist NIFTY500 --workers 12
```

This creates:
- `output/trade_predictions_*.csv`
- `output/trade_results_*.csv`
- `output/multi_day_test_summary.csv`

## Run as one long cycle (if machine will stay on)

```bash
python3 scripts/multi_day_intraday_hourly_test.py --days 5 --hours 6 --sleep-minutes 60 --watchlist NIFTY500 --workers 12
```

## Interpreting metrics

Use two metrics together:
1. Closed win rate: target/SL hit outcomes only
2. Live positive rate: current price above entry (intraday momentum health)

## GitHub push checklist

1. Validate local run:
   ```bash
   python3 scripts/multi_day_intraday_hourly_test.py --days 1 --hours 1 --sleep-minutes 1
   ```
2. Commit code + docs
3. Push branch:
   ```bash
   git push origin revert-hourly-071d810
   ```
4. Open PR to main

## Known limitations

- NSE APIs may return `403/404/503`; scanner falls back to yfinance.
- yfinance can rate-limit (`Too Many Requests`) under heavy scan frequency.
- For reliable multi-day collection, prefer one daily run rather than repeated ad-hoc loops.
