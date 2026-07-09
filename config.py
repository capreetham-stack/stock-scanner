"""
NSE Stock Scanner — Configuration
All thresholds, watchlists, and strategy parameters live here.
"""

# ─── Watchlist ────────────────────────────────────────────────────────────────
# NIFTY 50 symbols (NSE format)
NIFTY50 = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
    "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
    "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "SUNPHARMA",
    "TITAN", "BAJFINANCE", "WIPRO", "ONGC", "NTPC",
    "POWERGRID", "ULTRACEMCO", "TECHM", "HCLTECH", "INDUSINDBK",
    "TATAMOTORS", "TATASTEEL", "JSWSTEEL", "ADANIENT", "ADANIPORTS",
    "BAJAJFINSV", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
    "GRASIM", "HEROMOTOCO", "HINDALCO", "M&M", "NESTLEIND",
    "SBILIFE", "SHREECEM", "TATACONSUM", "BPCL", "CIPLA",
    "BRITANNIA", "HDFCLIFE", "UPL", "APOLLOHOSP", "LT"
]

# Additional F&O stocks worth scanning
FNO_EXTRAS = [
    "NIFTY", "BANKNIFTY", "FINNIFTY",
    "PIDILITIND", "BERGEPAINT", "MCDOWELL-N", "GODREJCP",
    "BOSCHLTD", "HAVELLS", "PAGEIND", "MUTHOOTFIN", "CHOLAFIN",
    "BANDHANBNK", "IDFCFIRSTB", "PNB", "CANBK", "FEDERALBNK",
    "RECLTD", "PFC", "SAIL", "NMDC", "NATIONALUM",
    "AUROPHARMA", "BIOCON", "TORNTPHARM", "ALKEM", "IPCALAB",
    "MINDTREE", "MPHASIS", "LTTS", "COFORGE", "PERSISTENT",
    "ABCAPITAL", "SBICARD", "MANAPPURAM", "LICHSGFIN",
]

# Full watchlist used for scanning
WATCHLIST = list(dict.fromkeys(NIFTY50 + FNO_EXTRAS))  # deduplicated

# ─── Time Settings ────────────────────────────────────────────────────────────
MARKET_OPEN_TIME  = "09:15"
PRE_MARKET_START  = "09:15"
SCAN_CUTOFF_TIME  = "09:14"   # Scanner must finish by this time
MID_DAY_SCAN_TIME = "12:15"   # 12:15 PM IST Mid-day run

# ─── Technical Indicator Parameters ──────────────────────────────────────────
RSI_PERIOD          = 14
RSI_OVERSOLD        = 35       # below this → bullish signal
RSI_OVERBOUGHT      = 65       # above this → sell / avoid
RSI_NEUTRAL_LOW     = 40
RSI_NEUTRAL_HIGH    = 60

MACD_FAST           = 12
MACD_SLOW           = 26
MACD_SIGNAL         = 9

BB_PERIOD           = 20
BB_STD              = 2.0

EMA_SHORT           = 9
EMA_MID             = 21
EMA_LONG            = 50
EMA_200             = 200

ATR_PERIOD          = 14
SUPERTREND_MULT     = 3.0

VWAP_DEVIATION_PCT  = 0.5     # within ±0.5 % of VWAP = near-VWAP
VWAP_PULLBACK_PCT   = 0.5     # strict pullback zone for high-conviction entries
VWAP_CHASE_PCT      = 1.2     # above this from VWAP = chase risk

# ─── Volume Analysis ──────────────────────────────────────────────────────────
VOLUME_AVG_PERIOD   = 20
VOLUME_SURGE_MULT   = 1.5     # today's vol > 1.5× avg ⇒ surge
RVOL_HIGH_CONVICTION = 1.0    # bullish-day relaxation: include solid movers with average RVOL

# RVOL penalty tuning (penalty = min(int((RVOL_HIGH_CONVICTION - rv) * RVOL_PENALTY_MULT), RVOL_PENALTY_MAX))
RVOL_PENALTY_MULT      = 12
RVOL_PENALTY_MAX       = 36

# MACD histogram quality filter (REFINED: ATHERENERG 12.79 won, ANANTRAJ 1.59 lost)
MIN_MACD_HISTOGRAM     = 3.0   # bullish-day relaxation: allow moderate MACD momentum

# Extreme RVOL penalty: >5x with weak ADX = risky (false signal)
MAX_RVOL_WITH_WEAK_TREND = 5.0  # if RVOL > this AND ADX < 30, penalize heavily
EXTREME_RVOL_PENALTY   = 15    # penalty for >5x RVOL when trend is weak

# Minimum RVOL required to appear in buy list (unless high_conviction override)
# default to the high-conviction threshold
MIN_RVOL_TO_QUALIFY    = 0.8

# Live-performance tuned quality gates (based on hourly positive-vs-negative analysis)
LIVE_POSITIVE_RVOL_MIN = 0.8
LIVE_POSITIVE_RVOL_MAX = 2.5
VERY_LOW_RVOL_CUTOFF   = 0.6
RSI_SOFT_OVERHEAT      = 75

# Trend / momentum / risk guards
MIN_EMA_PERIOD         = 50    # require price > EMA50 by default
RSI_MAX_FOR_BUY        = 80    # avoid buying if RSI > 80 (overbought)
HIGH_CONV_OVERRIDE_SCORE = 70  # allow overrides at lower high-conviction score
HIGH_SCORE_SOFT_OVERRIDE = 45  # allow soft override for high-score names with limited warnings

# ─── Market-Down Safety Rules ────────────────────────────────────────────────
MARKET_DOWN_MIN_STOCK_GAIN_PCT = 0.5  # stock must still be up at least this much when market is down
MARKET_DOWN_MIN_RVOL          = 0.5  # allow low RVOL on resilient stocks in weak markets (was 1.0)
MARKET_DOWN_RVOL_STRICT       = 0.8  # relaxed RVOL gate for strong relative strength (was 1.25)
MARKET_DOWN_RVOL_RELAXED      = 0.8  # relaxed further for exploration (was 1.15)
MARKET_DOWN_RELATIVE_STRENGTH_BONUS = 14
MARKET_DOWN_SECTOR_DECOUPLING_BONUS = 12
MARKET_DOWN_STRONG_RELATIVE_STRENGTH_PCT = 1.0  # relaxed from 2.0
MARKET_DOWN_STRONG_RVOL       = 1.0  # relaxed from 1.4
MARKET_DOWN_MAX_WARNINGS      = 3    # REFINED back to 3; ATHERENERG had 2 (win), ANANTRAJ had 5 (loss)
MARKET_DOWN_SECTOR_BONUS      = 10   # extra weight for resilient sectors when market is weak
VWAP_CHASE_PENALTY            = 3    # softer penalty for extended price above VWAP
TEMPORARY_MIN_SCORE_TO_BUY    = 15   # aggressive: lowered from 22 for intraday testing

# ─── Trend Strength (ADX) ───────────────────────────────────────────────────
ADX_STRONG_MIN      = 20      # bullish-day relaxation: keep trend filter but allow moderate ADX names

# ─── Demand / Supply Zone Parameters ─────────────────────────────────────────
DS_LOOKBACK_DAYS    = 60      # candles to look back for zones
DS_ZONE_STRENGTH    = 3       # min touches to call it a strong zone
DS_CLUSTER_PCT      = 0.5     # price within 0.5 % → same zone cluster
DS_FRESHNESS_BARS   = 5       # untested in last N bars = fresh zone
DS_PROXIMITY_PCT    = 1.0     # stock within 1 % of zone → trigger

# ─── Gap Analysis ─────────────────────────────────────────────────────────────
GAP_UP_PCT          = 0.5     # gap ≥ 0.5 % = meaningful gap-up
GAP_DOWN_PCT        = -0.5    # gap ≤ -0.5 % = meaningful gap-down

# ─── Signal Scoring Weights ───────────────────────────────────────────────────
SCORE_WEIGHTS = {
    "rsi_oversold":         15,
    "rsi_recovering":       10,
    "macd_crossover":       15,
    "macd_positive":         8,
    "ema_alignment":        12,   # short > mid > long
    "price_above_vwap":      8,
    "bollinger_bounce":     10,
    "demand_zone_near":     15,
    "volume_surge":         10,
    "supertrend_bullish":   10,
    "gap_up":                5,
    "support_bounce":        7,
    "prev_day_high_break":   8,
    "delivery_pct_high":     7,
    "buy_pressure":         12,

    # --- Hourly / Intraday Strategy Weights ---
    "1h_trend_aligned":     25,
    "15m_vwap_support":     15,
    "5m_breakout_vol":      20,
    "orb_vpoc_bullish":     15,
    "liquidity_sweep_trap": -20,
    "1030_reversal":        15,
}

MIN_SCORE_TO_BUY    = 18      # bullish-day relaxation to surface more qualifying names
TOP_N_STOCKS        = 10      # number of top picks to display

# ─── Risk Management ──────────────────────────────────────────────────────────
DEFAULT_RISK_PCT    = 0.5     # risk 0.5 % of capital per trade
SL_ATR_MULT         = 1.5     # stop-loss = entry - 1.5 × ATR
TARGET_RR           = 2.0     # minimum reward : risk ratio
RR_STRICT_MIN       = 1.0     # relax R:R gate further for exploratory scans
RR_STRICT_MIN_BREAKOUT = 1.5  # required R:R for breakout-mode entries

# Category caps to limit correlated indicator inflation
CATEGORY_CAPS = {
    # Revised buckets per architecture request
    "trend": 35,        # EMA stack, Supertrend, 1H trend, 30/90d momentum
    "momentum": 25,     # RSI, MACD, %B, candlesticks
    "volume": 20,       # RVOL, Delivery%, Volume surge
    "structure": 20,    # Demand zone, Pivots, VWAP, PDH
    "context": 15,      # Market regime, Relative strength, PCR, OI
    "other": 10,
}

# Regime multipliers (applied to category contributions)
REGIME_MULTIPLIERS = {
    "bull": {
        "trend": 1.2,
        "momentum": 1.1,
        "volume": 1.0,
        "structure": 0.9,
        "context": 1.0,
    },
    "bear": {
        "trend": 0.8,
        "momentum": 0.9,
        "volume": 1.1,
        "structure": 1.0,
        "context": 1.2,
    },
    "choppy": {
        "trend": 0.9,
        "momentum": 1.1,
        "volume": 1.0,
        "structure": 1.1,
        "context": 1.0,
    }
}

# Relative Strength (RS) kill-switch: disqualify stocks below this percentile
# Set to None to disable; otherwise value is 0-100 (e.g., 30 => disqualify bottom 30%)
RS_MIN_PERCENTILE = 30

# Mandatory kill switches / liquidity
MIN_AVG_DAILY_VOLUME = 5000    # avg 10-day volume threshold (relaxed for exploratory scans)
LOOSE_BEAR_200DMA = True       # allow bear-regime stocks below EMA200 when exploring signals

# Position sizing (not yet wired to execution): multiplier vs base size
POSITION_SIZING = {
    "25-40": 0.75,
    "40-55": 1.5,
    "55+": 2.0,
}

# Time decay (score loses this fraction per minute after trigger)
SCORE_TIME_DECAY_PER_MIN = 0.004  # ~0.4% per minute (~10% in 25 minutes)

# ─── Data Settings ────────────────────────────────────────────────────────────
HISTORICAL_DAYS     = 100     # days of OHLCV to fetch
INTRADAY_INTERVAL   = "5m"    # intraday candle size
DATA_CACHE_DIR      = "logs/cache"

# ─── Output / Logging ─────────────────────────────────────────────────────────
LOG_FILE            = "logs/scanner.log"
OUTPUT_CSV          = "output/buy_signals.csv"
OUTPUT_JSON         = "output/buy_signals.json"
CONSOLE_TOP_N       = 10
