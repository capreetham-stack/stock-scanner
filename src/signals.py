"""
src/signals.py
===============
Signal scoring engine.

Combines all indicators + demand/supply analysis into a single numerical
score for each stock.  The score determines buy priority.

Scoring rules (additive, weights from config.SCORE_WEIGHTS)
──────────────────────────────────────────────────────────────
• RSI oversold               → strong bullish setup
• RSI recovering             → momentum turning
• MACD crossover             → trend change confirmation
• MACD positive              → already in uptrend
• EMA bullish alignment      → structural uptrend
• Price above VWAP           → short-term strength
• Bollinger lower-band bounce→ oversold + potential reversal
• Demand zone nearby         → institutional support
• Volume surge               → conviction buy
• Supertrend bullish         → trend follower confirmation
• Gap-up from prev close     → overnight interest / news
• Support bounce (pivot)     → floor confirmed
• Prev day high breakout     → breakout trade
• High delivery percentage   → genuine buying (not speculative)

Negative adjustments
──────────────────────
• RSI overbought             → reduce score
• Bearish engulfing candle   → reduce score
• Price inside supply zone   → reduce score
• ADX < 20 (non-trending)    → reduce score
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config as cfg
from src.indicators    import Indicators
from src.demand_supply import DemandSupplyAnalyzer

logger = logging.getLogger(__name__)

W = cfg.SCORE_WEIGHTS


class StockSignal:
    """Holds the analysis result for one stock."""

    def __init__(self, symbol: str):
        self.symbol   = symbol
        self.score    = 0
        self.reasons  : list[str] = []
        self.warnings : list[str] = []
        # price info
        self.current_price  = 0.0
        self.prev_close     = 0.0
        self.gap_pct        = 0.0
        self.entry          = 0.0
        self.stop_loss      = 0.0
        self.target         = 0.0
        self.reward_risk    = 0.0
        # key indicators
        self.rsi            = 0.0
        self.macd_hist      = 0.0
        self.adx            = 0.0
        self.vol_ratio      = 0.0
        self.supertrend_dir = 0
        self.atr            = 0.0
        self.pattern        = "none"
        self.adx_rising     = False
        self.vwap_pullback_ok = False
        self.high_conviction = False
        self.buy_qty        = 0.0
        self.sell_qty       = 0.0
        self.buy_sell_ratio = None
        # zone info
        self.nearest_demand_zone = None
        self.nearest_supply_zone = None
        self.demand_proximity    = 999.0
        self.supply_proximity    = 999.0
        self.indicator_messages: dict[str, str] = {}
        self.chg_7d_pct = None
        self.chg_30d_pct = None
        self.chg_90d_pct = None
        self.buy_heading = ""
        self.buy_reason_summary = ""
        self.caution_summary = ""

    def add(self, points: int, reason: str):
        self.score   += points
        self.reasons.append(f"+{points} {reason}")

    def subtract(self, points: int, reason: str):
        self.score   = max(0, self.score - points)
        self.warnings.append(f"-{points} {reason}")

    def to_dict(self) -> dict:
        return {
            "symbol":          self.symbol,
            "score":           self.score,
            "current_price":   round(self.current_price, 2),
            "gap_pct":         round(self.gap_pct, 2),
            "rsi":             round(self.rsi, 1),
            "macd_hist":       round(self.macd_hist, 4),
            "adx":             round(self.adx, 1),
            "vol_ratio":       round(self.vol_ratio, 2),
            "supertrend":      "BULL" if self.supertrend_dir == 1 else "BEAR",
            "atr":             round(self.atr, 2),
            "pattern":         self.pattern,
            "adx_rising":      self.adx_rising,
            "vwap_pullback_ok": self.vwap_pullback_ok,
            "high_conviction": self.high_conviction,
            "buy_qty":         round(self.buy_qty, 0),
            "sell_qty":        round(self.sell_qty, 0),
            "buy_sell_ratio":  (round(self.buy_sell_ratio, 2)
                                 if self.buy_sell_ratio is not None else None),
            "entry":           round(self.entry, 2),
            "stop_loss":       round(self.stop_loss, 2),
            "target":          round(self.target, 2),
            "reward_risk":     round(self.reward_risk, 2),
            "demand_zone":     (f"{self.nearest_demand_zone.bottom:.2f}–"
                                f"{self.nearest_demand_zone.top:.2f}"
                                if self.nearest_demand_zone else "N/A"),
            "demand_prox_pct": round(self.demand_proximity, 2),
            "supply_zone":     (f"{self.nearest_supply_zone.bottom:.2f}–"
                                f"{self.nearest_supply_zone.top:.2f}"
                                if self.nearest_supply_zone else "N/A"),
            "chg_7d_pct":      (round(self.chg_7d_pct, 2)
                                 if self.chg_7d_pct is not None else None),
            "chg_30d_pct":     (round(self.chg_30d_pct, 2)
                                 if self.chg_30d_pct is not None else None),
            "chg_90d_pct":     (round(self.chg_90d_pct, 2)
                                 if self.chg_90d_pct is not None else None),
            "buy_heading":      self.buy_heading,
            "why_buy":          self.buy_reason_summary,
            "cautions_summary": self.caution_summary,
            "reasons":         " | ".join(self.reasons),
            "warnings":        " | ".join(self.warnings),
            "indicator_messages": self.indicator_messages,
        }


class SignalEngine:
    """Scores each stock and returns a sorted StockSignal list."""

    def __init__(self):
        self._ds = DemandSupplyAnalyzer()

    @staticmethod
    def _period_change(close_series: pd.Series, sessions: int) -> float | None:
        vals = close_series.dropna()
        if len(vals) <= sessions:
            return None
        base = float(vals.iloc[-(sessions + 1)])
        curr = float(vals.iloc[-1])
        if base == 0:
            return None
        return (curr - base) / base * 100

    @staticmethod
    def _build_buy_heading(sig: StockSignal) -> str:
        reasons = " | ".join(sig.reasons)
        if sig.high_conviction:
            return "BUY: High Conviction (Trend + RVOL + VWAP Pullback + R:R)"
        long_term_weak = (
            sig.chg_30d_pct is not None and sig.chg_90d_pct is not None and
            sig.chg_30d_pct < 0 and sig.chg_90d_pct < 0
        )
        if long_term_weak and sig.rsi <= cfg.RSI_NEUTRAL_LOW:
            return "WATCHLIST: Reversal Only"
        if long_term_weak:
            return "WATCHLIST: Long-Term Trend Weak"
        if (
            sig.chg_30d_pct is not None and sig.chg_90d_pct is not None and
            sig.chg_30d_pct > 0 and sig.chg_90d_pct > 0 and
            "EMA stack bullish aligned" in reasons
        ):
            return "BUY: Trend Continuation"
        if "Near demand zone" in reasons and "MACD" in reasons:
            return "BUY: Demand Zone + Momentum"
        if "RSI oversold" in reasons and "Bullish pattern" in reasons:
            return "BUY: Oversold Reversal Setup"
        if "Price above VWAP" in reasons and "Breaking prev day high" in reasons:
            return "BUY: Intraday Strength Breakout"
        return "BUY: Multi-signal Confluence"

    def score_stock(
        self,
        symbol:        str,
        daily_df:      pd.DataFrame,   # daily OHLCV
        intraday_df:   pd.DataFrame    = None,  # intraday (optional)
        delivery_pct:  float           = 0.0,
        pcr:           float           = None,
        buy_qty:       float           = 0.0,
        sell_qty:      float           = 0.0,
        buy_sell_ratio: float | None   = None,
    ) -> StockSignal:

        sig = StockSignal(symbol)

        if daily_df.empty or len(daily_df) < 30:
            logger.debug("%s: not enough data", symbol)
            return sig

        # ── Compute all indicators ────────────────────────────────────────────
        df  = Indicators.compute_all(daily_df)
        row = df.iloc[-1]   # today's / latest bar
        prev_row = df.iloc[-2]

        sig.current_price  = float(row["close"])
        sig.prev_close     = float(prev_row["close"])
        sig.gap_pct        = (sig.current_price - sig.prev_close) / sig.prev_close * 100
        sig.rsi            = float(row.get("rsi", 50))
        sig.macd_hist      = float(row.get("macd_hist", 0))
        sig.adx            = float(row.get("adx", 0))
        sig.vol_ratio      = float(row.get("vol_ratio", 1))
        sig.supertrend_dir = int(row.get("supertrend_direction", -1))
        sig.atr            = float(row.get("atr", 0))
        sig.pattern        = str(row.get("pattern", "none"))
        sig.buy_qty        = float(buy_qty or 0.0)
        sig.sell_qty       = float(sell_qty or 0.0)
        sig.buy_sell_ratio = float(buy_sell_ratio) if buy_sell_ratio is not None else None

        # Per-indicator explanation messages (one message for each indicator).
        rsi_val = sig.rsi
        if rsi_val < cfg.RSI_OVERSOLD:
            rsi_msg = f"RSI {rsi_val:.1f}: oversold, potential rebound setup"
        elif rsi_val > cfg.RSI_OVERBOUGHT:
            rsi_msg = f"RSI {rsi_val:.1f}: overbought, avoid chasing"
        else:
            rsi_msg = f"RSI {rsi_val:.1f}: neutral momentum"

        macd_now = float(row.get("macd", 0.0))
        macd_sig = float(row.get("macd_signal", 0.0))
        if macd_now > macd_sig:
            macd_msg = f"MACD bullish: MACD {macd_now:.2f} above signal {macd_sig:.2f}"
        else:
            macd_msg = f"MACD bearish: MACD {macd_now:.2f} below signal {macd_sig:.2f}"

        bb_pct_b = float(row.get("bb_pct_b", 0.5))
        if bb_pct_b < 0.2:
            bb_msg = f"Bollinger: near lower band (%B {bb_pct_b:.2f}), bounce zone"
        elif bb_pct_b > 0.8:
            bb_msg = f"Bollinger: near upper band (%B {bb_pct_b:.2f}), extended"
        else:
            bb_msg = f"Bollinger: mid-range (%B {bb_pct_b:.2f})"

        e_short = float(row.get(f"ema{cfg.EMA_SHORT}", 0.0))
        e_mid   = float(row.get(f"ema{cfg.EMA_MID}", 0.0))
        e_long  = float(row.get(f"ema{cfg.EMA_LONG}", 0.0))
        if e_short > e_mid > e_long:
            ema_msg = "EMA(9/21/50): bullish alignment"
        elif e_short < e_mid < e_long:
            ema_msg = "EMA(9/21/50): bearish alignment"
        else:
            ema_msg = "EMA(9/21/50): mixed alignment"

        st_msg = "Supertrend: bullish" if sig.supertrend_dir == 1 else "Supertrend: bearish"

        vwap_val = float(row.get("vwap", sig.current_price))
        vwap_dist_pct_msg = (
            abs(sig.current_price - vwap_val) / vwap_val * 100
            if vwap_val else 0.0
        )
        if sig.current_price >= vwap_val and vwap_dist_pct_msg <= cfg.VWAP_PULLBACK_PCT:
            vwap_msg = (
                f"VWAP: pullback zone ({vwap_dist_pct_msg:.2f}% from VWAP {vwap_val:.2f})"
            )
        elif sig.current_price > vwap_val:
            vwap_msg = (
                f"VWAP: extended {vwap_dist_pct_msg:.2f}% above VWAP {vwap_val:.2f}"
            )
        else:
            vwap_msg = f"VWAP: price {sig.current_price:.2f} below VWAP {vwap_val:.2f}"

        atr_pct = (sig.atr / sig.current_price * 100) if sig.current_price else 0.0
        atr_msg = f"ATR: {sig.atr:.2f} ({atr_pct:.2f}% daily volatility)"

        if sig.vol_ratio >= cfg.RVOL_HIGH_CONVICTION:
            vol_msg = f"Volume: RVOL strong at {sig.vol_ratio:.2f}x average"
        else:
            vol_msg = (
                f"Volume: {sig.vol_ratio:.2f}x average (below {cfg.RVOL_HIGH_CONVICTION:.1f}x)"
            )

        stoch_k = float(row.get("stoch_k", 50.0))
        stoch_d = float(row.get("stoch_d", 50.0))
        if stoch_k < 20 and stoch_d < 20:
            stoch_msg = f"Stochastic: oversold (K {stoch_k:.1f}, D {stoch_d:.1f})"
        elif stoch_k > 80 and stoch_d > 80:
            stoch_msg = f"Stochastic: overbought (K {stoch_k:.1f}, D {stoch_d:.1f})"
        else:
            stoch_msg = f"Stochastic: neutral (K {stoch_k:.1f}, D {stoch_d:.1f})"

        prev_adx = float(prev_row.get("adx", np.nan))
        sig.adx_rising = bool(np.isfinite(prev_adx) and sig.adx > prev_adx)

        adx_val = sig.adx
        if adx_val >= cfg.ADX_STRONG_MIN and sig.adx_rising:
            adx_msg = f"ADX: {adx_val:.1f}, strong and rising (prev {prev_adx:.1f})"
        elif adx_val >= cfg.ADX_STRONG_MIN:
            adx_msg = f"ADX: {adx_val:.1f}, strong but not rising (prev {prev_adx:.1f})"
        elif adx_val >= 20:
            adx_msg = f"ADX: {adx_val:.1f}, moderate trend (prev {prev_adx:.1f})"
        else:
            adx_msg = f"ADX: {adx_val:.1f}, weak trend/chop"

        # MACD df for crossover check
        macd_df = df[["macd", "macd_signal", "macd_hist"]].rename(
            columns={"macd_signal": "signal"}
        )

        # ── Demand / Supply zones ─────────────────────────────────────────────
        ds = self._ds.analyze(df)
        sig.nearest_demand_zone = ds["nearest_demand"]
        sig.nearest_supply_zone = ds["nearest_supply"]
        sig.demand_proximity    = ds["demand_proximity_pct"]
        sig.supply_proximity    = ds["supply_proximity_pct"]

        # ── Entry / Stop / Target (set early so R:R can be used) ─────────────
        sig.entry     = sig.current_price
        sig.stop_loss = sig.entry - cfg.SL_ATR_MULT * sig.atr
        reward_risk   = self._ds.reward_risk(ds, sig.entry, sig.atr)
        sig.reward_risk = reward_risk
        if ds["nearest_supply"] and reward_risk >= cfg.TARGET_RR:
            sig.target = ds["nearest_supply"].bottom
        else:
            sig.target = sig.entry + cfg.TARGET_RR * (sig.entry - sig.stop_loss)

        # ── Scoring pass ──────────────────────────────────────────────────────

        # RSI
        if Indicators.is_rsi_oversold(sig.rsi):
            sig.add(W["rsi_oversold"], f"RSI oversold ({sig.rsi:.1f})")
        elif Indicators.is_rsi_recovering(df["rsi"]):
            sig.add(W["rsi_recovering"], f"RSI recovering ({sig.rsi:.1f})")

        # MACD
        if Indicators.is_macd_crossover(macd_df):
            sig.add(W["macd_crossover"], "MACD bullish crossover")
        elif sig.macd_hist > 0 and prev_row.get("macd_hist", 0) < sig.macd_hist:
            sig.add(W["macd_positive"], "MACD hist expanding positive")

        # EMA alignment
        if Indicators.is_ema_bullish_aligned(row):
            sig.add(W["ema_alignment"], "EMA stack bullish aligned")

        # ADX (strict): trend must be strong and rising
        if sig.adx >= cfg.ADX_STRONG_MIN and sig.adx_rising:
            sig.add(8, f"ADX strong+rising ({sig.adx:.1f})")
        elif 0 < sig.adx < cfg.ADX_STRONG_MIN:
            sig.subtract(8, f"ADX below {cfg.ADX_STRONG_MIN} ({sig.adx:.1f})")

        # Price vs VWAP (strict pullback, avoid chasing)
        vwap_val = float(row.get("vwap", sig.current_price))
        vwap_dist_pct = (
            abs(sig.current_price - vwap_val) / vwap_val * 100
            if vwap_val else 0.0
        )
        sig.vwap_pullback_ok = bool(sig.current_price >= vwap_val and vwap_dist_pct <= cfg.VWAP_PULLBACK_PCT)
        if sig.vwap_pullback_ok:
            sig.add(W["price_above_vwap"], f"VWAP pullback entry ({vwap_dist_pct:.2f}% from VWAP)")
        elif sig.current_price > vwap_val and vwap_dist_pct >= cfg.VWAP_CHASE_PCT:
            sig.subtract(6, f"Price extended {vwap_dist_pct:.2f}% above VWAP (chasing risk)")
        elif sig.current_price < vwap_val:
            sig.subtract(4, "Price below VWAP")

        # Bollinger band bounce
        if Indicators.is_near_bb_lower(row):
            sig.add(W["bollinger_bounce"], "Near BB lower band (bounce setup)")

        # Demand zone
        if self._ds.is_near_demand(ds):
            strength = ds["nearest_demand"].strength if ds["nearest_demand"] else 0
            extra = min(int(strength / 10), 5)
            sig.add(W["demand_zone_near"] + extra,
                    f"Near demand zone ({sig.demand_proximity:.1f}% away)")

        # Volume surge (strict): RVOL must clear high-conviction floor
        if sig.vol_ratio >= cfg.RVOL_HIGH_CONVICTION:
            sig.add(W["volume_surge"], f"RVOL strong ({sig.vol_ratio:.2f}x)")
        else:
            sig.subtract(6, f"RVOL below {cfg.RVOL_HIGH_CONVICTION:.1f}x ({sig.vol_ratio:.2f}x)")

        # Supertrend
        if Indicators.is_supertrend_bullish(row):
            sig.add(W["supertrend_bullish"], "Supertrend bullish")

        # Gap-up
        if sig.gap_pct >= cfg.GAP_UP_PCT:
            sig.add(W["gap_up"], f"Gap up {sig.gap_pct:.1f}%")

        # Pivot support bounce
        prev_close  = float(df["close"].iloc[-2])
        prev_high   = float(df["high"].iloc[-2])
        prev_low    = float(df["low"].iloc[-2])
        pivots = Indicators.pivot_points(prev_high, prev_low, prev_close)
        s1, s2 = pivots["s1"], pivots["s2"]
        close = sig.current_price
        if abs(close - s1) / close * 100 < 0.5 or abs(close - s2) / close * 100 < 0.5:
            sig.add(W["support_bounce"], "Near pivot support (S1/S2)")
            sr_msg = f"S/R: price near support (S1 {s1:.2f}, S2 {s2:.2f})"
        else:
            sr_msg = f"S/R: support S1 {s1:.2f}, resistance R1 {pivots['r1']:.2f}"

        # Previous day high breakout
        prev_day_high = float(df["high"].iloc[-2])
        if close > prev_day_high:
            sig.add(W["prev_day_high_break"], f"Breaking prev day high ({prev_day_high:.2f})")

        # Delivery %
        if delivery_pct >= 50:
            sig.add(W["delivery_pct_high"], f"High delivery {delivery_pct:.0f}%")

        # Buy-side pressure from order-book quantities
        if sig.buy_sell_ratio is not None:
            if sig.buy_sell_ratio >= 1.20:
                sig.add(W["buy_pressure"], f"Strong buy pressure (B/S {sig.buy_sell_ratio:.2f}x)")
            elif sig.buy_sell_ratio >= 1.05:
                sig.add(max(4, W["buy_pressure"] // 2),
                        f"Mild buy pressure (B/S {sig.buy_sell_ratio:.2f}x)")
            elif sig.buy_sell_ratio < 0.85:
                sig.subtract(8, f"Sell pressure (B/S {sig.buy_sell_ratio:.2f}x)")

        # Bullish candlestick bonus
        if sig.pattern in ("bullish_engulfing", "hammer"):
            sig.add(8, f"Bullish pattern: {sig.pattern}")

        if sig.pattern == "none":
            candle_msg = "Candlestick: no strong reversal pattern"
        else:
            candle_msg = f"Candlestick: detected {sig.pattern}"

        sig.indicator_messages = {
            "RSI": rsi_msg,
            "MACD": macd_msg,
            "Bollinger Bands": bb_msg,
            "EMA (9/21/50)": ema_msg,
            "Supertrend": st_msg,
            "VWAP": vwap_msg,
            "ATR": atr_msg,
            "Volume": vol_msg,
            "Stochastic": stoch_msg,
            "ADX": adx_msg,
            "Support/Resistance": sr_msg,
            "Candlestick": candle_msg,
        }

        # ── Negative adjustments ──────────────────────────────────────────────

        # RSI overbought — might be extended
        if sig.rsi > cfg.RSI_OVERBOUGHT:
            sig.subtract(12, f"RSI overbought ({sig.rsi:.1f})")

        # Bearish candle
        if sig.pattern in ("bearish_engulfing", "shooting_star"):
            sig.subtract(10, f"Bearish pattern: {sig.pattern}")

        # Inside supply zone
        if ds["in_supply_zone"]:
            sig.subtract(15, "Price inside supply zone")

        # Weak trend (ADX < 20)
        if 0 < sig.adx < 20:
            sig.subtract(5, f"Weak trend ADX ({sig.adx:.1f})")

        # Gap-down (bad omen for a morning buy)
        if sig.gap_pct < cfg.GAP_DOWN_PCT:
            sig.subtract(10, f"Gap down {sig.gap_pct:.1f}%")

        # Risk-reward gate (strict)
        if 0 < sig.reward_risk < cfg.RR_STRICT_MIN:
            sig.subtract(12, f"R:R below {cfg.RR_STRICT_MIN:.1f}x ({sig.reward_risk:.2f}x)")
        elif sig.reward_risk >= cfg.RR_STRICT_MIN:
            sig.add(6, f"R:R acceptable ({sig.reward_risk:.2f}x)")

        # ── PCR (optional) ────────────────────────────────────────────────────
        if pcr is not None:
            if pcr > 1.2:
                sig.add(5, f"Bullish PCR ({pcr:.2f})")
            elif pcr < 0.7:
                sig.subtract(5, f"Bearish PCR ({pcr:.2f})")

        # ── Lookback checks: 7d, 30d, ~3 months (90d) ───────────────────────
        close_series = df["close"]
        sig.chg_7d_pct = self._period_change(close_series, 7)
        sig.chg_30d_pct = self._period_change(close_series, 30)
        sig.chg_90d_pct = self._period_change(close_series, 90)

        if sig.chg_30d_pct is not None and sig.chg_30d_pct > 0:
            sig.add(4, f"30D trend positive ({sig.chg_30d_pct:.1f}%)")
        elif sig.chg_30d_pct is not None and sig.chg_30d_pct < 0:
            sig.subtract(6, f"30D trend weak ({sig.chg_30d_pct:.1f}%)")

        if sig.chg_90d_pct is not None and sig.chg_90d_pct > 0:
            sig.add(6, f"3M trend positive ({sig.chg_90d_pct:.1f}%)")
        elif sig.chg_90d_pct is not None and sig.chg_90d_pct < 0:
            sig.subtract(8, f"3M trend weak ({sig.chg_90d_pct:.1f}%)")

        if (
            sig.chg_30d_pct is not None and sig.chg_90d_pct is not None and
            sig.chg_30d_pct < 0 and sig.chg_90d_pct < 0
        ):
            sig.subtract(10, "Both 30D and 3M trends are negative")

        if sig.supertrend_dir != 1 and sig.chg_30d_pct is not None and sig.chg_90d_pct is not None:
            if sig.chg_30d_pct < 0 and sig.chg_90d_pct < 0:
                sig.subtract(8, "Supertrend bearish against 1M/3M trend")

        sig.indicator_messages["Lookback Trend (7/30/90d)"] = (
            f"{sig.chg_7d_pct:.2f}% / {sig.chg_30d_pct:.2f}% / {sig.chg_90d_pct:.2f}%"
            if None not in (sig.chg_7d_pct, sig.chg_30d_pct, sig.chg_90d_pct)
            else "Insufficient candles for full 7/30/90d trend"
        )

        sig.high_conviction = bool(
            sig.adx >= cfg.ADX_STRONG_MIN and
            sig.adx_rising and
            sig.vol_ratio >= cfg.RVOL_HIGH_CONVICTION and
            sig.reward_risk >= cfg.RR_STRICT_MIN and
            sig.vwap_pullback_ok
        )

        sig.buy_heading = self._build_buy_heading(sig)
        sig.buy_reason_summary = " | ".join(sig.reasons[:4])
        sig.caution_summary = " | ".join(sig.warnings[:2])

        return sig

    def rank(self, signals: list[StockSignal]) -> list[StockSignal]:
        """Sort by score descending, then filter by MIN_SCORE_TO_BUY."""
        return sorted(
            [s for s in signals if s.score >= cfg.MIN_SCORE_TO_BUY],
            key=lambda s: s.score,
            reverse=True,
        )
