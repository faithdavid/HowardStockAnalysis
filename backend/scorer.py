import logging
from datetime import date, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

import os

# Variant 1: earnings season only (Feb, May, Aug, Nov)
#   ATR >= 3.5%, volume $30M-$100M
#   SL = 50% ATR, TP = 100% ATR
EARNINGS_MONTHS = {2, 5, 8, 11}
V1_ATR_MIN = float(os.getenv("V1_ATR_MIN") or "3.5")
V1_VOL_MIN_M = float(os.getenv("V1_VOL_MIN_M") or "30")    # $30M
V1_VOL_MAX_M = float(os.getenv("V1_VOL_MAX_M") or "100")   # $100M

# Variant 2: year-round
#   ATR 7%-20%, volume $30M-$10B
#   SL = 150% ATR, TP = None (hold to close)
V2_ATR_MIN = float(os.getenv("V2_ATR_MIN") or "7.0")
V2_ATR_MAX = float(os.getenv("V2_ATR_MAX") or "20.0")
V2_VOL_MIN_M = float(os.getenv("V2_VOL_MIN_M") or "30")
V2_VOL_MAX_M = float(os.getenv("V2_VOL_MAX_M") or "10000")  # $10B

# Repeat buy window (ignore if same insider bought within 30 days)
REPEAT_BUY_DAYS = int(os.getenv("REPEAT_BUY_DAYS") or "30")

# SPY gap threshold — if SPY gap > 0.5% up or down, mark as caution
SPY_GAP_THRESHOLD = 0.5

# Senior title keywords (higher weight)
SENIOR_TITLES = ["ceo", "cfo", "coo", "president", "chairman", "director", "officer"]


def is_earnings_season() -> bool:
    return date.today().month in EARNINGS_MONTHS


def detect_repeat_buys(trades: list[dict]) -> set[str]:
    """
    Returns a set of insider names who have bought within the last 30 days
    more than once. We flag these as repeat buyers to reduce their score.
    """
    # Group by (ticker, insider_name)
    buy_history = defaultdict(list)
    for t in trades:
        key = (t["ticker"], t["insider_name"])
        buy_history[key].append(t["trade_date"])

    repeat_keys = set()
    for key, dates in buy_history.items():
        if len(dates) > 1:
            dates_sorted = sorted(dates)
            for i in range(1, len(dates_sorted)):
                gap = (dates_sorted[i] - dates_sorted[i - 1]).days
                if gap <= REPEAT_BUY_DAYS:
                    repeat_keys.add(key)
    return repeat_keys


def count_same_day_insiders(trades: list[dict]) -> dict[str, int]:
    """
    Returns a dict: ticker -> count of unique insiders buying on the same day.
    Used to boost score when multiple insiders buy the same stock same day.
    """
    counts = defaultdict(lambda: defaultdict(set))
    for t in trades:
        counts[t["ticker"]][t["trade_date"]].add(t["insider_name"])

    result = {}
    for ticker, date_map in counts.items():
        max_same_day = max(len(names) for names in date_map.values())
        result[ticker] = max_same_day
    return result


def determine_variant(atr_pct: float, dollar_volume_m: float) -> str | None:
    """
    Returns 'V1', 'V2', or None if the trade doesn't qualify for either variant.
    Variant 1 takes priority during earnings season.
    """
    in_earnings = is_earnings_season()

    v1_ok = (
        atr_pct >= V1_ATR_MIN
        and V1_VOL_MIN_M <= dollar_volume_m <= V1_VOL_MAX_M
        and in_earnings
    )
    v2_ok = (
        V2_ATR_MIN <= atr_pct <= V2_ATR_MAX
        and V2_VOL_MIN_M <= dollar_volume_m <= V2_VOL_MAX_M
    )

    if v1_ok:
        return "V1"
    if v2_ok:
        return "V2"
    return None


def score_trade(
    trade: dict,
    market: dict,
    is_repeat: bool,
    same_day_count: int,
    spy_gap_pct: float = 0.0,
) -> dict | None:
    """
    Score a single insider trade.
    Returns an enriched dict with score + SL/TP, or None if it doesn't qualify.

    trade    : from scraper.py
    market   : from market_data.py
    is_repeat: True if this insider bought this ticker recently
    same_day_count: how many insiders bought this ticker today
    spy_gap_pct: today's SPY open gap %
    """
    atr_pct = market["atr_pct"]
    dollar_volume_m = market["dollar_volume_m"]
    last_close = market["last_close"]

    # ── Determine which variant applies ─────────────────────────────────────
    variant = determine_variant(atr_pct, dollar_volume_m)
    if variant is None:
        logger.debug(f"{trade['ticker']} doesn't qualify for V1 or V2 — skipping")
        return None

    # ── 1. Insider Strength (0-25) ───────────────────────────────────────────
    # Base: transaction value
    value_m = trade["value"] / 1_000_000
    strength = min(value_m / 2.0, 1.0)  # $2M+ = full score

    # Senior title boost
    title_lower = trade["title"].lower()
    if any(t in title_lower for t in SENIOR_TITLES):
        strength = min(strength + 0.2, 1.0)

    # Penalise repeat buys
    if is_repeat:
        strength *= 0.5

    # Multiple insiders buying same day boost
    if same_day_count >= 3:
        strength = min(strength + 0.3, 1.0)
    elif same_day_count == 2:
        strength = min(strength + 0.15, 1.0)

    insider_strength_score = round(strength * 25, 1)

    # ── 2. Volatility Match (0-25) ───────────────────────────────────────────
    if variant == "V1":
        # V1: want ATR >= 3.5% — more the better up to ~8%
        vol_score = min((atr_pct - V1_ATR_MIN) / 4.5, 1.0) if atr_pct >= V1_ATR_MIN else 0.4
    else:
        # V2: sweet spot is 7-20%, penalise extremes
        if V2_ATR_MIN <= atr_pct <= 15:
            vol_score = 1.0
        elif atr_pct <= 20:
            vol_score = 0.7
        else:
            vol_score = 0.3

    volatility_score = round(vol_score * 25, 1)

    # ── 3. Liquidity Score (0-25) ────────────────────────────────────────────
    if variant == "V1":
        # Sweet spot $30M-$100M
        if V1_VOL_MIN_M <= dollar_volume_m <= V1_VOL_MAX_M:
            liq = (dollar_volume_m - V1_VOL_MIN_M) / (V1_VOL_MAX_M - V1_VOL_MIN_M)
            liq_score = 0.5 + liq * 0.5  # 0.5 to 1.0
        else:
            liq_score = 0.2
    else:
        # V2: $30M-$10B, higher is better up to $1B
        if dollar_volume_m >= V2_VOL_MIN_M:
            liq_score = min(dollar_volume_m / 1000, 1.0)
        else:
            liq_score = 0.2

    liquidity_score = round(liq_score * 25, 1)

    # ── 4. Timing Score (0-25) ───────────────────────────────────────────────
    timing = 1.0

    # Earnings season bonus for V1
    if variant == "V1" and is_earnings_season():
        timing = min(timing + 0.2, 1.0)

    # Penalise repeat buys
    if is_repeat:
        timing *= 0.6

    # SPY gap caution
    spy_gap_note = ""
    if abs(spy_gap_pct) > SPY_GAP_THRESHOLD:
        timing *= 0.8
        spy_gap_note = f"SPY gap {spy_gap_pct:+.1f}% — caution"

    # Recency: trade_date close to today
    days_ago = (date.today() - trade["trade_date"]).days
    recency = max(1.0 - (days_ago / 5.0), 0.5)
    timing *= recency

    timing_score = round(timing * 25, 1)

    # ── Total score ──────────────────────────────────────────────────────────
    total = insider_strength_score + volatility_score + liquidity_score + timing_score

    # ── Rating label ─────────────────────────────────────────────────────────
    if total >= 80:
        rating = "Excellent"
    elif total >= 60:
        rating = "Good"
    elif total >= 40:
        rating = "Fair"
    else:
        rating = "Weak"

    # ── SL / TP based on variant ─────────────────────────────────────────────
    atr_dollar = last_close * (atr_pct / 100)
    entry = last_close  # buy at open next day; use close as estimate

    if variant == "V1":
        stop_loss = round(entry - 0.5 * atr_dollar, 2)
        take_profit = round(entry + 1.0 * atr_dollar, 2)
    else:  # V2
        stop_loss = round(entry - 1.5 * atr_dollar, 2)
        take_profit = None  # hold to close

    # ── Human-readable rationale ─────────────────────────────────────────────
    reasons = []
    if same_day_count >= 2:
        reasons.append(f"{same_day_count} insiders bought same day")
    if is_earnings_season() and variant == "V1":
        reasons.append("Earnings season window")
    if not is_repeat:
        reasons.append("First-time buy (no repeat in 30 days)")
    if strength > 0.7:
        reasons.append(f"Strong insider ({trade['title']})")
    if spy_gap_note:
        reasons.append(spy_gap_note)
    if not reasons:
        reasons.append("Insider accumulation signal")

    return {
        # Identity
        "ticker": trade["ticker"],
        "company": trade["company"],
        "insider_name": trade["insider_name"],
        "title": trade["title"],
        "trade_date": str(trade["trade_date"]),
        "scan_date": str(date.today()),
        # Market data
        "last_close": last_close,
        "atr_pct": atr_pct,
        "dollar_volume_m": dollar_volume_m,
        "high_52w": market["high_52w"],
        # Trade details
        "shares": int(trade["shares"]),
        "price_paid": trade["price"],
        "total_value": trade["value"],
        # Strategy
        "variant": variant,
        "entry_price": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        # Scores
        "insider_strength": insider_strength_score,
        "volatility_score": volatility_score,
        "liquidity_score": liquidity_score,
        "timing_score": timing_score,
        "total_score": round(total, 1),
        "rating": rating,
        "rationale": " | ".join(reasons),
        "same_day_insiders": same_day_count,
        "is_repeat_buy": is_repeat,
        "spy_gap_note": spy_gap_note,
        # Disclaimer
        "disclaimer": "NOT financial advice. For research only. Past performance ≠ future results.",
    }


if __name__ == "__main__":
    # Quick test
    trade = {
        "ticker": "TEST",
        "company": "Test Corp",
        "insider_name": "John Smith",
        "title": "CEO",
        "trade_date": date.today(),
        "shares": 10000,
        "price": 5.50,
        "value": 55000,
    }
    market = {
        "ticker": "TEST",
        "last_close": 5.50,
        "atr_pct": 4.2,
        "dollar_volume_m": 45.0,
        "high_52w": 8.00,
    }
    result = score_trade(trade, market, is_repeat=False, same_day_count=1)
    print(result)
