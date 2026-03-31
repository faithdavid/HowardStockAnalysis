"""
market_data.py
--------------
Fetches ATR%, daily dollar volume, and 52-week high for a ticker.
Uses Polygon.io as primary source (better TSX coverage than yfinance).
Falls back to yfinance if Polygon key is not set or call fails.
"""

import logging
import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_BASE    = "https://api.polygon.io"

# Polygon free plan: 5 requests/minute → 12s between calls
_POLYGON_MIN_INTERVAL = 12.0
_last_polygon_call: float = 0.0


def get_market_data(ticker: str) -> dict | None:
    """
    Fetch market data for a ticker and return:
      - last_close       : most recent closing price
      - atr_pct          : 14-day ATR as % of close
      - dollar_volume_m  : last day's dollar volume in $M
      - high_52w         : 52-week high

    Tries Polygon first, falls back to yfinance.
    Returns None if both fail.
    """
    if POLYGON_API_KEY:
        result = _get_from_polygon(ticker)
        if result:
            return result
        logger.warning(f"Polygon failed for {ticker}, trying yfinance fallback...")

    return _get_from_yfinance(ticker)


def _get_from_polygon(ticker: str) -> dict | None:
    """
    Fetch 90 days of daily bars from Polygon.
    TSX tickers: Polygon uses "X:SU" format for Canadian stocks.
    Strip the .TO / .V suffix before calling.
    """
    global _last_polygon_call
    elapsed = time.time() - _last_polygon_call
    if elapsed < _POLYGON_MIN_INTERVAL:
        wait = _POLYGON_MIN_INTERVAL - elapsed
        logger.debug(f"Polygon rate limiter: sleeping {wait:.1f}s")
        time.sleep(wait)
    _last_polygon_call = time.time()

    try:
        # Strip exchange suffix for Polygon (SU.TO -> SU, SHOP.TO -> SHOP)
        polygon_ticker = ticker.replace(".TO", "").replace(".V", "")

        to_date   = date.today()
        from_date = to_date - timedelta(days=90)

        url = (
            f"{POLYGON_BASE}/v2/aggs/ticker/{polygon_ticker}/range/1/day"
            f"/{from_date}/{to_date}"
            f"?adjusted=true&sort=asc&limit=120&apiKey={POLYGON_API_KEY}"
        )

        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if len(results) < 15:
            logger.warning(f"Polygon: not enough bars for {ticker} ({len(results)} bars)")
            return None

        closes  = [r["c"] for r in results]
        highs   = [r["h"] for r in results]
        volumes = [r["v"] for r in results]

        # 14-day ATR
        recent = results[-15:]
        tr_list = []
        for i in range(1, len(recent)):
            high       = recent[i]["h"]
            low        = recent[i]["l"]
            prev_close = recent[i - 1]["c"]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)

        atr        = sum(tr_list) / len(tr_list)
        last_close = closes[-1]
        atr_pct    = (atr / last_close) * 100 if last_close > 0 else 0

        dollar_volume_m = (last_close * volumes[-1]) / 1_000_000
        high_52w        = max(highs)

        logger.info(f"Polygon OK {ticker}: close=${last_close}, ATR={atr_pct:.1f}%, vol=${dollar_volume_m:.1f}M")

        return {
            "ticker":          ticker,
            "last_close":      round(last_close, 2),
            "atr_pct":         round(atr_pct, 2),
            "dollar_volume_m": round(dollar_volume_m, 1),
            "high_52w":        round(high_52w, 2),
        }

    except requests.HTTPError as e:
        logger.warning(f"Polygon HTTP error for {ticker}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Polygon failed for {ticker}: {e}")
        return None


def _get_from_yfinance(ticker: str) -> dict | None:
    """Fallback: fetch from yfinance (free, no API key needed)."""
    try:
        import yfinance as yf  # noqa: F401 — may fail if multitasking not built
        data = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=True)

        if data.empty or len(data) < 15:
            logger.warning(f"yfinance: not enough data for {ticker}")
            return None

        closes_series = data["Close"]
        highs_series = data["High"]
        lows_series = data["Low"]
        volume_series = data["Volume"]

        # yfinance may return MultiIndex columns for a single ticker (e.g., Close/GWRS)
        if hasattr(closes_series, "columns") and not closes_series.empty:
            closes_series = closes_series.iloc[:, 0]
            highs_series = highs_series.iloc[:, 0]
            lows_series = lows_series.iloc[:, 0]
            volume_series = volume_series.iloc[:, 0]

        closes  = closes_series.dropna().tolist()
        highs   = highs_series.dropna().tolist()
        volumes = volume_series.dropna().tolist()

        recent = pd.DataFrame({
            "High": highs_series,
            "Low": lows_series,
            "Close": closes_series,
        }).dropna().tail(15)

        tr_list = []
        for i in range(1, len(recent)):
            high       = float(recent["High"].iloc[i])
            low        = float(recent["Low"].iloc[i])
            prev_close = float(recent["Close"].iloc[i - 1])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)

        atr        = sum(tr_list) / len(tr_list) if tr_list else 0
        last_close = float(closes[-1])
        atr_pct    = (atr / last_close) * 100 if last_close > 0 else 0

        dollar_volume_m = (last_close * float(volumes[-1])) / 1_000_000
        high_52w        = float(max(highs))

        logger.info(f"yfinance OK {ticker}: close=${last_close}, ATR={atr_pct:.1f}%, vol=${dollar_volume_m:.1f}M")

        return {
            "ticker":          ticker,
            "last_close":      round(last_close, 2),
            "atr_pct":         round(atr_pct, 2),
            "dollar_volume_m": round(dollar_volume_m, 1),
            "high_52w":        round(high_52w, 2),
        }

    except Exception as e:
        logger.warning(f"yfinance failed for {ticker}: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for t in ["AAPL", "MSFT"]:
        print(get_market_data(t))