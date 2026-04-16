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
    """
    if POLYGON_API_KEY:
        result = _get_from_polygon(ticker)
        if result:
            return result
        logger.warning(f"Polygon failed for {ticker}, trying yfinance fallback...")

    return _get_from_yfinance(ticker)


def _get_from_polygon(ticker: str) -> dict | None:
    global _last_polygon_call
    elapsed = time.time() - _last_polygon_call
    if elapsed < _POLYGON_MIN_INTERVAL:
        wait = _POLYGON_MIN_INTERVAL - elapsed
        time.sleep(wait)
    _last_polygon_call = time.time()

    try:
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
        if len(results) < 15: return None

        closes  = [r["c"] for r in results]
        highs   = [r["h"] for r in results]
        volumes = [r["v"] for r in results]

        recent = results[-15:]
        tr_list = []
        for i in range(1, len(recent)):
            h, l, pc = recent[i]["h"], recent[i]["l"], recent[i-1]["c"]
            tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))

        atr        = sum(tr_list) / len(tr_list)
        last_close = closes[-1]
        atr_pct    = (atr / last_close) * 100
        dollar_volume_m = (last_close * volumes[-1]) / 1_000_000
        high_52w        = max(highs)

        return {
            "ticker":          ticker,
            "last_close":      round(last_close, 2),
            "atr_pct":         round(atr_pct, 2),
            "dollar_volume_m": round(dollar_volume_m, 1),
            "high_52w":        round(high_52w, 2),
        }
    except Exception as e:
        logger.warning(f"Polygon failed: {e}")
        return None


def _get_from_yfinance(ticker: str) -> dict | None:
    try:
        import yfinance as yf
        data = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=True)
        if data.empty or len(data) < 15: return None

        if hasattr(data.columns, "levels"): data.columns = data.columns.droplevel(1)
        
        last_close = float(data["Close"].iloc[-1])
        recent = data.tail(15)
        tr_list = []
        for i in range(1, len(recent)):
            h, l, pc = float(recent["High"].iloc[i]), float(recent["Low"].iloc[i]), float(recent["Close"].iloc[i-1])
            tr_list.append(max(h-l, abs(h-pc), abs(l-pc)))
        
        atr = sum(tr_list) / len(tr_list)
        atr_pct = (atr / last_close) * 100
        dollar_volume_m = (last_close * float(data["Volume"].iloc[-1])) / 1_000_000
        high_52w = float(data["High"].max())

        return {
            "ticker": ticker,
            "last_close": round(last_close, 2),
            "atr_pct": round(atr_pct, 2),
            "dollar_volume_m": round(dollar_volume_m, 1),
            "high_52w": round(high_52w, 2),
        }
    except Exception as e:
        logger.warning(f"yfinance failed: {e}")
        return None


def get_spy_gap(on_date: date | None = None) -> float:
    """
    Returns the SPY gap percentage: ((Open - Prev Close) / Prev Close) * 100.
    """
    import yfinance as yf
    try:
        ticker = "SPY"
        if on_date:
            start = on_date - timedelta(days=7)
            end   = on_date + timedelta(days=2)
            df = yf.download(ticker, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), progress=False)
            if df.empty: return 0.0
            
            on_date_str = on_date.strftime("%Y-%m-%d")
            found_idx = -1
            for i, dt in enumerate(df.index):
                if dt.strftime("%Y-%m-%d") == on_date_str:
                    found_idx = i
                    break
            
            if found_idx <= 0: return 0.0
            prev_close_val = df.iloc[found_idx - 1]["Close"]
            open_price_val = df.iloc[found_idx]["Open"]
            prev_close = float(prev_close_val.iloc[0]) if hasattr(prev_close_val, 'iloc') else float(prev_close_val)
            open_price = float(open_price_val.iloc[0]) if hasattr(open_price_val, 'iloc') else float(open_price_val)
        else:
            df = yf.download(ticker, period="5d", progress=False)
            if df.empty or len(df) < 2: return 0.0
            prev_close_val = df.iloc[-2]["Close"]
            open_price_val = df.iloc[-1]["Open"]
            prev_close = float(prev_close_val.iloc[0]) if hasattr(prev_close_val, 'iloc') else float(prev_close_val)
            open_price = float(open_price_val.iloc[0]) if hasattr(open_price_val, 'iloc') else float(open_price_val)

        gap = ((open_price - prev_close) / prev_close) * 100
        logger.info(f"SPY Gap OK: {gap:.2f}%")
        return gap
    except Exception as e:
        logger.error(f"SPY Gap Error: {e}")
        return 0.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(get_spy_gap())
