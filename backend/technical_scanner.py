"""
technical_scanner.py
--------------------
Scans TSX/TSXV stocks under $5, $10, and $20 for short-term upside.
Uses TradingView Screener API for technical indicators and MGPR scoring.
"""

import os
import logging
from datetime import date
from tradingview_screener import Query, Column

logger = logging.getLogger(__name__)

# MGPR Scoring Thresholds
MIN_SCAN_SCORE = float(os.getenv("MIN_SCAN_SCORE") or "50")   # Minimum score to be considered for Airtable
MIN_VOLUME_SHARES = int(os.getenv("MIN_VOLUME_SHARES") or "50000")

def get_technical_signals(price_threshold: float = 20.0) -> list[dict]:
    """
    Query TradingView for Canada (TSX/TSXV) stocks under a price threshold.
    Calculates MGPR (Market Growth Potential Rating) for each.
    """
    logger.info(f"Scanning TSX/TSXV for stocks under ${price_threshold}...")
    
    try:
        q = (Query()
             .set_markets('canada')
             .select(
                 'name', 'description', 'close', 'high', 'low', 'open', 'volume',
                 'relative_volume_10d_calc', 'RSI', 'MACD.macd', 'MACD.signal',
                 'ATR', 'EMA20', 'EMA50', 'SMA200', 'market_cap_basic'
             )
             .where(
                 Column('close') > 0.1,
                 Column('close') <= price_threshold,
                 Column('volume') >= MIN_VOLUME_SHARES,
                 Column('type') == 'stock'
             )
             .limit(100))  # Top 100 results
        
        count, df = q.get_scanner_data()
        logger.info(f"Scan result type: {type(df)} | Count: {count}")
        
        if df is None or len(df) == 0:
            logger.info(f"No stocks found under ${price_threshold}")
            return []
            
        signals = []
        for _, row in df.iterrows():
            score_data = calculate_mgpr(row)
            if score_data['total_score'] >= MIN_SCAN_SCORE:
                signals.append(score_data)
                
        logger.info(f"Found {len(signals)} qualifying technical signals under ${price_threshold}")
        return sorted(signals, key=lambda x: x['total_score'], reverse=True)

    except Exception as e:
        logger.error(f"Technical scan failed: {e}")
        return []

def calculate_mgpr(row: dict) -> dict:
    """
    Calculate MGPR score (0-100) based on technical indicators.
    Returns a dict with scores and metadata.
    """
    ticker = row['ticker'].split(':')[-1]
    raw_exchange = row['ticker'].split(':')[0]
    # Map to Airtable singleSelect: [TSX|TSXV|NYSE|NASDAQ|AMEX]
    if 'XTSX' in raw_exchange or 'NEO' in raw_exchange or 'AEO' in raw_exchange:
        exchange = 'TSX'
    elif 'XTSV' in raw_exchange:
        exchange = 'TSXV'
    elif 'NYSE' in raw_exchange:
        exchange = 'NYSE'
    elif 'NASDAQ' in raw_exchange:
        exchange = 'NASDAQ'
    else:
        exchange = 'TSX' # Default fallback for Canada
    
    close = row['close']
    ema20 = row['EMA20']
    ema50 = row['EMA50']
    rsi = row['RSI']
    macd = row['MACD.macd']
    macd_signal = row['MACD.signal']
    atr = row['ATR']
    rel_vol = row['relative_volume_10d_calc']
    volume = row['volume']
    
    # ── 1. Trend Score (30 pts) ──────────────────────────────────────────
    trend_score = 0
    if close > ema20 > ema50:
        trend_score += 15
    if macd > macd_signal:
        trend_score += 15
        
    # ── 2. Momentum Score (30 pts) ───────────────────────────────────────
    momentum_score = 0
    if 50 <= rsi <= 75:
        momentum_score += 20
    elif rsi > 75:
        momentum_score += 10  # slightly overbought but strong
        
    if ema20 > ema50:
        momentum_score += 10
        
    # ── 3. Volatility Score (20 pts) ─────────────────────────────────────
    volatility_score = 0
    atr_pct = (atr / close * 100) if close > 0 else 0
    if 4 <= atr_pct <= 15:
        volatility_score = 20
    elif atr_pct > 15:
        volatility_score = 10  # high volatility, higher risk
        
    # ── 4. Volume Score (20 pts) ─────────────────────────────────────────
    volume_score = 0
    if rel_vol > 1.2:
        volume_score += 10
    if (volume * close) > 500000:
        volume_score += 10
        
    total_score = trend_score + momentum_score + volatility_score + volume_score
    
    # Dynamic Entry/SL/TP
    entry_price = close
    stop_loss = round(entry_price - (1.5 * atr), 2) if atr else round(entry_price * 0.9, 2)
    risk = entry_price - stop_loss
    take_profit = round(entry_price + (2.5 * risk), 2) if risk > 0 else round(entry_price * 1.2, 2)
    
    # Rationale
    rationale_parts = []
    if trend_score >= 30: rationale_parts.append("Strong multi-EMA trend with MACD confirmation.")
    if momentum_score >= 20: rationale_parts.append(f"Optimal momentum (RSI {rsi:.1f}).")
    if volume_score >= 10: rationale_parts.append(f"Increased relative volume ({rel_vol:.1f}x).")
    
    return {
        "ticker": ticker,
        "exchange": exchange,
        "company": row['description'],
        "total_score": total_score,
        "momentum_score": momentum_score,
        "trend_score": trend_score,
        "volatility_score": volatility_score,
        "volume_score": volume_score,
        "current_price": close,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "rsi": rsi,
        "macd_signal": "Bullish Cross" if macd > macd_signal else "Neutral",
        "atr_pct": round(atr_pct, 2),
        "high_52w": row.get('high_52w', 0), # placeholder if not in row
        "low_52w": row.get('low_52w', 0),   # placeholder if not in row
        "rationale": " ".join(rationale_parts) or "Neutral technical profile.",
        "scan_date": str(date.today())
    }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = get_technical_signals(price_threshold=5.0)
    for r in results[:5]:
        print(f"Ticker: {r['ticker']} | Score: {r['total_score']} | Price: {r['current_price']}")
