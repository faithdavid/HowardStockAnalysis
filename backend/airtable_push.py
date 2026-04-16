"""
airtable_push.py
----------------
Pushes scored insider signals to Airtable tables:
  - Raw Insider Data            : one row per trade scraped (all signals)
  - Filtered/Qualified Insider List : top signals above MIN_SCORE threshold
  - Market Pulls                : OHLCV / ATR market data per ticker
  - Alert History               : log of every pipeline run
  - Technical Scans             : MGPR-scored Canadian stocks (Under $5/$10/$20)
  - Historical/Backtest         : aggregate performance stats for strategy backtests
"""

import json
import logging
import os
import requests
from datetime import date, datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

AIRTABLE_TOKEN   = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Table names (exactly as they appear in Airtable)
TABLE_RAW       = "Raw Insider Data"
TABLE_QUALIFIED = "Filtered/Qualified Insider List"
TABLE_MARKET    = "Market Pulls"
TABLE_ALERTS    = "Alert History"
TABLE_TECH_5    = "Technical Scans Under $5 CAD"
TABLE_TECH_10   = "Technical Scans Under $10 CAD"
TABLE_TECH_20   = "Technical Scans Under $20 CAD"
TABLE_BACKTEST  = "Historical/Backtest"

# Only push to Qualified List if score >= this
MIN_QUALIFY_SCORE = 60

BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
HEADERS  = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}


def _post(table: str, fields: dict) -> dict:
    """POST a single record to an Airtable table. Returns the record dict."""
    url = f"{BASE_URL}/{requests.utils.quote(table, safe='')}"
    resp = requests.post(url, headers=HEADERS, json={"fields": fields}, timeout=15)
    if resp.status_code not in (200, 201):
        logger.error(f"Airtable error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    return resp.json()


# ── Raw Insider Data ──────────────────────────────────────────────────────────

def push_raw_signal(signal: dict) -> str:
    """
    Push one scored trade to 'Raw Insider Data'.
    Correctly maps to the actual Airtable field types:
      - Transaction Type  : singleSelect [Buy|Selll]
      - Processing Status : singleSelect [New|Processed|Archived]
      - Is Multiple Buy   : checkbox
    """
    tp = f"${signal['take_profit']}" if signal.get("take_profit") else "Hold to Close"

    notes_lines = [
        f"Score: {signal['total_score']}/100 | Rating: {signal['rating']} | Variant: {signal['variant']}",
        f"Rationale: {signal['rationale']}",
        f"Entry: ${signal['entry_price']} | Stop Loss: ${signal['stop_loss']} | TP: {tp}",
        f"ATR%: {signal['atr_pct']} | Vol $M: {signal['dollar_volume_m']} | 52W-High: ${signal['high_52w']}",
        f"Sub-scores → Strength: {signal['insider_strength']} | Volatility: {signal['volatility_score']} | Liquidity: {signal['liquidity_score']} | Timing: {signal['timing_score']}",
    ]
    if signal.get("spy_gap_note"):
        notes_lines.append(f"SPY: {signal['spy_gap_note']}")
    if signal.get("same_day_insiders", 1) > 1:
        notes_lines.append(f"Same-day insiders: {signal['same_day_insiders']}")

    trade_date = signal["trade_date"]
    if hasattr(trade_date, "isoformat"):
        trade_date = trade_date.isoformat()

    scan_date = signal.get("scan_date", str(date.today()))
    if hasattr(scan_date, "isoformat"):
        scan_date = scan_date.isoformat()

    fields = {
        "Ticker":             signal["ticker"],
        "Insider Name":       signal["insider_name"],
        "Insider Title":      signal["title"],
        "Trade Date":         trade_date,
        "Filing Date":        scan_date,
        "Shares Traded":      int(signal["shares"]),
        "Price Per Share":    float(signal["price_paid"]),
        "Value":              float(signal["total_value"]),
        "Is Multiple Buy":    bool(signal.get("same_day_insiders", 1) > 1),
        "Transaction Type":   "Buy",               # valid option in Airtable
        "Processing Status":  "New",               # valid option: New|Processed|Archived
        "Notes":              "\n".join(notes_lines),
    }

    record = _post(TABLE_RAW, fields)
    record_id = record.get("id", "unknown")
    logger.info(f"Pushed {signal['ticker']} → Raw Insider Data [{record_id}]")
    return record_id


# ── Filtered/Qualified Insider List ──────────────────────────────────────────

def push_qualified_signal(signal: dict, raw_record_id: str) -> str:
    """
    Push a high-scoring signal to 'Filtered/Qualified Insider List'.
    Only called for signals with score >= MIN_QUALIFY_SCORE.
    """
    tp = signal.get("take_profit") or None

    # Map variant to Airtable's singleSelect options
    variant_map = {
        "V1": "Variant 1 (Earnings)",
        "V2": "Variant 2 (Non-Earnings)",
    }
    season_variant = variant_map.get(signal["variant"], "N/A")

    # Map rating to Market Flag options: Strong Buy|Buy|Match|Caution
    score = signal["total_score"]
    if score >= 80:
        market_flag = "Strong Buy"
    elif score >= 65:
        market_flag = "Buy"
    elif score >= 50:
        market_flag = "Match"
    else:
        market_flag = "Caution"

    fields = {
        "Ticker":                 signal["ticker"],
        "Company Name":           signal["company"],
        "Rating":                 float(signal["total_score"]),
        "Date Qualified":         str(date.today()),
        "Entry Price":            float(signal["entry_price"]),
        "Stop Loss":              float(signal["stop_loss"]),
        "Season Variant":         season_variant,
        "Gap Note":               signal.get("spy_gap_note", ""),
        "Reasons":                signal["rationale"],
        "Insider Strength Score": float(signal["insider_strength"]),
        "Volatility Score":       float(signal["volatility_score"]),
        "Liquidity Score":        float(signal["liquidity_score"]),
        "Timing Score":           float(signal["timing_score"]),
        "Market Flag":            market_flag,
        "Alert Enabled":          True,
        "Insider Data":           [raw_record_id],   # link back to Raw Insider Data
    }
    if tp:
        fields["Take Profit"] = float(tp)

    record = _post(TABLE_QUALIFIED, fields)
    record_id = record.get("id", "unknown")
    logger.info(f"Pushed {signal['ticker']} → Filtered/Qualified Insider List [{record_id}]")
    return record_id


# ── Market Pulls ──────────────────────────────────────────────────────────────

def push_market_pull(signal: dict) -> str | None:
    """
    Push market data snapshot to 'Market Pulls'.
    This records the OHLCV / ATR at the time of the scan.
    """
    try:
        # Determine currency type from ticker suffix
        currency = "CAD" if signal["ticker"].endswith(".TO") or signal["ticker"].endswith(".V") else "USD"

        fields = {
            "Ticker":      signal["ticker"],
            "Date":        str(date.today()),
            "Close":       float(signal["entry_price"]),    # last close price
            "ATR":         float(signal.get("atr_pct", 0)),
            "Data Source": "Python Script",
            "Currency Type": currency,
        }
        record = _post(TABLE_MARKET, fields)
        record_id = record.get("id", "unknown")
        logger.info(f"Pushed {signal['ticker']} → Market Pulls [{record_id}]")
        return record_id
    except Exception as e:
        logger.warning(f"Market pull push failed for {signal.get('ticker')}: {e}")
        return None


# ── Alert History ─────────────────────────────────────────────────────────────

def log_alert(signals: list[dict], raw_record_ids: list[str], status: str = "Sent"):
    """Log a sent alert to the Alert History table."""
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        return
    try:
        tickers = ", ".join([s["ticker"] for s in signals[:10]])
        fields = {
            "Alert ID":   f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
            "Alert Type": "Slack",
            "Recipients": os.getenv("ALERT_EMAIL_TO", ""),
            "Status":     status,   # Sent|Failed|Pending
        }
        if raw_record_ids:
            fields["Stocks Included"] = raw_record_ids[:10]  # link to raw records
        _post(TABLE_ALERTS, fields)
        logger.info(f"Alert logged → Alert History ({status})")
    except Exception as e:
        logger.debug(f"Alert history logging skipped: {e}")

# ── Technical Scans ──────────────────────────────────────────────────────────

def push_technical_signal(signal: dict) -> str:
    """
    Push one technical scan result to the appropriate table ($5, $10, or $20).
    """
    price = signal["current_price"]
    if price <= 5.0:
        table = TABLE_TECH_5
    elif price <= 10.0:
        table = TABLE_TECH_10
    else:
        table = TABLE_TECH_20

    fields = {
        "Ticker":           signal["ticker"],
        "Company Name":     signal["company"],
        "MGPR score":       float(signal["total_score"]),
        "Exchange":         signal["exchange"],
        "Description":      signal["company"],
        "Scan Date":        signal["scan_date"],
        "Current Price":    float(signal["current_price"]),
        "Entry Price":      float(signal["entry_price"]),
        "Stop Loss":        float(signal["stop_loss"]),
        "Take Profit":      float(signal["take_profit"]),
        "RSI":              float(signal["rsi"]),
        "MACD Signal":      signal["macd_signal"],
        "Rationale":        signal["rationale"],
        "Momentum Score":   float(signal["momentum_score"]),
        "Volatility Score": float(signal["volatility_score"]),
        "Volume Score":     float(signal["volume_score"]),
        "MGPR Breakdown":   (f"Momentum: {signal['momentum_score']} | Trend: {signal['trend_score']} | "
                             f"Volatility: {signal['volatility_score']} | Volume: {signal['volume_score']}"),
        "Alert Enabled":    True
    }

    record = _post(table, fields)
    record_id = record.get("id", "unknown")
    logger.info(f"Pushed {signal['ticker']} → {table} [{record_id}]")
    return record_id

def push_all_tech_signals(signals: list[dict]) -> list[str]:
    """Push all technical signals and return record IDs."""
    ids = []
    for s in signals:
        try:
            rid = push_technical_signal(s)
            ids.append(rid)
        except Exception as e:
            logger.error(f"Failed to push tech signal {s.get('ticker')}: {e}")
    return ids


# ── Orchestrators ─────────────────────────────────────────────────────────────

def push_all_signals(signals: list[dict]) -> list[str]:
    """
    Full push pipeline:
      1. Every signal → Raw Insider Data (Processing Status = New)
      2. Signals with score >= MIN_QUALIFY_SCORE → Filtered/Qualified Insider List
      3. Market data → Market Pulls
    Returns list of Raw Insider Data record IDs.
    """
    raw_ids = []

    for signal in signals:
        raw_id = None

        # Step 1: Raw Insider Data (always)
        try:
            raw_id = push_raw_signal(signal)
            raw_ids.append(raw_id)
        except Exception as e:
            logger.error(f"Failed to push raw signal for {signal.get('ticker')}: {e}")
            continue

        # Step 2: Qualified List (high-scoring only)
        if signal["total_score"] >= MIN_QUALIFY_SCORE:
            try:
                push_qualified_signal(signal, raw_id)
            except Exception as e:
                logger.warning(f"Qualified push failed for {signal.get('ticker')}: {e}")

        # Step 3: Market Pulls
        try:
            push_market_pull(signal)
        except Exception as e:
            logger.warning(f"Market pull push failed for {signal.get('ticker')}: {e}")

    logger.info(f"Pushed {len(raw_ids)} records to Airtable")
    return raw_ids


# Keep legacy name so main.py doesn't break
def log_run(status: str, message: str, signals_count: int = 0):
    """Legacy wrapper — silently skips."""
    logger.debug(f"log_run called: {status} | {message} | {signals_count} signals")


# ── Historical / Backtest ─────────────────────────────────────────────────────

def push_backtest_result(metrics: dict) -> str:
    """
    Push a single backtest summary to the 'Historical/Backtest' table.

    Expected keys in `metrics`:
      Required:
        test_name       (str)  – e.g. "Insider Run – Q1 2025"
        module          (str)  – e.g. "Insider" | "Technical_Under_5" | ...
        date_range_start (str) – ISO date, e.g. "2025-01-01"
        date_range_end   (str) – ISO date, e.g. "2025-03-31"
        total_trades    (int)
        win_rate        (float) – percentage, e.g. 62.5
        average_return  (float) – percentage per trade, e.g. 4.2
        total_return    (float) – cumulative %, e.g. 38.0

      Optional:
        sharpe_ratio         (float)
        max_drawdown         (float) – percentage, e.g. -12.3
        profit_factor        (float)
        random_control_win   (float) – random baseline win rate %
        edge_vs_random       (float) – percentage point edge over random
        edge_metrics         (str)   – free-text or JSON summary
        control_comparison   (str)   – free-text comparison narrative
        configuration_snapshot (str) – JSON or TOML string of settings used
        notes                (str)
        status               (str)   – e.g. "Draft" | "Complete" | "Archived"
        tested_by            (str)   – your name or "AutoPipeline"

    Returns:
        The Airtable record ID of the newly created backtest record.
    """
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    fields = {
        "Test Name":    metrics["test_name"],
        "Run Date":     run_date,
        "Module":       metrics["module"],
        "Date Range Start": metrics["date_range_start"],
        "Date Range End":   metrics["date_range_end"],
        "Total Trades": int(metrics["total_trades"]),
        "Win Rate":     float(metrics["win_rate"]) / 100.0, # Percent field (decimal)
        "Average Return": float(metrics["average_return"]),  # Number field
        "Total Return": float(metrics["total_return"]),      # Number field
    }

    # Optional fields — only included if present
    optional_floats = {
        "sharpe_ratio":       "Sharpe Ratio",
        "max_drawdown":       "Max Drawdown",
        "profit_factor":      "Profit Factor",
        "random_control_win": "Random Control Win",
    }
    for key, col in optional_floats.items():
        if metrics.get(key) is not None:
            fields[col] = float(metrics[key])

    optional_strings = {
        "edge_metrics":           "Edge Metrics",
        "control_comparison":     "Control Comparison",
        "configuration_snapshot": "Configuration Snapshot",
    }
    for key, col in optional_strings.items():
        if metrics.get(key):
            fields[col] = str(metrics[key])

    # Handle Simulation Results + Notes combination
    sim_log = metrics.get("simulation_results", "")
    notes = metrics.get("notes", "")
    if sim_log:
        if notes:
            fields["Notes"] = f"{notes}\n\n---\n{sim_log}"
        else:
            fields["Notes"] = sim_log
    elif notes:
         fields["Notes"] = notes

    record = _post(TABLE_BACKTEST, fields)
    record_id = record.get("id", "unknown")
    logger.info(
        f"Backtest result logged → Historical/Backtest [{record_id}] "
        f"({metrics['module']} | {metrics['date_range_start']} → {metrics['date_range_end']})"
    )
    return record_id
