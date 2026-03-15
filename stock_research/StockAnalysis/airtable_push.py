"""
airtable_push.py
----------------
Pushes scored insider signals to Airtable.
Creates two tables:
  - InsiderSignals  : one row per scored signal
  - PipelineRuns    : one row per pipeline execution (for logging)
"""

import logging
import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

AIRTABLE_TOKEN   = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_INSIDER    = os.getenv("AIRTABLE_TABLE_INSIDER", "InsiderSignals")
TABLE_RUNS       = os.getenv("AIRTABLE_TABLE_RUNS", "PipelineRuns")

BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
HEADERS  = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}


def _post(table: str, fields: dict) -> dict:
    """POST a single record to an Airtable table."""
    url = f"{BASE_URL}/{table}"
    resp = requests.post(url, headers=HEADERS, json={"fields": fields}, timeout=15)
    if resp.status_code not in (200, 201):
        logger.error(f"Airtable error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    return resp.json()


def push_signal(signal: dict) -> str:
    """
    Push one insider signal to the InsiderSignals table.
    Returns the Airtable record ID.
    """
    fields = {
        "Ticker":             signal["ticker"],
        "Company":            signal["company"],
        "Insider Name":       signal["insider_name"],
        "Title":              signal["title"],
        "Trade Date":         signal["trade_date"],
        "Scan Date":          signal["scan_date"],
        "Shares":             signal["shares"],
        "Price Paid":         signal["price_paid"],
        "Total Value ($)":    signal["total_value"],
        "Last Close":         signal["last_close"],
        "ATR %":              signal["atr_pct"],
        "Dollar Volume ($M)": signal["dollar_volume_m"],
        "52W High":           signal["high_52w"],
        "Variant":            signal["variant"],
        "Entry Price":        signal["entry_price"],
        "Stop Loss":          signal["stop_loss"],
        "Take Profit":        signal["take_profit"] if signal["take_profit"] else "Hold to Close",
        "Insider Strength":   signal["insider_strength"],
        "Volatility Score":   signal["volatility_score"],
        "Liquidity Score":    signal["liquidity_score"],
        "Timing Score":       signal["timing_score"],
        "Total Score":        signal["total_score"],
        "Rating":             signal["rating"],
        "Rationale":          signal["rationale"],
        "Same-Day Insiders":  signal["same_day_insiders"],
        "Repeat Buy":         signal["is_repeat_buy"],
        "SPY Gap Note":       signal["spy_gap_note"],
        "Disclaimer":         signal["disclaimer"],
    }
    record = _post(TABLE_INSIDER, fields)
    record_id = record.get("id", "unknown")
    logger.info(f"Pushed {signal['ticker']} to Airtable → {record_id}")
    return record_id


def push_all_signals(signals: list[dict]) -> list[str]:
    """Push all signals and return list of record IDs."""
    ids = []
    for signal in signals:
        try:
            record_id = push_signal(signal)
            ids.append(record_id)
        except Exception as e:
            logger.error(f"Failed to push {signal.get('ticker')}: {e}")
    return ids


def log_run(status: str, message: str, signals_count: int = 0):
    """Log pipeline execution to PipelineRuns table."""
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        return
    try:
        _post(TABLE_RUNS, {
            "Run Time":       datetime.now(timezone.utc).isoformat(),
            "Status":         status,
            "Message":        message,
            "Signals Found":  signals_count,
        })
    except Exception as e:
        logger.warning(f"Could not log run to Airtable: {e}")