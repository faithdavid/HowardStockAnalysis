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
TABLE_RUNS       = os.getenv("AIRTABLE_TABLE_RUNS", "Alert History")

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
    Push one insider signal to the Raw Insider Data table.
    Returns the Airtable record ID.

    Column mapping (Airtable field → pipeline key):
      Ticker            ← ticker
      Insider Name      ← insider_name
      Insider Title     ← title
      Trade Date        ← trade_date
      Filing Date       ← scan_date
      Shares Traded     ← shares
      Price Per Share   ← price_paid
      Value             ← total_value
      Is Multiple Buy   ← is_repeat_buy  (checkbox)
      Buy Type          ← variant  (V1 / V2)
      Transaction Type  ← "Buy"
      Processing Status ← scoring summary  (Score + Rating)
      Notes             ← full rationale + entry/stop/TP + SPY note
    """
    tp = signal["take_profit"] if signal["take_profit"] else "Hold to Close"
    notes_parts = [
        f"Score: {signal['total_score']}/100 ({signal['rating']}) | Variant: {signal['variant']}",
        f"Rationale: {signal['rationale']}",
        f"Entry: ${signal['entry_price']} | Stop Loss: ${signal['stop_loss']} | TP: {tp}",
        f"ATR%: {signal['atr_pct']} | Vol$M: {signal['dollar_volume_m']} | 52W-High: ${signal['high_52w']}",
        f"Sub-scores: Strength={signal['insider_strength']} | Volatility={signal['volatility_score']} | Liquidity={signal['liquidity_score']} | Timing={signal['timing_score']}",
    ]
    if signal.get("spy_gap_note"):
        notes_parts.append(f"SPY Gap: {signal['spy_gap_note']}")
    if signal.get("same_day_insiders", 1) > 1:
        notes_parts.append(f"Multiple insiders same day: {signal['same_day_insiders']}")
    if signal.get("disclaimer"):
        notes_parts.append(f"DISCLAIMER: {signal['disclaimer']}")

    fields = {
        "Ticker":          signal["ticker"],
        "Insider Name":    signal["insider_name"],
        "Insider Title":   signal["title"],
        "Trade Date":      signal["trade_date"],
        "Filing Date":     signal["scan_date"],
        "Shares Traded":   signal["shares"],
        "Price Per Share": signal["price_paid"],
        "Value":           signal["total_value"],
        "Is Multiple Buy": bool(signal["same_day_insiders"] > 1),
        "Notes":           "\n".join(notes_parts),
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
    """Log pipeline execution to Alert History table. Silently skips if table missing."""
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
        logger.debug(f"Run logging skipped (table may not exist yet): {e}")