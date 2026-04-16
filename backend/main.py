"""
main.py
-------
The main pipeline. Run this every morning before market open.

What it does:
  1. Scrapes OpenInsider for today's insider buys
  2. Gets ATR% and volume for each ticker (via yfinance)
  3. Filters and scores each trade (book strategy, V1 + V2)
  4. Pushes results to Airtable
  5. Sends alert via Zapier → Email + Slack

Usage:
  python main.py

Schedule (cron example — runs at 7:00 AM EST / 5:00 AM MST):
  0 7 * * 1-5 cd /path/to/insider_scanner && python main.py >> logs/run.log 2>&1
"""

import logging
import os
from dotenv import load_dotenv

from scraper import fetch_insider_buys
from market_data import get_market_data, get_spy_gap
from scorer import score_trade, detect_repeat_buys, count_same_day_insiders
from airtable_push import push_all_signals, log_run, log_alert, push_all_tech_signals
from technical_scanner import get_technical_signals
from alerts import send_alert

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Only push signals above this score to Airtable / alerts
MIN_SCORE_FOR_ALERT = float(os.getenv("MIN_SCORE_FOR_ALERT") or "85")


def run():
    logger.info("=" * 60)
    logger.info("INSIDER SCANNER STARTING")
    logger.info("=" * 60)

    # ── Step 0: Market Intelligence (SPY Gap) ────────────────────
    spy_gap = get_spy_gap()
    logger.info(f"Current SPY Gap: {spy_gap:+.2f}%")

    # ── Step 1: Scrape insider buys ──────────────────────────────
    try:
        trades = fetch_insider_buys()
    except Exception as e:
        msg = f"Scraper failed: {e}"
        logger.error(msg)
        log_run("FAILED", msg)
        return

    if not trades:
        msg = "No insider buys found today"
        logger.info(msg)
        log_run("COMPLETED", msg, 0)

    logger.info(f"Scraped {len(trades)} trades")

    # ── Step 2: Detect repeat buys and same-day multi-insider ────
    repeat_keys = detect_repeat_buys(trades)
    same_day_counts = count_same_day_insiders(trades)

    # ── Step 3: Get market data + score each trade ───────────────
    signals = []
    seen_tickers = set()  # avoid duplicate market data fetches

    market_cache = {}
    for trade in trades:
        ticker = trade["ticker"]
        if ticker not in market_cache:
            market_cache[ticker] = get_market_data(ticker)

        market = market_cache[ticker]
        if not market:
            logger.warning(f"Skipping {ticker} — no market data")
            continue

        is_repeat = (ticker, trade["insider_name"]) in repeat_keys
        same_day = same_day_counts.get(ticker, 1)

        result = score_trade(
            trade=trade,
            market=market,
            is_repeat=is_repeat,
            same_day_count=same_day,
            spy_gap_pct=spy_gap,
        )

        if result:
            signals.append(result)
            logger.info(
                f"  {ticker:6s}  score={result['total_score']:5.1f}  "
                f"rating={result['rating']:9s}  variant={result['variant']}"
            )

    logger.info(f"Scored {len(signals)} qualifying signals")

    if not signals:
        msg = "No signals passed filters today"
        logger.info(msg)
        log_run("COMPLETED", msg, 0)

    # ── Step 4: Push to Airtable ─────────────────────────────────
    raw_ids = []
    airtable_ok = True
    if signals and os.getenv("AIRTABLE_TOKEN") and os.getenv("AIRTABLE_BASE_ID"):
        try:
            raw_ids = push_all_signals(signals)
            logger.info(f"Pushed {len(raw_ids)} records to Airtable")
        except Exception as e:
            logger.error(f"Airtable push failed: {e}")
            airtable_ok = False
    else:
        logger.warning("Airtable not configured — skipping push")

    # ── Step 5: Send alert via Zapier ────────────────────────────
    alert_signals = [s for s in signals if s["total_score"] >= MIN_SCORE_FOR_ALERT]
    alert_ok = False
    if alert_signals:
        alert_ok = send_alert(alert_signals)
    else:
        logger.info(f"No signals above {MIN_SCORE_FOR_ALERT} score threshold for alert")

    # ── Step 6: Log to Alert History in Airtable ─────────────────
    if alert_signals:
        log_alert(alert_signals, raw_ids, status="Sent" if alert_ok else "Failed")

    # ── Step 7: Technical Scan ──────────────────────────────────
    logger.info("Starting Technical MGPR Scans ($5, $10, $20)...")
    tech_map = {}
    for threshold in [5.0, 10.0, 20.0]:
        try:
            batch = get_technical_signals(price_threshold=threshold)
            for s in batch:
                ticker = s["ticker"]
                # Keep if new or if this version has a higher score
                if ticker not in tech_map or s["total_score"] > tech_map[ticker]["total_score"]:
                    tech_map[ticker] = s
        except Exception as e:
            logger.error(f"Technical scan for ${threshold} failed: {e}")
    
    tech_signals = list(tech_map.values())
    logger.info(f"Generated {len(tech_signals)} unique Technical signals")

    # ── Step 8: Push Technical Signals to Airtable ─────────────
    if tech_signals:
        try:
            tech_ids = push_all_tech_signals(tech_signals)
            logger.info(f"Pushed {len(tech_ids)} Technical signals to Airtable")
        except Exception as e:
            logger.error(f"Technical Airtable push failed: {e}")

    # ── Step 9: Send Technical alerts ───────────────────────────
    # We can send a separate alert for technical signals if any are high-scoring
    high_tech = [s for s in tech_signals if s["total_score"] >= 85]
    if high_tech:
        send_alert(high_tech)  # Using same alert logic for now
    
    logger.info("=" * 60)
    logger.info(f"DONE — {len(signals)} Insider, {len(tech_signals)} Tech signals")
    logger.info("=" * 60)

    return signals


if __name__ == "__main__":
    run()
