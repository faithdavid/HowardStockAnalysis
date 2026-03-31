"""
test_pipeline.py
----------------
End-to-end pipeline test using sample trades WITH synthetic market data
that is designed to PASS the V1/V2 filters so we can verify that:
  - Scoring works correctly
  - Signals actually push to Airtable (Raw Insider Data table)
  - Zapier webhook fires and you can check it received the payload

Run: python test_pipeline.py
"""

import logging
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Sample insider trades ────────────────────────────────────────────────────
# Using TSX small/mid-cap names that would realistically have higher ATR
SAMPLE_TRADES = [
    {
        "ticker":       "TVE.TO",
        "company":      "Tamarack Valley Energy Ltd.",
        "insider_name": "Brian Schmidt",
        "title":        "Director",
        "trade_date":   date.today(),
        "shares":       120000,
        "price":        8.83,
        "value":        1_059_600,
    },
    {
        "ticker":       "WCP.TO",
        "company":      "Whitecap Resources Inc.",
        "insider_name": "Grant Fagerheim",
        "title":        "President & CEO",
        "trade_date":   date.today(),
        "shares":       100000,
        "price":        11.30,
        "value":        1_130_000,
    },
    {
        "ticker":       "ATH.TO",
        "company":      "Athabasca Oil Corporation",
        "insider_name": "Rob Broen",
        "title":        "CEO",
        "trade_date":   date.today(),
        "shares":       200000,
        "price":        4.50,
        "value":        900_000,
    },
    {
        "ticker":       "ATH.TO",  # 2nd insider same day = multiple buy signal
        "company":      "Athabasca Oil Corporation",
        "insider_name": "Matthew Taylor",
        "title":        "CFO",
        "trade_date":   date.today(),
        "shares":       150000,
        "price":        4.52,
        "value":        678_000,
    },
]

# ── Synthetic market data that WILL pass V1/V2 filters ───────────────────────
# These are realistic numbers for volatile TSX small-caps
MOCK_MARKET_DATA = {
    "TVE.TO": {
        "ticker":          "TVE.TO",
        "last_close":      8.85,
        "atr_pct":         8.2,     # Passes V2 (7-20% ATR)
        "dollar_volume_m": 42.0,    # Passes V1 + V2 ($30M+)
        "high_52w":        14.10,
    },
    "WCP.TO": {
        "ticker":          "WCP.TO",
        "last_close":      11.35,
        "atr_pct":         7.5,     # Passes V2 (7-20% ATR)
        "dollar_volume_m": 55.0,
        "high_52w":        15.80,
    },
    "ATH.TO": {
        "ticker":          "ATH.TO",
        "last_close":      4.52,
        "atr_pct":         12.0,    # Strong V2 qualification
        "dollar_volume_m": 38.0,
        "high_52w":        7.25,
    },
}


def run_sample_test(use_live_market_data: bool = False):
    logger.info("=" * 60)
    logger.info("PIPELINE TEST — SAMPLE DATA MODE")
    logger.info(f"Market data: {'LIVE (Polygon/yfinance)' if use_live_market_data else 'SYNTHETIC (forced to qualify)'}")
    logger.info("=" * 60)

    from scorer import score_trade, detect_repeat_buys, count_same_day_insiders
    from airtable_push import push_all_signals, log_run
    from alerts import send_alert

    # Step 1: detect repeat / same-day buys
    repeat_keys = detect_repeat_buys(SAMPLE_TRADES)
    same_day_counts = count_same_day_insiders(SAMPLE_TRADES)
    logger.info(f"Repeat keys detected: {repeat_keys}")
    logger.info(f"Same-day insider counts: {same_day_counts}")

    # Step 2: market data + scoring
    signals = []
    market_cache = {}

    for trade in SAMPLE_TRADES:
        ticker = trade["ticker"]

        if use_live_market_data:
            from market_data import get_market_data
            logger.info(f"Fetching LIVE market data for {ticker}...")
            if ticker not in market_cache:
                market_cache[ticker] = get_market_data(ticker)
            market = market_cache[ticker]
        else:
            # Use synthetic data — guaranteed to pass filters
            market = MOCK_MARKET_DATA.get(ticker)
            if ticker not in market_cache:
                market_cache[ticker] = market

        if not market:
            logger.warning(f"No market data for {ticker} — skipping")
            continue

        is_repeat  = (ticker, trade["insider_name"]) in repeat_keys
        same_day   = same_day_counts.get(ticker, 1)

        result = score_trade(
            trade=trade,
            market=market,
            is_repeat=is_repeat,
            same_day_count=same_day,
            spy_gap_pct=0.0,
        )
        if result:
            tp = f"${result['take_profit']}" if result["take_profit"] else "Hold to close"
            logger.info(
                f"  ✅ {ticker:8s}  score={result['total_score']:5.1f}  "
                f"rating={result['rating']:9s}  variant={result['variant']}  "
                f"SL=${result['stop_loss']}  TP={tp}"
            )
            logger.info(f"     rationale: {result['rationale']}")
            signals.append(result)
        else:
            logger.info(f"  ⛔ {ticker:8s}  did not qualify for V1 or V2")

    logger.info(f"\n{'─'*60}")
    logger.info(f"Total qualifying signals: {len(signals)}")

    if not signals:
        logger.info("No signals to push — done.")
        return

    # Step 3: push to Airtable
    logger.info("\n--- Pushing signals to Airtable: Raw Insider Data ---")
    try:
        ids = push_all_signals(signals)
        if ids:
            logger.info(f"✅ Pushed {len(ids)} record(s) to Airtable!")
            for i, rid in enumerate(ids):
                logger.info(f"   Record {i+1}: {rid}")
        else:
            logger.warning("No records were returned from Airtable")
    except Exception as e:
        logger.error(f"❌ Airtable push failed: {e}")

    # Step 4: send Zapier alert
    logger.info("\n--- Sending Zapier webhook alert ---")
    try:
        ok = send_alert(signals)
        if ok:
            logger.info("✅ Zapier alert sent! Check your Zapier dashboard → Task History")
        else:
            logger.warning("Zapier alert returned False — check ZAPIER_WEBHOOK_URL in .env")
    except Exception as e:
        logger.error(f"❌ Zapier alert failed: {e}")

    logger.info("\n" + "=" * 60)
    logger.info("TEST COMPLETE — check Airtable + Zapier for received data")
    logger.info("=" * 60)


if __name__ == "__main__":
    import sys
    # Pass --live as argument to use real Polygon data instead of synthetic
    use_live = "--live" in sys.argv
    run_sample_test(use_live_market_data=use_live)
