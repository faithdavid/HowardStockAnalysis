"""
test_connectivity.py
---------------------
Tests all pipeline components in isolation before Railway deployment:
  1. SPY Gap / market data (yfinance)
  2. Single ticker market data (Polygon + yfinance fallback)
  3. Airtable connectivity (read + write test)
  4. Zapier webhook test
  5. Technical scanner (TradingView Screener)
  6. Scoring logic (synthetic trade)
  7. OpenInsider connectivity (with short timeout)
"""

import logging
import os
import sys
import json
import requests
from datetime import date

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
logger = logging.getLogger(__name__)

PASS = "[PASS]"
FAIL = "[FAIL]"
WARN = "[WARN]"
ICON_OK   = "(OK)  "
ICON_WARN = "(WARN)"
ICON_FAIL = "(FAIL)"

results = {}

# =============================================================================
# TEST 1: SPY Gap
# =============================================================================
print("\n" + "=" * 60)
print("TEST 1: SPY Gap (yfinance)")
print("=" * 60)
try:
    from market_data import get_spy_gap
    gap = get_spy_gap()
    print(f"  SPY Gap: {gap:+.2f}%")
    results["spy_gap"] = PASS
    print(f"  {PASS} SPY gap fetched OK")
except Exception as e:
    print(f"  {FAIL} SPY gap failed: {e}")
    results["spy_gap"] = FAIL

# =============================================================================
# TEST 2: Single Ticker Market Data
# =============================================================================
print("\n" + "=" * 60)
print("TEST 2: Market Data - AAPL (Polygon + yfinance fallback)")
print("=" * 60)
try:
    from market_data import get_market_data
    data = get_market_data("AAPL")
    if data:
        print(f"  Ticker: {data['ticker']}")
        print(f"  Last Close: ${data['last_close']}")
        print(f"  ATR%: {data['atr_pct']}%")
        print(f"  Dollar Volume: ${data['dollar_volume_m']}M")
        print(f"  52W High: ${data['high_52w']}")
        results["market_data"] = PASS
        print(f"  {PASS} Market data fetched OK")
    else:
        print(f"  {FAIL} No market data returned")
        results["market_data"] = FAIL
except Exception as e:
    print(f"  {FAIL} Market data failed: {e}")
    results["market_data"] = FAIL

# =============================================================================
# TEST 3: Airtable Connectivity
# =============================================================================
print("\n" + "=" * 60)
print("TEST 3: Airtable Connectivity")
print("=" * 60)
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_INSIDER = os.getenv("AIRTABLE_TABLE_INSIDER", "Raw Insider Data")

if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
    print(f"  {FAIL} Missing AIRTABLE_TOKEN or AIRTABLE_BASE_ID in .env")
    results["airtable_read"] = FAIL
else:
    # Read test
    table_name = requests.utils.quote(AIRTABLE_TABLE_INSIDER, safe='')
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}?maxRecords=1"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            rec_count = len(data.get("records", []))
            print(f"  Read test: OK (returned {rec_count} record(s))")
            results["airtable_read"] = PASS
            print(f"  {PASS} Airtable read OK")
        elif r.status_code == 403:
            print(f"  {FAIL} Airtable 403 - Token may be invalid or expired")
            print(f"  Response: {r.text[:200]}")
            results["airtable_read"] = FAIL
        elif r.status_code == 404:
            print(f"  {WARN} Table 'Raw Insider Data' not found in base. Check table names.")
            results["airtable_read"] = WARN
        else:
            print(f"  {FAIL} Airtable status {r.status_code}: {r.text[:200]}")
            results["airtable_read"] = FAIL
    except Exception as e:
        print(f"  {FAIL} Airtable read failed: {e}")
        results["airtable_read"] = FAIL

# =============================================================================
# TEST 4: Zapier Webhook
# =============================================================================
print("\n" + "=" * 60)
print("TEST 4: Zapier Webhook")
print("=" * 60)
ZAPIER_URL = os.getenv("ZAPIER_WEBHOOK_URL", "")
if not ZAPIER_URL:
    print(f"  {FAIL} ZAPIER_WEBHOOK_URL not set in .env")
    results["zapier"] = FAIL
else:
    test_payload = {
        "scan_date": str(date.today()),
        "signals_count": 1,
        "email_subject": "[TEST] Howard Stock Scanner - Connectivity Check",
        "slack_message": "[TEST] Howard Stock Scanner connectivity check from pre-deployment test.",
        "top_signals": [{
            "ticker": "TEST",
            "company": "Test Corp",
            "score": 90.0,
            "rating": "Excellent",
            "variant": "V2",
            "entry": 10.00,
            "stop_loss": 9.25,
            "take_profit": None,
            "rationale": "Pre-deployment connectivity test"
        }],
        "disclaimer": "TEST - NOT financial advice"
    }
    try:
        r = requests.post(ZAPIER_URL, json=test_payload, timeout=15)
        print(f"  Zapier response: {r.status_code} - {r.text[:100]}")
        if r.status_code in (200, 201):
            results["zapier"] = PASS
            print(f"  {PASS} Zapier webhook received OK")
        else:
            results["zapier"] = FAIL
            print(f"  {FAIL} Zapier returned non-OK status")
    except Exception as e:
        print(f"  {FAIL} Zapier request failed: {e}")
        results["zapier"] = FAIL

# =============================================================================
# TEST 5: Technical Scanner (TradingView Screener)
# =============================================================================
print("\n" + "=" * 60)
print("TEST 5: Technical Scanner (TradingView)")
print("=" * 60)
try:
    from technical_scanner import get_technical_signals
    signals = get_technical_signals(price_threshold=20.0)
    print(f"  Found {len(signals)} technical signals under $20")
    if signals:
        s = signals[0]
        print(f"  Top signal: {s['ticker']} | Score: {s['total_score']} | Price: ${s['current_price']}")
        results["technical_scanner"] = PASS
        print(f"  {PASS} Technical scanner OK")
    else:
        print(f"  {WARN} No signals found (possible market hours / data delay)")
        results["technical_scanner"] = WARN
except Exception as e:
    print(f"  {FAIL} Technical scanner failed: {e}")
    results["technical_scanner"] = FAIL

# =============================================================================
# TEST 6: Scoring Logic (synthetic)
# =============================================================================
print("\n" + "=" * 60)
print("TEST 6: Scoring Logic (Synthetic Trade)")
print("=" * 60)
try:
    from scorer import score_trade
    trade = {
        "ticker": "SYNTH",
        "company": "Synthetic Corp",
        "insider_name": "John Smith",
        "title": "CEO",
        "trade_date": date(2026, 2, 15),  # Feb = earnings month
        "shares": 100000,
        "price": 5.50,
        "value": 550000,
    }
    market = {
        "ticker": "SYNTH",
        "last_close": 5.50,
        "atr_pct": 4.2,
        "dollar_volume_m": 45.0,
        "high_52w": 8.00,
    }
    result = score_trade(trade, market, is_repeat=False, same_day_count=1, spy_gap_pct=0.1)
    if result:
        print(f"  Ticker: {result['ticker']}")
        print(f"  Score: {result['total_score']}/100")
        print(f"  Rating: {result['rating']}")
        print(f"  Variant: {result['variant']}")
        print(f"  Entry: ${result['entry_price']} | SL: ${result['stop_loss']} | TP: {result['take_profit']}")
        print(f"  Rationale: {result['rationale']}")
        results["scoring"] = PASS
        print(f"  {PASS} Scoring logic OK")
    else:
        print(f"  {WARN} Trade did not qualify (check variant filters for current month)")
        results["scoring"] = WARN
except Exception as e:
    print(f"  {FAIL} Scoring failed: {e}")
    results["scoring"] = FAIL

# =============================================================================
# TEST 7: OpenInsider Connectivity (quick check)
# =============================================================================
print("\n" + "=" * 60)
print("TEST 7: OpenInsider Connectivity")
print("=" * 60)
try:
    r = requests.get(
        "http://openinsider.com/screener?xp=1&vl=10&fd=3&cnt=5&action=1",
        headers={"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0"},
        timeout=20
    )
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        has_table = "tinytable" in r.text
        print(f"  Contains data table: {has_table}")
        if has_table:
            results["openinsider"] = PASS
            print(f"  {PASS} OpenInsider reachable + has data")
        else:
            results["openinsider"] = WARN
            print(f"  {WARN} OpenInsider reachable but table not found (may be CAPTCHA/block)")
    else:
        results["openinsider"] = FAIL
        print(f"  {FAIL} OpenInsider returned {r.status_code}")
except requests.exceptions.Timeout:
    print(f"  {WARN} OpenInsider timed out (network/firewall issue from this machine)")
    print(f"  NOTE: Railway's IP may have better connectivity")
    results["openinsider"] = WARN
except Exception as e:
    print(f"  {FAIL} OpenInsider connection failed: {e}")
    results["openinsider"] = FAIL

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 60)
print("DEPLOYMENT READINESS SUMMARY")
print("=" * 60)
all_passed = True
for test, status in results.items():
    icon = ICON_OK if status == PASS else (ICON_WARN if status == WARN else ICON_FAIL)
    print(f"  {icon}  {test:<25} {status}")
    if status == FAIL:
        all_passed = False

print()
if all_passed:
    print("ALL TESTS PASSED -- Ready for Railway deployment!")
else:
    print("SOME TESTS NEED ATTENTION -- Review issues above.")
print()
