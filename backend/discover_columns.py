from tradingview_screener import Query
import sys

# Test a broad list of potential indicator names
test_names = [
    'EMA20', 'EMA50', 'EMA100', 'EMA200',
    'ATR', 'ATR7', 'ATR14', 'average_true_range',
    'moving_average_exponential_20', 'moving_average_exponential_50',
    'RSI7', 'RSI14', 'RSI[14]',
    'MACD.macd', 'MACD.signal',
    'SMA20', 'SMA50', 'SMA200',
    'VWAP', 'BB.upper', 'BB.lower',
    'volume', 'relative_volume_10d_calc',
    'change', 'change_abs',
    'high', 'low', 'open', 'close',
    'P.E', 'market_cap_basic'
]

results = []
print(f"Testing {len(test_names)} potential column names...")

for name in test_names:
    try:
        # We use a very small query to test each column
        q = Query().set_markets('canada').select('name', name).limit(1)
        df, count = q.get_scanner_data()
        results.append((name, "SUCCESS"))
    except Exception as e:
        results.append((name, f"FAILED: {e}"))

print("\n--- RESULTS ---")
for name, status in results:
    print(f"{name:30} : {status}")
