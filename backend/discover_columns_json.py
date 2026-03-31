from tradingview_screener import Query
import json

test_names = [
    'EMA20', 'EMA50', 'EMA100', 'EMA200',
    'ATR', 'ATR7', 'ATR14', 'average_true_range',
    'moving_average_exponential_20', 'moving_average_exponential_50',
    'RSI', 'RSI7', 'RSI14',
    'MACD.macd', 'MACD.signal',
    'SMA20', 'SMA50', 'SMA200',
    'VWAP', 'volume', 'relative_volume_10d_calc',
    'change', 'high', 'low', 'open', 'close',
    'market_cap_basic'
]

results = {}
for name in test_names:
    try:
        q = Query().set_markets('canada').select('name', name).limit(1)
        df, count = q.get_scanner_data()
        results[name] = "SUCCESS"
    except Exception as e:
        results[name] = f"FAILED: {str(e)}"

with open('columns_results.json', 'w') as f:
    json.dump(results, f, indent=2)

print("Results saved to columns_results.json")
