import logging
from tradingview_scraper import TradingView

logging.basicConfig(level=logging.INFO)
tv = TradingView()

# Test getting TSX (Canada) stocks
# We want to see what fields we can get
try:
    # Get top gainers from Canada to see the structure
    # Markets supported include 'america', 'canada', etc.
    res = tv.get_process(market='canada', indicators=['RSI', 'MACD.macd', 'MACD.signal', 'ATR', 'average_volume_10d_calc', 'EMA20', 'EMA50'])
    if res and len(res) > 0:
        print("Fields available:", list(res.iloc[0].keys()))
        print("\nSample row:")
        print(res.iloc[0])
    else:
        print("No results found or empty response.")
except Exception as e:
    print(f"Error testing tradingview-scraper: {e}")
