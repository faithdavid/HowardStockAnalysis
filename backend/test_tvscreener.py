import pandas as pd
from tradingview_screener import Query, Column

def test_screener():
    print("Querying TradingView Screener (Canada)...")
    try:
        # We target Canada market, and filter for Price < 20
        # Indicators: RSI, MACD, Volume, ATR
        # Note: ATR in TV screener is often 'ATR' or 'average_true_range'
        q = (Query()
             .set_markets('canada')
             .select('name', 'close', 'volume', 'relative_volume_10d_calc', 'RSI', 'MACD.macd', 'MACD.signal', 'EMA20', 'EMA50')
             .where(
                 Column('close') > 0.1,
                 Column('close') < 20.0,
                 Column('volume') > 50000
             )
             .limit(10))
        
        df, count = q.get_scanner_data()
        print(f"Found {count} stocks. Showing top 10:")
        print(df)
        
        if not df.empty:
            print("\nColumns available:", df.columns.tolist())
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_screener()
