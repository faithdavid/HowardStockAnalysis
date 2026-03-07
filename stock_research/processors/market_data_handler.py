"""Market data handler: fetch aggregated bars and compute ATR/volume metrics."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from stock_research.config import get_api_config, get_filter_config

logger = logging.getLogger(__name__)


class MarketDataHandlerError(RuntimeError):
    """Raised when market data fetch fails."""
    pass


class MarketDataHandler:
    """Fetch and aggregate market data from Polygon.io or similar."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_cfg = get_api_config()
        self.filter_cfg = get_filter_config()
        self.api_key = api_key or self.api_cfg.polygon_api_key
        
        if not self.api_key:
            logger.warning("Polygon API key not configured; market data fetch will fail")
    
    def fetch_bars(
        self,
        ticker: str,
        from_date: date,
        to_date: date,
    ) -> list[dict]:
        """
        Fetch aggregated bars (OHLCV) from Polygon.
        
        Args:
            ticker: Stock symbol
            from_date: Start date (inclusive)
            to_date: End date (inclusive)
        
        Returns:
            List of bar dicts with keys: date, open, high, low, close, volume
        
        Raises:
            MarketDataHandlerError: On API failure
        """
        if not self.api_key:
            raise MarketDataHandlerError("Polygon API key not configured")
        
        # Placeholder implementation
        # In production, call Polygon API: GET /v1/open-close/{ticker}/{date}
        # or /v2/aggs/ticker/{ticker}/range/1/day/{fromDate}/{toDate}
        
        logger.info(f"Fetching bars for {ticker} from {from_date} to {to_date}")
        return []
    
    def compute_atr_pct(
        self,
        bars: list[dict],
        lookback_days: int = 14,
    ) -> float:
        """
        Compute Average True Range as % of close.
        
        Args:
            bars: List of OHLCV bars
            lookback_days: Number of bars to use for ATR (default 14)
        
        Returns:
            ATR as percentage of the most recent close
        """
        if len(bars) < lookback_days:
            logger.warning(f"Insufficient bars ({len(bars)}) for ATR calculation")
            return 0.0
        
        recent = bars[-lookback_days:]
        tr_values = []
        
        for i, bar in enumerate(recent):
            if i == 0:
                tr = bar["high"] - bar["low"]
            else:
                prev_close = recent[i - 1]["close"]
                tr = max(
                    bar["high"] - bar["low"],
                    abs(bar["high"] - prev_close),
                    abs(bar["low"] - prev_close),
                )
            tr_values.append(tr)
        
        atr = sum(tr_values) / len(tr_values)
        close = bars[-1]["close"]
        atr_pct = (atr / close) * 100.0
        
        logger.debug(f"ATR={atr:.2f}, Close={close:.2f}, ATR%={atr_pct:.2f}%")
        return atr_pct
    
    def compute_volume_sma_ratio(
        self,
        bars: list[dict],
        sma_period: int = 20,
    ) -> float:
        """
        Compute current volume / SMA20 ratio.
        
        Args:
            bars: List of OHLCV bars
            sma_period: SMA period (default 20)
        
        Returns:
            Ratio of most recent volume to SMA
        """
        if len(bars) < sma_period:
            logger.warning(f"Insufficient bars ({len(bars)}) for volume SMA")
            return 1.0
        
        recent_bars = bars[-sma_period:]
        volumes = [b["volume"] for b in recent_bars]
        sma = sum(volumes) / len(volumes)
        current_vol = volumes[-1]
        
        ratio = current_vol / sma if sma > 0 else 1.0
        logger.debug(f"Current volume={current_vol}, SMA{sma_period}={sma:.0f}, Ratio={ratio:.2f}x")
        return ratio
    
    def check_spy_gaps(
        self,
        spy_bars: list[dict],
        lookback_days: int = 5,
    ) -> dict[str, float]:
        """
        Check for SPY gaps (up or down) in the last N days.
        
        Args:
            spy_bars: SPY OHLCV bars
            lookback_days: How many days to check
        
        Returns:
            Dict with "gap_up_pct" and "gap_down_pct" (negative gap_down)
        """
        if len(spy_bars) < lookback_days + 1:
            logger.warning(f"Insufficient SPY bars for gap check")
            return {"gap_up_pct": 0.0, "gap_down_pct": 0.0}
        
        recent = spy_bars[-lookback_days:]
        max_gap_up = 0.0
        min_gap_down = 0.0
        
        for i in range(len(recent) - 1):
            prev_close = recent[i]["close"]
            curr_open = recent[i + 1]["open"]
            gap_pct = ((curr_open - prev_close) / prev_close) * 100.0
            
            if gap_pct > 0:
                max_gap_up = max(max_gap_up, gap_pct)
            else:
                min_gap_down = min(min_gap_down, gap_pct)
        
        logger.debug(f"SPY gaps: up {max_gap_up:.2f}%, down {min_gap_down:.2f}%")
        return {
            "gap_up_pct": max_gap_up,
            "gap_down_pct": min_gap_down,
        }
