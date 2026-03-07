"""Technical indicator scanning and MGPR scoring."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

try:
    import talib
except ImportError:
    talib = None

from stock_research.core.models import TechnicalScore
from stock_research.config import get_filter_config, get_scoring_config

logger = logging.getLogger(__name__)


class TechnicalScannerError(RuntimeError):
    """Raised when technical analysis fails."""
    pass


class TechnicalScanner:
    """Compute technical indicators and MGPR scores for market symbols."""
    
    def __init__(self):
        self.filter_cfg = get_filter_config()
        self.scoring_cfg = get_scoring_config()
    
    def compute_indicators(
        self,
        ticker: str,
        closes: list[float],
        highs: list[float],
        lows: list[float],
        volumes: list[float],
    ) -> dict[str, float]:
        """
        Compute technical indicators (RSI, MACD, ATR, etc.) from OHLCV data.
        
        Args:
            ticker: Stock symbol
            closes: List of closing prices (oldest to newest)
            highs: List of high prices
            lows: List of low prices
            volumes: List of volumes
        
        Returns:
            Dict of indicator values (RSI, MACD, Signal, Histogram, ATR, etc.)
        
        Raises:
            TechnicalScannerError: If TA-Lib is not installed or data is invalid
        """
        if not talib:
            raise TechnicalScannerError(
                "TA-Lib not installed. Install with: pip install TA-Lib"
            )
        
        if len(closes) < 14:
            raise TechnicalScannerError(
                f"Insufficient data for {ticker}: need >= 14 bars, got {len(closes)}"
            )
        
        try:
            # RSI
            rsi = talib.RSI(np.array(closes), timeperiod=14)[-1]
            
            # MACD
            macd, macd_signal, macd_hist = talib.MACD(np.array(closes))
            
            # ATR
            atr = talib.ATR(np.array(highs), np.array(lows), np.array(closes), timeperiod=14)[-1]
            atr_pct = (atr / closes[-1]) * 100.0
            
            # SMA20 for volume context
            sma20 = talib.SMA(np.array(closes), timeperiod=20)[-1]
            volume_sma20 = talib.SMA(np.array(volumes), timeperiod=20)[-1]
            
            # Beta approximation: rolling correlation to SPY (simplified; requires SPY data)
            # For now, return a placeholder
            beta = 1.0
            
            return {
                "rsi": float(rsi),
                "macd": float(macd[-1]),
                "macd_signal": float(macd_signal[-1]),
                "macd_hist": float(macd_hist[-1]),
                "atr": float(atr),
                "atr_pct": float(atr_pct),
                "sma20": float(sma20),
                "volume_sma20": float(volume_sma20),
                "beta": float(beta),
            }
        except Exception as e:
            raise TechnicalScannerError(f"Indicator compute failed for {ticker}: {e}") from e
    
    def score_technical(
        self,
        ticker: str,
        indicators: dict[str, float],
        beta: float = 1.0,
    ) -> tuple[float, str]:
        """
        Compute MGPR score (Momentum, Greed, Price, Risk) on a 0-100 scale.
        
        Args:
            ticker: Stock symbol
            indicators: Dict of indicator values from compute_indicators
            beta: Stock beta (default 1.0 for market)
        
        Returns:
            Tuple of (score, rationale_text)
        """
        rsi = indicators.get("rsi", 50.0)
        macd = indicators.get("macd", 0.0)
        macd_signal = indicators.get("macd_signal", 0.0)
        atr_pct = indicators.get("atr_pct", 1.0)
        
        # Momentum: RSI above 50 and MACD > Signal
        momentum_score = 0.0
        if rsi > 50.0:
            momentum_score += (rsi - 50.0) / 50.0 * 0.5  # Up to 0.5
        if macd > macd_signal:
            momentum_score += 0.5
        momentum_score = min(momentum_score, 1.0)
        
        # Greed: volume spikes and volatility in sweet spot
        # (simplified; would use actual volume data)
        greed_score = 0.7 if 2.0 <= atr_pct <= 12.0 else 0.4
        
        # Price: technical breakouts (simplified; would use support/resistance levels)
        # For now, neutral
        price_score = 0.5
        
        # Risk: ATR and beta check
        risk_score = 1.0
        if atr_pct < self.filter_cfg.atr_window_min or atr_pct > self.filter_cfg.atr_window_max:
            risk_score -= 0.2
        if beta > self.filter_cfg.max_beta:
            risk_score -= 0.3
        elif beta < self.filter_cfg.min_beta:
            risk_score -= 0.1
        risk_score = max(risk_score, 0.0)
        
        # Weighted MGPR score (0-100)
        overall = (
            momentum_score * self.scoring_cfg.momentum_weight
            + greed_score * self.scoring_cfg.greed_weight
            + price_score * self.scoring_cfg.price_weight
            + risk_score * self.scoring_cfg.risk_weight
        ) * 100.0
        
        # Rationale
        rationale_parts = []
        if momentum_score > 0.7:
            rationale_parts.append(f"Strong momentum (RSI={rsi:.0f})")
        if greed_score > 0.7:
            rationale_parts.append(f"Optimal volatility ({atr_pct:.1f}%)")
        if risk_score > 0.7:
            rationale_parts.append("Favorable risk profile")
        
        rationale = " + ".join(rationale_parts) or "Mixed technical signal"
        
        return overall, rationale
    
    def screen_symbols(
        self,
        symbols: list[str],
        ohlcv_data: dict[str, dict],  # ticker -> {"closes": [...], "highs": [...], ...}
    ) -> list[TechnicalScore]:
        """
        Screen a list of symbols and return scored results.
        
        Args:
            symbols: List of stock symbols
            ohlcv_data: Dict mapping ticker -> OHLCV data
        
        Returns:
            List of TechnicalScore records sorted by score descending
        """
        scores = []
        
        for symbol in symbols:
            if symbol not in ohlcv_data:
                logger.warning(f"No OHLCV data for {symbol}, skipping")
                continue
            
            try:
                data = ohlcv_data[symbol]
                indicators = self.compute_indicators(
                    symbol,
                    closes=data["closes"],
                    highs=data["highs"],
                    lows=data["lows"],
                    volumes=data["volumes"],
                )
                
                score, rationale = self.score_technical(symbol, indicators)
                
                ts = TechnicalScore(
                    symbol=symbol,
                    date=date.today(),
                    score=score,
                    drivers_positive=[d for d in [
                        "RSI momentum" if indicators["rsi"] > 60 else None,
                        "MACD bullish" if indicators["macd"] > indicators["macd_signal"] else None,
                        f"ATR {indicators['atr_pct']:.1f}%" if self.filter_cfg.atr_window_min <= indicators["atr_pct"] <= self.filter_cfg.atr_window_max else None,
                    ] if d],
                    drivers_negative=[d for d in [
                        "RSI bearish" if indicators["rsi"] < 40 else None,
                        "Low liquidity" if indicators["volume_sma20"] < 100_000 else None,
                    ] if d],
                )
                scores.append(ts)
                logger.debug(f"Scored {symbol}: {score:.1f}")
            
            except TechnicalScannerError as e:
                logger.warning(f"Failed to score {symbol}: {e}")
                continue
        
        scores.sort(key=lambda s: s.score, reverse=True)
        return scores


# Import numpy locally to avoid hard dependency in type hints
import numpy as np
