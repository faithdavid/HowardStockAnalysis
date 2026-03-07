"""Insider trade data processor: fetch, parse and filter insider transactions."""

from __future__ import annotations

import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from stock_research.core.models import InsiderTrade, InsiderSignal
from stock_research.config import get_filter_config, get_scoring_config

logger = logging.getLogger(__name__)


@dataclass
class InsiderTradeRaw:
    """Raw insider trade data (before filtering/scoring)."""
    ticker: str
    insider_name: str
    insider_title: str
    trade_date: date
    transaction_type: str  # "Buy" or "Sell"
    shares: float
    price: float
    value: float
    filing_url: Optional[str] = None
    sec_filing_date: Optional[datetime] = None


class InsiderProcessorError(RuntimeError):
    """Raised when insider data fetch or parse fails."""
    pass


class InsiderProcessor:
    """Fetch and process insider trade data from OpenInsider or SEC EDGAR."""
    
    def __init__(self, timeout_s: float = 30.0):
        self.timeout_s = timeout_s
        self.filter_cfg = get_filter_config()
        self.scoring_cfg = get_scoring_config()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def fetch_insider_trades_openinsider(
        self,
        ticker: str,
        days_back: int = 30,
    ) -> list[InsiderTradeRaw]:
        """
        Scrape OpenInsider for recent insider trades on a ticker.
        
        Args:
            ticker: Stock ticker symbol (e.g. "AAPL")
            days_back: Only fetch trades from the last N days
        
        Returns:
            List of raw insider trade records
        
        Raises:
            InsiderProcessorError: On fetch or parse failure
        """
        try:
            url = f"https://www.openinsider.com/screener?s={ticker}&o=&pl=&ph=&ll=&lh=&fd=&tdmc=&l=&cc=&lcb=&hcb=&cbd=&cd=&cdr=&ta=&etd=&tr=&tdmch=&sort=&p="
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            }
            resp = requests.get(url, headers=headers, timeout=self.timeout_s)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            trades = []
            
            # Parse the insider transaction table (simplified; adjust selector if needed)
            table = soup.find("table", {"class": "tablesorter"})
            if not table:
                logger.warning(f"No insider table found for {ticker}")
                return []
            
            for row in table.find_all("tr")[1:]:  # Skip header
                cells = row.find_all("td")
                if len(cells) < 10:
                    continue
                
                try:
                    # Example: adjust indices based on actual OpenInsider table structure
                    insider_name = cells[2].get_text(strip=True)
                    insider_title = cells[3].get_text(strip=True)
                    trade_date_str = cells[5].get_text(strip=True)
                    transaction_type = cells[7].get_text(strip=True)
                    shares = float(cells[8].get_text(strip=True).replace(",", ""))
                    price = float(cells[9].get_text(strip=True).replace("$", ""))
                    value = float(cells[10].get_text(strip=True).replace(",", ""))
                    
                    trade_date = datetime.strptime(trade_date_str, "%Y-%m-%d").date()
                    
                    trade = InsiderTradeRaw(
                        ticker=ticker,
                        insider_name=insider_name,
                        insider_title=insider_title,
                        trade_date=trade_date,
                        transaction_type=transaction_type,
                        shares=shares,
                        price=price,
                        value=value,
                    )
                    trades.append(trade)
                except (ValueError, IndexError) as e:
                    logger.debug(f"Skipped malformed row for {ticker}: {e}")
                    continue
            
            logger.info(f"Fetched {len(trades)} insider trades for {ticker}")
            return trades
        
        except requests.RequestException as e:
            logger.error(f"Failed to fetch insider trades for {ticker}: {e}")
            raise InsiderProcessorError(f"OpenInsider fetch failed: {e}") from e
    
    def filter_insider_trades(
        self,
        trades: list[InsiderTradeRaw],
    ) -> list[InsiderTradeRaw]:
        """
        Apply filters to insider trades (min transaction value, shares, etc.).
        
        Args:
            trades: Raw insider trade records
        
        Returns:
            Filtered list of trades meeting criteria
        """
        filtered = [
            t for t in trades
            if (
                t.value >= self.filter_cfg.min_transaction_value
                and t.shares >= self.filter_cfg.min_shares
                and t.transaction_type.lower() == "buy"  # Focus on buys for now
            )
        ]
        logger.info(f"Filtered {len(trades)} trades -> {len(filtered)} passed filters")
        return filtered
    
    def score_insider_trades(
        self,
        trades: list[InsiderTradeRaw],
        current_atr_pct: dict[str, float],  # ticker -> ATR%
        current_volume_sma_ratio: dict[str, float],  # ticker -> ratio
    ) -> list[InsiderSignal]:
        """
        Score insider trades based on insider strength, volatility, liquidity and timing.
        
        Args:
            trades: Filtered insider trades
            current_atr_pct: Current ATR% for each ticker
            current_volume_sma_ratio: Current volume/SMA20 ratio for each ticker
        
        Returns:
            List of scored InsiderSignal records
        """
        signals = []
        
        for trade in trades:
            ticker = trade.ticker
            atr_pct = current_atr_pct.get(ticker, 0.0)
            vol_ratio = current_volume_sma_ratio.get(ticker, 1.0)
            
            # Insider strength: weight by transaction value and insider seniority
            # (Simplified: 0-1 based on value and title match for "Officer"/"Director")
            insider_score = min(trade.value / 500_000.0, 1.0)
            if "Officer" in trade.insider_title or "Director" in trade.insider_title:
                insider_score *= 1.1
            insider_score = min(insider_score, 1.0)
            
            # Volatility score: prefer 2-15% ATR
            if self.filter_cfg.atr_window_min <= atr_pct <= self.filter_cfg.atr_window_max:
                volatility_score = 1.0
            elif atr_pct < self.filter_cfg.atr_window_min:
                volatility_score = 0.6
            else:
                volatility_score = 0.4
            
            # Liquidity score: volume surge
            if vol_ratio >= self.filter_cfg.min_volume_sma_ratio:
                liquidity_score = min(vol_ratio / 2.0, 1.0)
            else:
                liquidity_score = vol_ratio / self.filter_cfg.min_volume_sma_ratio
            
            # Timing score: recent buys are better
            days_ago = (date.today() - trade.trade_date).days
            timing_score = max(1.0 - (days_ago / 30.0), 0.3)  # Decay over 30 days
            
            # Weighted overall score (0-100)
            overall = (
                insider_score * self.scoring_cfg.insider_strength_weight
                + volatility_score * self.scoring_cfg.volatility_weight
                + liquidity_score * self.scoring_cfg.liquidity_weight
                + timing_score * self.scoring_cfg.timing_weight
            ) * 100.0
            
            # Natural-language rationale
            rationale_parts = []
            if insider_score > 0.8:
                rationale_parts.append(f"High-confidence insider ({trade.insider_title})")
            if volatility_score > 0.9:
                rationale_parts.append(f"Optimal ATR% window ({atr_pct:.1f}%)")
            if liquidity_score > 0.9:
                rationale_parts.append(f"Volume surge detected ({vol_ratio:.1f}x)")
            if timing_score > 0.9:
                rationale_parts.append("Recent accumulation")
            
            rationale = " + ".join(rationale_parts) or "Insider accumulation signal"
            
            signal = InsiderSignal(
                symbol=ticker,
                signal_date=date.today(),
                score=overall,
                rationale=rationale,
            )
            signals.append(signal)
            logger.debug(f"Scored {ticker} insider trade: {overall:.1f}")
        
        # Sort by score descending
        signals.sort(key=lambda s: s.score, reverse=True)
        return signals
