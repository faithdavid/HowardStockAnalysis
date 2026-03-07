"""Backtesting utilities for "buy open / sell close" and MGPR strategies."""

from __future__ import annotations

import logging
from datetime import date
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Summary statistics from a backtest run."""
    symbol: str
    start_date: date
    end_date: date
    num_trades: int
    num_wins: int
    num_losses: int
    win_rate: float  # 0-1
    total_return_pct: float
    avg_return_per_trade: float
    sharpe_ratio: float
    max_drawdown_pct: float


class BacktesterError(RuntimeError):
    """Raised when backtest setup or execution fails."""
    pass


class Backtester:
    """Simulate "buy open / sell close" strategies on historical OHLCV data."""
    
    def __init__(self):
        pass
    
    def simulate_buy_open_sell_close(
        self,
        bars: list[dict],  # Historical OHLCV bars: date, open, high, low, close, volume
        entry_signal_dates: set[date],  # Dates on which to enter long at open
        exit_on_loss_pct: float = -5.0,  # Stop loss %
        exit_on_profit_pct: float = 3.0,  # Take profit %
    ) -> BacktestResult:
        """
        Simulate "buy at open, sell at close" for signal dates.
        
        Args:
            bars: List of OHLCV bar dicts
            entry_signal_dates: Set of dates to enter trades
            exit_on_loss_pct: Stop loss threshold (e.g., -5 = exit on -5%)
            exit_on_profit_pct: Take profit threshold (e.g., 3 = exit on +3%)
        
        Returns:
            BacktestResult summary
        
        Raises:
            BacktesterError: On invalid input
        """
        if not bars:
            raise BacktesterError("No bars provided")
        
        if not entry_signal_dates:
            logger.warning("No entry signal dates; empty backtest")
            return BacktestResult(
                symbol=bars[0].get("symbol", "UNKNOWN"),
                start_date=bars[0]["date"],
                end_date=bars[-1]["date"],
                num_trades=0,
                num_wins=0,
                num_losses=0,
                win_rate=0.0,
                total_return_pct=0.0,
                avg_return_per_trade=0.0,
                sharpe_ratio=0.0,
                max_drawdown_pct=0.0,
            )
        
        symbol = bars[0].get("symbol", "UNKNOWN")
        trades = []  # List of (entry_price, exit_price, return_pct)
        
        for bar in bars:
            bar_date = bar["date"]
            
            # Check if we should enter on this date
            if bar_date in entry_signal_dates:
                entry_price = bar["open"]
                
                # For simplicity, exit at close of same day
                # In production, support multi-day holds with intra-day stops
                exit_price = bar["close"]
                return_pct = ((exit_price - entry_price) / entry_price) * 100.0
                
                trades.append({
                    "entry_date": bar_date,
                    "entry_price": entry_price,
                    "exit_date": bar_date,
                    "exit_price": exit_price,
                    "return_pct": return_pct,
                })
        
        # Compute statistics
        if not trades:
            logger.warning(f"No trades executed for {symbol}")
            return BacktestResult(
                symbol=symbol,
                start_date=bars[0]["date"],
                end_date=bars[-1]["date"],
                num_trades=0,
                num_wins=0,
                num_losses=0,
                win_rate=0.0,
                total_return_pct=0.0,
                avg_return_per_trade=0.0,
                sharpe_ratio=0.0,
                max_drawdown_pct=0.0,
            )
        
        returns = [t["return_pct"] for t in trades]
        num_wins = sum(1 for r in returns if r > 0)
        num_losses = len(returns) - num_wins
        win_rate = num_wins / len(returns) if returns else 0.0
        total_return = sum(returns)
        avg_return = total_return / len(returns)
        
        # Sharpe ratio (simplified, assuming risk-free = 0)
        if len(returns) > 1:
            variance = sum((r - avg_return) ** 2 for r in returns) / len(returns)
            std_dev = variance ** 0.5
            sharpe_ratio = avg_return / std_dev if std_dev > 0 else 0.0
        else:
            sharpe_ratio = 0.0
        
        # Max drawdown (simplified: peak to trough)
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for r in returns:
            cumulative += r
            if cumulative > peak:
                peak = cumulative
            dd = ((peak - cumulative) / abs(peak)) * 100.0 if peak != 0 else 0.0
            max_dd = max(max_dd, dd)
        
        logger.info(
            f"Backtest {symbol}: {len(trades)} trades, "
            f"{num_wins} wins ({win_rate*100:.1f}%), "
            f"Total={total_return:.2f}%, Avg={avg_return:.2f}%, Sharpe={sharpe_ratio:.2f}"
        )
        
        return BacktestResult(
            symbol=symbol,
            start_date=bars[0]["date"],
            end_date=bars[-1]["date"],
            num_trades=len(trades),
            num_wins=num_wins,
            num_losses=num_losses,
            win_rate=win_rate,
            total_return_pct=total_return,
            avg_return_per_trade=avg_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown_pct=max_dd,
        )
