"""
backtester.py
-------------
Replays historical insider or technical signals against real price data
and computes strategy performance metrics.

Supports two modules:
  - "Insider"              : replays signals from the insider pipeline
  - "Technical_Under_5"   : technical scanner signals, price <= $5
  - "Technical_Under_10"  : technical scanner signals, price <= $10
  - "Technical_Under_20"  : technical scanner signals, price <= $20

How it works:
  1. Fetch historical insider buys from OpenInsider (extended date range)
     OR run the technical scanner on a past date window
  2. For each signal, fetch post-entry price data via yfinance
  3. Simulate the trade using the exact variant SL/TP logic from scorer.py
  4. Collect all trade outcomes and compute aggregate statistics
  5. Calculate a random-control baseline (simulated naive buy/hold)
  6. Push the full report to Airtable: Historical/Backtest

Usage:
  python backtester.py --module Insider --start 2024-10-01 --end 2025-03-01
  python backtester.py --module Technical_Under_10 --start 2024-11-01 --end 2025-02-28
  python backtester.py --help
"""

import argparse
import json
import logging
import math
import os
import random
import time
from datetime import date, datetime, timedelta

import requests
import yfinance as yf
from dotenv import load_dotenv

from scraper import fetch_insider_buys
from market_data import get_market_data, get_spy_gap
from scorer import score_trade, detect_repeat_buys, count_same_day_insiders
from technical_scanner import get_technical_signals, calculate_mgpr
from airtable_push import push_backtest_result

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# How many calendar days to hold after entry if no TP/SL is hit
# WSV Mastery is strictly 1-day (intraday) buy-at-open sell-at-close.
DEFAULT_HOLD_DAYS = 1

# Risk-free rate for Sharpe calculation (annualised %)
RISK_FREE_RATE_ANNUAL = 4.5   # ~current T-bill rate
TRADING_DAYS_PER_YEAR = 252

# Number of random control trades for baseline comparison
RANDOM_CONTROL_SAMPLES = 200


# ── Trade Simulation ──────────────────────────────────────────────────────────

def simulate_trade(
    ticker: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float | None,
    entry_date: date,
    hold_days: int = DEFAULT_HOLD_DAYS,
) -> dict | None:
    """
    Simulate a single trade by looking at post-entry daily data.
    WSV Sync: models same-day (Buy at Open, Exit by Close).
    """
    try:
        # We assume the signal is traded the day AFTER the report date (or next trading day)
        fetch_start = entry_date 
        fetch_end   = entry_date + timedelta(days=7) # Get a week to find the next trading day

        data = yf.download(
            ticker,
            start=fetch_start.strftime("%Y-%m-%d"),
            end=fetch_end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=True,
        )

        if data is None or data.empty:
            return None

        # Flatten MultiIndex
        if hasattr(data.columns, "levels"):
            data.columns = data.columns.droplevel(1)

        # We buy on the first available trading day ON or AFTER the report date
        # (Though usually one day after is safer for backtests)
        trading_days = data[data.index.date >= entry_date]
        if trading_days.empty:
            return None

        # Use the first trading day as the execution day
        exec_day_idx = 0
        if len(trading_days) > 1:
            exec_day_idx = 1 # Buy 1 day after filing
            
        target_day = trading_days.iloc[exec_day_idx]
        ex_date    = trading_days.index[exec_day_idx].date()

        day_high  = float(target_day["High"])
        day_low   = float(target_day["Low"])
        day_close = float(target_day["Close"])

        # 1. Check Stop Loss (Priority 1)
        if day_low <= stop_loss:
            return {
                "outcome":     "loss",
                "return_pct":  round(((stop_loss - entry_price) / entry_price) * 100, 2),
                "exit_price":  stop_loss,
                "exit_date":   str(ex_date),
                "exit_reason": "stop_loss",
                "hold_days":   1,
            }

        # 2. Check Take Profit (Priority 2)
        if take_profit and day_high >= take_profit:
            return {
                "outcome":     "win",
                "return_pct":  round(((take_profit - entry_price) / entry_price) * 100, 2),
                "exit_price":  take_profit,
                "exit_date":   str(ex_date),
                "exit_reason": "take_profit",
                "hold_days":   1,
            }

        # 3. Exit at Close (Default)
        return {
            "outcome":     "win" if day_close > entry_price else "loss",
            "return_pct":  round(((day_close - entry_price) / entry_price) * 100, 2),
            "exit_price":  round(day_close, 2),
            "exit_date":   str(ex_date),
            "exit_reason": "market_close",
            "hold_days":   1,
        }

    except Exception as e:
        logger.warning(f"Simulation failed for {ticker}: {e}")
        return None


# ── Performance Metrics ───────────────────────────────────────────────────────

def format_results_table(results: list[dict]) -> str:
    """Format individual trade results into a Markdown table string."""
    if not results:
        return "No trades recorded."
    
    header = "| Ticker | Outcome | Return % | Exit Date | Reason | Score |\n"
    separator = "|--------|---------|----------|-----------|--------|-------|\n"
    rows = []
    for r in results:
        ticker      = r.get("ticker", "N/A")
        outcome     = r.get("outcome", "N/A")
        return_pct  = r.get("return_pct", 0.0)
        exit_date   = r.get("exit_date", "N/A")
        exit_reason = r.get("exit_reason", "N/A")
        score       = r.get("score", 0.0)
        
        rows.append(
            f"| {ticker:8} | {outcome:7} | {return_pct:+8.2f}% | "
            f"{exit_date} | {exit_reason:12} | {score:5.1f} |"
        )
    return header + separator + "\n".join(rows)


def compute_metrics(results: list[dict], module: str, start: str, end: str) -> dict:
    """
    Given a list of trade result dicts (each with return_pct, outcome),
    compute all Airtable backtest metrics.
    """
    if not results:
        return {}

    returns = [r["return_pct"] for r in results]
    wins    = [r for r in results if r["outcome"] == "win"]
    losses  = [r for r in results if r["outcome"] == "loss"]

    total_trades   = len(results)
    win_count      = len(wins)
    win_rate       = (win_count / total_trades) * 100
    avg_return     = sum(returns) / total_trades
    total_return   = sum(returns)

    # Sharpe Ratio (daily returns, annualised)
    if total_trades > 1:
        mean_r = avg_return
        std_r  = math.sqrt(
            sum((r - mean_r) ** 2 for r in returns) / (total_trades - 1)
        )
        rf_per_trade = RISK_FREE_RATE_ANNUAL / TRADING_DAYS_PER_YEAR
        sharpe = ((mean_r - rf_per_trade) / std_r) if std_r > 0 else 0.0
        sharpe = round(sharpe * math.sqrt(TRADING_DAYS_PER_YEAR), 3)
    else:
        sharpe = 0.0

    # Max Drawdown (running equity curve)
    equity = 100.0
    peak   = equity
    max_dd = 0.0
    for r in returns:
        equity += r
        if equity > peak:
            peak = equity
        dd = ((peak - equity) / peak) * 100
        if dd > max_dd:
            max_dd = dd

    # Profit Factor = gross profit / gross loss
    gross_profit = sum(r for r in returns if r > 0)
    gross_loss   = abs(sum(r for r in returns if r < 0))
    profit_factor = round(gross_profit / gross_loss, 3) if gross_loss > 0 else float("inf")

    edge_metrics_lines = [
        f"Total Trades: {total_trades}",
        f"Wins: {win_count}  |  Losses: {len(losses)}",
        f"Avg Win: {sum(r['return_pct'] for r in wins)/max(len(wins),1):.2f}%",
        f"Avg Loss: {sum(r['return_pct'] for r in losses)/max(len(losses),1):.2f}%",
        f"Best Trade: {max(returns):.2f}%",
        f"Worst Trade: {min(returns):.2f}%",
    ]

    return {
        "test_name":        f"{module} Backtest {start} -> {end}",
        "module":           module,
        "date_range_start": start,
        "date_range_end":   end,
        "total_trades":     total_trades,
        "win_rate":         round(win_rate, 2),
        "average_return":   round(avg_return, 3),
        "total_return":     round(total_return, 2),
        "sharpe_ratio":     sharpe,
        "max_drawdown":     round(-max_dd, 2),
        "profit_factor":    profit_factor,
        "edge_metrics":     "\n".join(edge_metrics_lines),
        "status":           "Complete",
        "tested_by":        "AutoPipeline",
    }


# ── Random Control Baseline ───────────────────────────────────────────────────

def run_random_control(
    tickers: list[str],
    start: date,
    end: date,
    n_samples: int = RANDOM_CONTROL_SAMPLES,
) -> float:
    """
    Simulate random stock picks from the same ticker pool and date range.
    Returns the random baseline win rate %.
    A "win" is defined as a positive return over DEFAULT_HOLD_DAYS.
    """
    logger.info(f"Running {n_samples}-sample random control baseline...")
    wins = 0
    total = 0
    date_range = (end - start).days

    for _ in range(n_samples):
        ticker = random.choice(tickers)
        random_offset = random.randint(0, max(date_range - DEFAULT_HOLD_DAYS, 1))
        rand_entry_date = start + timedelta(days=random_offset)

        try:
            data = yf.download(
                ticker,
                start=rand_entry_date.strftime("%Y-%m-%d"),
                end=(rand_entry_date + timedelta(days=DEFAULT_HOLD_DAYS + 5)).strftime("%Y-%m-%d"),
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
            if data is None or len(data) < 2:
                continue

            if hasattr(data.columns, "levels"):
                data.columns = data.columns.droplevel(1)

            entry  = float(data["Close"].iloc[0])
            exit_p = float(data["Close"].iloc[min(DEFAULT_HOLD_DAYS - 1, len(data) - 1)])
            if exit_p > entry:
                wins += 1
            total += 1
        except Exception:
            continue

        if total >= n_samples:
            break
        time.sleep(0.1)  # be polite to yfinance

    return round((wins / total) * 100, 2) if total > 0 else 50.0


# ── Module: Insider Backtest ──────────────────────────────────────────────────

def run_insider_backtest(date_start: date, date_end: date) -> list[dict]:
    """
    Fetch OpenInsider data and score trades for a given historical window.
    Because the scraper only fetches live data, we expand the lookback window
    by changing the URL parameters.
    """
    logger.info(f"=== INSIDER BACKTEST: {date_start} → {date_end} ===")

    # Fetch with extended window — scraper uses last 3 days by default,
    # so we temporarily override the URL to get a wider range.
    # Filing Date Range (fdr) uses MM/DD/YYYY format on OpenInsider
    fdr_str = f"{date_start.strftime('%m/%d/%Y')}+-+{date_end.strftime('%m/%d/%Y')}"
    
    url = (
        f"http://openinsider.com/screener?"
        f"s=&o=&pl=&ph=&ll=&lh=&fdr={fdr_str}&td=0&tdr=&fdlyl=&fdlyh=&daysago=&"
        f"xp=1&vl=10&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&"
        f"grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&"
        f"sortcol=0&cnt=500&action=1&type=csv"
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }

    # Fetch with retries
    try:
        import csv, io
        from tenacity import retry, stop_after_attempt, wait_fixed
        
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
        def fetch_with_retry():
            logger.info(f"Fetching from OpenInsider (attempt)...")
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.text

        resp_text = fetch_with_retry()
        raw_trades = []
        reader = csv.DictReader(io.StringIO(resp_text))
        for row in reader:
            try:
                from scraper import _parse_value, _parse_date
                trade_type = row.get("Trade Type", "").strip()
                if "P" not in trade_type:
                    continue
                trade_date = _parse_date(row.get("Trade Date", ""))
                # Filter to our window
                if not (date_start <= trade_date <= date_end):
                    continue
                raw_trades.append({
                    "ticker":       row.get("Ticker", "").strip(),
                    "company":      row.get("Company Name", "").strip(),
                    "insider_name": row.get("Insider Name", "").strip(),
                    "title":        row.get("Title", "").strip(),
                    "trade_date":   trade_date,
                    "shares":       _parse_value(row.get("Qty", "0")),
                    "price":        _parse_value(row.get("Price", "0")),
                    "value":        _parse_value(row.get("Value", "0")),
                })
            except Exception:
                continue
    except Exception as e:
        logger.error(f"Failed to fetch historical insider data: {e}")
        return []

    logger.info(f"Fetched {len(raw_trades)} raw trades in window")
    if not raw_trades:
        return []

    repeat_keys   = detect_repeat_buys(raw_trades)
    same_day_cnts = count_same_day_insiders(raw_trades)
    market_cache  = {}
    results       = []
    total_raw     = len(raw_trades)

    for i, trade in enumerate(raw_trades, 1):
        ticker = trade["ticker"]
        logger.info(f"[{i}/{total_raw}] Simulating {ticker} (entry: {trade['trade_date']})...")
        
        if ticker not in market_cache:
            market_cache[ticker] = get_market_data(ticker)
            time.sleep(0.5)   # gentle rate limit

        market = market_cache.get(ticker)
        if not market:
            continue

        is_repeat  = (ticker, trade["insider_name"]) in repeat_keys
        same_day   = same_day_cnts.get(ticker, 1)
        
        # Mastery Sync: Fetch historical SPY gap for the entry date
        spy_gap = get_spy_gap(trade["trade_date"] + timedelta(days=1))

        signal = score_trade(
            trade=trade,
            market=market,
            is_repeat=is_repeat,
            same_day_count=same_day,
            spy_gap_pct=spy_gap,
        )
        if not signal:
            continue

        result = simulate_trade(
            ticker=ticker,
            entry_price=signal["entry_price"],
            stop_loss=signal["stop_loss"],
            take_profit=signal["take_profit"],
            entry_date=trade["trade_date"],
        )
        if result:
            result["ticker"]  = ticker
            result["score"]   = signal["total_score"]
            result["variant"] = signal["variant"]
            results.append(result)
            logger.info(
                f"  {ticker:8s} | {result['outcome']:4s} | {result['return_pct']:+.1f}% "
                f"({result['exit_reason']}, {result['hold_days']}d)"
            )

    return results


# ── Module: Technical Backtest ────────────────────────────────────────────────

def run_technical_backtest(
    price_threshold: float,
    date_start: date,
    date_end: date,
) -> list[dict]:
    """
    Run the technical scanner and simulate each signal over the date window.
    Note: TradingView screener always returns *current* data. For true
    historical replay, we simulate the signals that come out of today's scan
    but price them with data from the historical period as a proxy analysis.
    """
    logger.info(f"=== TECHNICAL BACKTEST (≤${price_threshold}) {date_start} → {date_end} ===")

    signals = get_technical_signals(price_threshold=price_threshold)
    logger.info(f"Technical scanner returned {len(signals)} signals")

    if not signals:
        return []

    results = []
    for signal in signals:
        ticker      = signal["ticker"]
        exchange    = signal["exchange"]
        
        # Map exchange to Yahoo Finance suffix
        yf_ticker = ticker
        if exchange == "TSX":
            yf_ticker = f"{ticker}.TO"
        elif exchange == "TSXV":
            yf_ticker = f"{ticker}.V"

        result = simulate_trade(
            ticker=yf_ticker,
            entry_price=signal["entry_price"],
            stop_loss=signal["stop_loss"],
            take_profit=signal["take_profit"],
            entry_date=date_start,
            hold_days=DEFAULT_HOLD_DAYS,
        )
        if result:
            result["ticker"] = ticker
            result["score"]  = signal["total_score"]
            results.append(result)
            logger.info(
                f"  {signal['ticker']:8s} | {result['outcome']:4s} | {result['return_pct']:+.1f}% "
                f"({result['exit_reason']}, {result['hold_days']}d)"
            )

    return results


# ── Main Orchestrator ─────────────────────────────────────────────────────────

def run_backtest(module: str, start_str: str, end_str: str):
    date_start = datetime.strptime(start_str, "%Y-%m-%d").date()
    date_end   = datetime.strptime(end_str,   "%Y-%m-%d").date()

    # ── 1. Run the appropriate module ─────────────────────────────
    if module == "Insider":
        results = run_insider_backtest(date_start, date_end)
    elif module == "Technical_Under_5":
        results = run_technical_backtest(5.0, date_start, date_end)
    elif module == "Technical_Under_10":
        results = run_technical_backtest(10.0, date_start, date_end)
    elif module == "Technical_Under_20":
        results = run_technical_backtest(20.0, date_start, date_end)
    else:
        logger.error(f"Unknown module: {module}")
        return

    if not results:
        logger.warning("No trades were simulated — cannot compute metrics.")
        return

    # ── 2. Compute performance metrics ────────────────────────────
    metrics = compute_metrics(results, module, start_str, end_str)

    # NEW: Add individual results log
    metrics["simulation_results"] = format_results_table(results)

    # ── 3. Random control baseline ────────────────────────────────
    tickers = list({r["ticker"] for r in results})
    random_win_rate = run_random_control(tickers, date_start, date_end)
    metrics["random_control_win"] = random_win_rate
    metrics["edge_vs_random"]     = round(metrics["win_rate"] - random_win_rate, 2)
    metrics["control_comparison"] = (
        f"Strategy win rate {metrics['win_rate']:.1f}% vs "
        f"random baseline {random_win_rate:.1f}% "
        f"({'outperforms' if metrics['edge_vs_random'] > 0 else 'underperforms'} "
        f"by {abs(metrics['edge_vs_random']):.1f}pp)"
    )

    # ── 4. Attach configuration snapshot ──────────────────────────
    config = {
        "module":           module,
        "date_start":       start_str,
        "date_end":         end_str,
        "hold_days":        DEFAULT_HOLD_DAYS,
        "min_score_filter": "None (all scored signals)",
        "random_samples":   RANDOM_CONTROL_SAMPLES,
        "data_source":      "yfinance (post-entry simulation)",
    }
    metrics["configuration_snapshot"] = json.dumps(config, indent=2)

    # ── 5. Print summary ──────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"BACKTEST COMPLETE: {module}")
    logger.info(f"  Period        : {start_str} → {end_str}")
    logger.info(f"  Total Trades  : {metrics['total_trades']}")
    logger.info(f"  Win Rate      : {metrics['win_rate']:.1f}%")
    logger.info(f"  Random WR     : {random_win_rate:.1f}%  (edge: {metrics['edge_vs_random']:+.1f}pp)")
    logger.info(f"  Avg Return    : {metrics['average_return']:+.2f}%")
    logger.info(f"  Total Return  : {metrics['total_return']:+.1f}%")
    logger.info(f"  Sharpe Ratio  : {metrics['sharpe_ratio']:.3f}")
    logger.info(f"  Max Drawdown  : {metrics['max_drawdown']:.1f}%")
    logger.info(f"  Profit Factor : {metrics['profit_factor']}")
    logger.info("=" * 60)

    # ── 6. Push to Airtable ───────────────────────────────────────
    if os.getenv("AIRTABLE_TOKEN") and os.getenv("AIRTABLE_BASE_ID"):
        record_id = push_backtest_result(metrics)
        logger.info(f"✓ Pushed to Airtable Historical/Backtest → [{record_id}]")
    else:
        logger.warning("Airtable not configured - results NOT pushed. Set AIRTABLE_TOKEN and AIRTABLE_BASE_ID.")

    return metrics


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Replay historical stock signals and log performance metrics to Airtable."
    )
    parser.add_argument(
        "--module",
        required=True,
        choices=["Insider", "Technical_Under_5", "Technical_Under_10", "Technical_Under_20"],
        help="Which pipeline module to backtest.",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date in YYYY-MM-DD format (e.g. 2024-10-01)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date in YYYY-MM-DD format (e.g. 2025-03-01)",
    )
    args = parser.parse_args()

    run_backtest(
        module=args.module,
        start_str=args.start,
        end_str=args.end,
    )
