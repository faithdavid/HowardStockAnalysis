from __future__ import annotations

import logging
import json
from datetime import datetime, timezone, date
from uuid import uuid4
from pathlib import Path
from typing import Optional

from stock_research.connectors.airtable import AirtableClient, AirtableConfig
from stock_research.settings import settings
from stock_research.processors.insider_processor import InsiderProcessor
from stock_research.processors.market_data_handler import MarketDataHandler
from stock_research.processors.technical_scanner import TechnicalScanner
from stock_research.processors.backtest import Backtester
from stock_research.config import get_api_config

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def write_run_report(status: str, message: str | None = None) -> str:
    """Write a run report to Airtable documenting pipeline execution."""
    if not settings.airtable_token or not settings.airtable_base_id:
        raise RuntimeError("AIRTABLE_TOKEN and AIRTABLE_BASE_ID must be set in .env")

    client = AirtableClient(
        AirtableConfig(token=settings.airtable_token, base_id=settings.airtable_base_id)
    )

    run_id = uuid4().hex
    fields = {
        "RunID": run_id,
        "StartedAt": utcnow().isoformat(),
        "Status": status,
        "Message": message or "",
    }

    client.create_record(settings.airtable_table_runreports, fields)
    return run_id


class PipelineRunner:
    """Orchestrate the full stock research pipeline: insider trades, technical analysis, backtesting."""
    
    def __init__(self):
        self.insider_processor = InsiderProcessor()
        self.market_handler = MarketDataHandler()
        self.technical_scanner = TechnicalScanner()
        self.backtester = Backtester()
        self.airtable_client = None
        
        api_cfg = get_api_config()
        if settings.airtable_token and settings.airtable_base_id:
            self.airtable_client = AirtableClient(
                AirtableConfig(
                    token=settings.airtable_token,
                    base_id=settings.airtable_base_id,
                )
            )
    
    def run_insider_scan(
        self,
        tickers: list[str],
        days_back: int = 30,
    ) -> dict:
        """
        Run insider trade scan and score on a list of tickers.
        
        Args:
            tickers: List of stock symbols to scan
            days_back: How many days back to look for insider trades
        
        Returns:
            Dict with results and statistics
        """
        logger.info(f"Starting insider scan for {len(tickers)} tickers")
        
        all_trades = []
        for ticker in tickers:
            try:
                trades = self.insider_processor.fetch_insider_trades_openinsider(
                    ticker, days_back=days_back
                )
                all_trades.extend(trades)
            except Exception as e:
                logger.warning(f"Failed to fetch insider trades for {ticker}: {e}")
                continue
        
        if not all_trades:
            logger.warning("No insider trades found")
            return {"trades_found": 0, "signals": []}
        
        filtered_trades = self.insider_processor.filter_insider_trades(all_trades)
        
        # Simulate market data (in production, fetch from Polygon)
        atr_pct = {t.ticker: 4.5 for t in filtered_trades}
        vol_ratio = {t.ticker: 1.5 for t in filtered_trades}
        
        signals = self.insider_processor.score_insider_trades(
            filtered_trades,
            atr_pct,
            vol_ratio,
        )
        
        logger.info(f"Insider scan complete: {len(signals)} signals generated")
        return {
            "scan_date": date.today().isoformat(),
            "trades_found": len(all_trades),
            "trades_filtered": len(filtered_trades),
            "signals": [s.model_dump() for s in signals],
        }
    
    def run_technical_scan(
        self,
        tickers: list[str],
    ) -> dict:
        """
        Run technical indicator scan on a list of tickers.
        
        Args:
            tickers: List of stock symbols to scan
        
        Returns:
            Dict with technical scores and statistics
        """
        logger.info(f"Starting technical scan for {len(tickers)} tickers")
        
        # Placeholder: in production, fetch real OHLCV from Polygon
        ohlcv_data = {}
        
        try:
            scores = self.technical_scanner.screen_symbols(tickers, ohlcv_data)
            logger.info(f"Technical scan complete: {len(scores)} scores generated")
            return {
                "scan_date": date.today().isoformat(),
                "tickers_scanned": len(tickers),
                "scores": [s.model_dump() for s in scores],
            }
        except Exception as e:
            logger.error(f"Technical scan failed: {e}")
            return {"error": str(e)}
    
    def save_results_to_json(
        self,
        results: dict,
        output_dir: str = "./output",
    ) -> str:
        """
        Save pipeline results to JSON file for later Airtable import.
        
        Args:
            results: Results dict from run_insider_scan, etc.
            output_dir: Directory to save files
        
        Returns:
            Path to saved file
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        filename = f"pipeline_results_{date.today().isoformat()}.json"
        filepath = output_path / filename
        
        with open(filepath, "w") as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Results saved to {filepath}")
        return str(filepath)
    
    def push_results_to_airtable(
        self,
        signals: list[dict],
        table_name: str = "TechnicalScans",
    ) -> bool:
        """
        Push scored signals to an Airtable table.
        
        Args:
            signals: List of signal dicts to push
            table_name: Name of the Airtable table
        
        Returns:
            True if successful, False otherwise
        """
        if not self.airtable_client:
            logger.warning("Airtable client not configured; skipping push")
            return False
        
        try:
            for signal in signals:
                # Map signal fields to Airtable field names
                fields = {
                    "Symbol": signal.get("symbol"),
                    "Score": signal.get("score"),
                    "Rationale": signal.get("rationale"),
                    "ScanDate": signal.get("signal_date") or date.today().isoformat(),
                }
                self.airtable_client.create_record(table_name, fields)
            
            logger.info(f"Pushed {len(signals)} signals to Airtable table '{table_name}'")
            return True
        
        except Exception as e:
            logger.error(f"Failed to push to Airtable: {e}")
            return False
