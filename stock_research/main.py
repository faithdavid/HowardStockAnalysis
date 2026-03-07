"""Main entry point for the stock research pipeline."""

import logging
from stock_research.settings import configure_logging
from stock_research.pipelines.run_reports import PipelineRunner, write_run_report

logger = logging.getLogger(__name__)


def main():
    """Run the stock research pipeline: insider scans, technical analysis, results to Airtable."""
    configure_logging()
    
    logger.info("=" * 70)
    logger.info("STOCK RESEARCH PIPELINE STARTING")
    logger.info("=" * 70)
    
    try:
        # Log pipeline start to Airtable
        run_id = write_run_report("started", "Insider and technical scans in progress")
        logger.info(f"Pipeline run ID: {run_id}")
        
        # Initialize pipeline runner
        runner = PipelineRunner()
        
        # Example: scan a few tickers for insider trades
        tickers_to_scan = ["AAPL", "MSFT", "NVDA", "TSLA"]
        
        logger.info(f"Scanning {len(tickers_to_scan)} tickers for insider activity...")
        insider_results = runner.run_insider_scan(tickers_to_scan, days_back=30)
        
        # Save results to JSON
        runner.save_results_to_json(insider_results, output_dir="./output")
        
        # Push insider signals to Airtable (if any found and if configured)
        if insider_results.get("signals"):
            runner.push_results_to_airtable(
                insider_results["signals"],
                table_name="InsiderSignals",
            )
        
        # Log completion
        write_run_report(
            "completed",
            f"Processed {insider_results.get('trades_found', 0)} trades, "
            f"generated {len(insider_results.get('signals', []))} signals",
        )
        
        logger.info("=" * 70)
        logger.info("STOCK RESEARCH PIPELINE COMPLETE")
        logger.info("=" * 70)
        
        return insider_results
    
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        write_run_report("failed", f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
