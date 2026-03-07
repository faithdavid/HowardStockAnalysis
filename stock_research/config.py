"""Configuration and constants for the stock research pipeline."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FilterConfig:
    """Insider trade and technical scan filter thresholds."""
    
    # Insider trade filters
    min_transaction_value: float = 50_000.0  # Minimum transaction value in USD
    min_shares: float = 100.0  # Minimum shares traded
    atr_window_min: float = 2.0  # Minimum ATR% for the symbol
    atr_window_max: float = 15.0  # Maximum ATR% for the symbol
    min_volume_sma_ratio: float = 1.2  # Volume must be >= 1.2x SMA20
    
    # Technical scan filters
    min_beta: float = 1.0  # Minimum beta threshold
    max_beta: float = 2.5  # Maximum beta threshold
    rsi_oversold_threshold: float = 30.0
    rsi_overbought_threshold: float = 70.0
    macd_signal_crossover: bool = True  # Trigger on MACD > Signal
    
    # Seasonal/timing filters
    avoid_earnings_days: int = 7  # Avoid trading N days before/after earnings
    season_based_stop_loss_pct: dict[str, float] = None  # e.g., {"spring": 0.05, "summer": 0.08}
    season_based_target_profit_pct: dict[str, float] = None
    
    def __post_init__(self):
        """Validate and set defaults for mutable fields."""
        if self.season_based_stop_loss_pct is None:
            object.__setattr__(self, 'season_based_stop_loss_pct', {
                "spring": 0.05,
                "summer": 0.08,
                "fall": 0.06,
                "winter": 0.07,
            })
        if self.season_based_target_profit_pct is None:
            object.__setattr__(self, 'season_based_target_profit_pct', {
                "spring": 0.10,
                "summer": 0.08,
                "fall": 0.12,
                "winter": 0.10,
            })


@dataclass(frozen=True)
class ScoringConfig:
    """Weights and parameters for MGPR (Momentum, Greed, Price, Risk) scoring."""
    
    # Insider signal scoring weights
    insider_strength_weight: float = 0.25
    volatility_weight: float = 0.25
    liquidity_weight: float = 0.25
    timing_weight: float = 0.25
    
    # MGPR technical scoring weights
    momentum_weight: float = 0.25  # RSI, MACD
    greed_weight: float = 0.25  # Volume surge, money flow
    price_weight: float = 0.25  # Price action, breakouts
    risk_weight: float = 0.25  # ATR, beta, drawdown
    
    # Score normalization (0-100)
    min_score_for_alert: float = 65.0
    min_score_for_auto_trade: float = 80.0


@dataclass(frozen=True)
class ApiConfig:
    """API endpoints and configurations."""
    
    # Polygon.io (market data)
    polygon_base_url: str = "https://api.polygon.io"
    polygon_api_key: Optional[str] = None
    
    # Airtable
    airtable_api_url: str = "https://api.airtable.com/v0"
    airtable_token: Optional[str] = None
    airtable_base_id: Optional[str] = None
    
    # OpenInsider scrape URL
    openinsider_url: str = "https://www.openinsider.com"
    
    # SEC EDGAR (fallback for insider trades)
    sec_edgar_url: str = "https://data.sec.gov"
    
    # Email/Slack/webhook configs
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    
    slack_webhook_url: Optional[str] = None
    zapier_webhook_url: Optional[str] = None
    tradingview_webhook_url: Optional[str] = None


def get_filter_config() -> FilterConfig:
    """Return singleton filter config (can be extended to load from env/file)."""
    return FilterConfig()


def get_scoring_config() -> ScoringConfig:
    """Return singleton scoring config."""
    return ScoringConfig()


def get_api_config() -> ApiConfig:
    """Return singleton API config (loads from env vars if available)."""
    import os
    return ApiConfig(
        polygon_api_key=os.getenv("POLYGON_API_KEY"),
        airtable_token=os.getenv("AIRTABLE_TOKEN"),
        airtable_base_id=os.getenv("AIRTABLE_BASE_ID"),
        smtp_host=os.getenv("SMTP_HOST"),
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        smtp_user=os.getenv("SMTP_USER"),
        smtp_password=os.getenv("SMTP_PASSWORD"),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        zapier_webhook_url=os.getenv("ZAPIER_WEBHOOK_URL"),
        tradingview_webhook_url=os.getenv("TRADINGVIEW_WEBHOOK_URL"),
    )
