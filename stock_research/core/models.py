from datetime import date, datetime
from pydantic import BaseModel


class Symbol(BaseModel):
    ticker: str
    exchange: str
    active: bool = True


class Bar(BaseModel):
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int


class TechnicalScore(BaseModel):
    symbol: str
    date: date
    score: float
    drivers_positive: list[str]
    drivers_negative: list[str]


class InsiderTrade(BaseModel):
    symbol: str
    insider_name: str
    trade_date: date
    transaction_type: str
    shares: float
    price: float
    value: float


class InsiderSignal(BaseModel):
    symbol: str
    signal_date: date
    score: float
    rationale: str


class RunReport(BaseModel):
    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str
    message: str | None = None

