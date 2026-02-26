"""Data models for the arbitrage dashboard."""
from dataclasses import dataclass


@dataclass
class MarketRow:
    """Single exchange's market data for one symbol."""

    exchange: str
    bid: float
    ask: float
    last: float
    vol24_usd: float
    fund_rate: float
    fund24_est: float
    url: str
    next_funding_ts: float
    funding_interval_h: int
