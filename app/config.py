"""Configuration: all environment-based settings and constants."""
import json
import os
import sys
from typing import Any, Dict


def app_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    # app/ is one level below the project root
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


BASE_DIR = app_dir()
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
LOGOS_DIR = os.path.join(ASSETS_DIR, "logos")
SOUNDS_DIR = os.path.join(ASSETS_DIR, "sounds")
CONFIG_PATH = os.path.join(BASE_DIR, "arb_dashboard_config.json")
AUTH_KEY_PATH = os.path.join(BASE_DIR, "auth_secret.key")
USERS_DB_PATH = os.path.join(BASE_DIR, "users.db.enc")

REFRESH_SEC = int(os.getenv("REFRESH_SEC", "3"))
CYCLE_WARN_MS = 2000
COLLECTOR_ONLY: bool = os.getenv("COLLECTOR_ONLY") == "1"
DEFAULT_MIN_VOL_USD = 5_000_000.0
DEFAULT_MIN_SPREAD = 0.0
HTTP_TIMEOUT = 12
INTERVAL_FETCH_TIMEOUT = 5
MAX_BINGX_SYMBOLS = 260
BINGX_CONCURRENCY = 8
DEFAULT_EXCH_ENABLED: Dict[str, bool] = {"MEXC": True, "Bybit": True, "BingX": True}
MAX_FREE_SPREAD = 0.02
SESSION_TTL_SEC = 7 * 24 * 3600
PBKDF2_ITERS = 100_000

TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_USERNAME: str = os.getenv("TELEGRAM_BOT_USERNAME", "arbitrageinsights_bot").lstrip("@")
_TG_API = "https://api.telegram.org/bot{token}/{method}"

# Exchange API endpoints
MEXC_TICKERS = "https://contract.mexc.com/api/v1/contract/ticker"
MEXC_CONTRACT_DETAIL = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_FUNDING_RATE_BTC = "https://contract.mexc.com/api/v1/contract/funding_rate/BTC_USDT"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"
BYBIT_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info"
BINGX_CONTRACTS = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
BINGX_BOOK_TICKER = "https://open-api.bingx.com/openApi/swap/v2/quote/bookTicker"
BINGX_TICKER_24H = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
BINGX_PREMIUM_INDEX = "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex"
BINGX_FUNDING_RATE = "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate"

TIMESTAMP_MS_THRESHOLD = 1e12
MEXC_FUNDING_CACHE_TTL_SEC = 60
MEXC_INTERVALS_TTL = 21600
BYBIT_INST_TTL = 3600
BINGX_CONTRACTS_TTL = 3600


def load_config() -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "refresh_sec": REFRESH_SEC,
        "min_vol": DEFAULT_MIN_VOL_USD,
        "min_spread": DEFAULT_MIN_SPREAD,
        "enabled": dict(DEFAULT_EXCH_ENABLED),
    }
    if not os.path.exists(CONFIG_PATH):
        return defaults
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            loaded = json.load(fh)
        if not isinstance(loaded, dict):
            return defaults
        defaults.update(loaded)
        enabled = defaults.get("enabled", {})
        defaults["enabled"] = {
            "MEXC": bool(enabled.get("MEXC", True)),
            "Bybit": bool(enabled.get("Bybit", True)),
            "BingX": bool(enabled.get("BingX", True)),
        }
        return defaults
    except Exception:
        return defaults


def save_config(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)


# Mutable live config dict — shared across the whole process
CFG: Dict[str, Any] = load_config()
