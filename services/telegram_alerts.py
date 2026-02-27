"""Telegram spread-alert notifications — completely isolated module.

Reads already-computed spread rows (produced by the aggregator).
Never calls exchange APIs.  Never touches authentication or API routes.
Uses a daemon thread for the HTTP call so the caller is never blocked.

Environment variables
---------------------
TELEGRAM_BOT_TOKEN  – Bot token obtained from @BotFather.  Required; module
                      is silently disabled when absent.
TELEGRAM_CHAT_ID    – Chat / channel ID to deliver messages to.  Required.

Optional tuning via environment variables (with hardcoded defaults):
MIN_SPREAD_ALERT        – Minimum spread (fraction) to include in alert.  Default 0.02.
MAX_ALERT_ROWS          – Max rows per message.  Default 5.
ALERT_COOLDOWN_SECONDS  – Minimum seconds between alerts.  Default 60.
"""
import logging
import os
import threading
import time

logger = logging.getLogger("arb_dashboard")

# ---------------------------------------------------------------------------
# Configurable constants (overridable via environment variables)
# ---------------------------------------------------------------------------
MIN_SPREAD_ALERT: float = 0.02
MAX_ALERT_ROWS: int = 5
ALERT_COOLDOWN_SECONDS: int = 60

try:
    MIN_SPREAD_ALERT = float(os.environ.get("MIN_SPREAD_ALERT", "0.02"))
except (ValueError, TypeError):
    logger.warning("[Telegram] Invalid MIN_SPREAD_ALERT env value; using default 0.02")

try:
    MAX_ALERT_ROWS = int(os.environ.get("MAX_ALERT_ROWS", "5"))
except (ValueError, TypeError):
    logger.warning("[Telegram] Invalid MAX_ALERT_ROWS env value; using default 5")

try:
    ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "60"))
except (ValueError, TypeError):
    logger.warning("[Telegram] Invalid ALERT_COOLDOWN_SECONDS env value; using default 60")

# ---------------------------------------------------------------------------
# Internal cooldown state
# ---------------------------------------------------------------------------
_last_alert_ts: float = 0.0
_cooldown_lock: threading.Lock = threading.Lock()


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    """Synchronous HTTP POST to Telegram Bot API.  Runs in a daemon thread."""
    try:
        import httpx  # already in requirements — no new dependency
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json={"chat_id": chat_id, "text": text})
        if not resp.is_success:
            logger.warning("[Telegram] sendMessage failed (%s): %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        logger.warning("[Telegram] HTTP request error: %s", exc)


def send_spread_alerts(rows: list) -> None:
    """Filter *rows* by threshold, format and send a single Telegram alert.

    Safe to call from any context:
    * If TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID are absent → returns immediately.
    * If cooldown has not elapsed → returns immediately.
    * HTTP is dispatched to a daemon thread → never blocks the caller.
    * All exceptions are caught internally → never raises.

    Parameters
    ----------
    rows:
        List of spread-row dicts as produced by the aggregator
        (fields: symbol, buy_ex, buy_ask, sell_ex, sell_bid, spread, …).
    """
    global _last_alert_ts

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return

    with _cooldown_lock:
        now = time.time()
        if now - _last_alert_ts < ALERT_COOLDOWN_SECONDS:
            return
        _last_alert_ts = now

    try:
        filtered = [
            r for r in rows
            if float(r.get("spread") or 0.0) >= MIN_SPREAD_ALERT
        ]
        if not filtered:
            return

        filtered.sort(key=lambda r: float(r.get("spread") or 0.0), reverse=True)
        top = filtered[:MAX_ALERT_ROWS]

        lines = ["🔥 Arbitrage Alert"]
        for r in top:
            symbol = r.get("symbol") or r.get("pair_key", "?")
            buy_ex = r.get("buy_ex", "?")
            sell_ex = r.get("sell_ex", "?")
            spread = float(r.get("spread") or 0.0)
            lines.append("")
            lines.append(str(symbol))
            buy_price = r.get("buy_ask") or r.get("buy_price")
            sell_price = r.get("sell_bid") or r.get("sell_price")
            if buy_price is not None:
                try:
                    lines.append(f"Buy: {buy_ex} @ {float(buy_price):.6g}")
                except Exception:
                    lines.append(f"Buy: {buy_ex}")
            else:
                lines.append(f"Buy: {buy_ex}")
            if sell_price is not None:
                try:
                    lines.append(f"Sell: {sell_ex} @ {float(sell_price):.6g}")
                except Exception:
                    lines.append(f"Sell: {sell_ex}")
            else:
                lines.append(f"Sell: {sell_ex}")
            lines.append(f"Spread: {spread * 100:.2f}%")

        text = "\n".join(lines)
        threading.Thread(
            target=_send_telegram,
            args=(token, chat_id, text),
            daemon=True,
        ).start()
    except Exception as exc:
        logger.warning("[Telegram] send_spread_alerts error: %s", exc)
