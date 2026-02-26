"""Authentication: RSA key pair, PBKDF2 hashing, Fernet user DB, session tokens, Telegram."""
import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import time
from typing import Any, Dict, Optional, Tuple

import httpx
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from app.config import (
    AUTH_KEY_PATH,
    PBKDF2_ITERS,
    SESSION_TTL_SEC,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_BOT_USERNAME,
    USERS_DB_PATH,
    _TG_API,
)

logger = logging.getLogger("arb_dashboard")


# ---------------------------------------------------------------------------
# Auth key management
# ---------------------------------------------------------------------------

def _get_or_create_auth_key() -> bytes:
    env_key = os.environ.get("ARB_AUTH_KEY")
    if env_key:
        return env_key.encode("utf-8")
    if os.path.exists(AUTH_KEY_PATH):
        with open(AUTH_KEY_PATH, "rb") as fh:
            return fh.read().strip()
    key = Fernet.generate_key()
    with open(AUTH_KEY_PATH, "wb") as fh:
        fh.write(key)
    return key


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def _hash_password(password: str, salt_b64: str, iters: int = PBKDF2_ITERS) -> str:
    salt = base64.b64decode(salt_b64.encode("utf-8"))
    raw = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
    return base64.b64encode(raw).decode("utf-8")


def _make_password_record(password: str) -> Tuple[str, str]:
    salt_b64 = base64.b64encode(secrets.token_bytes(16)).decode("utf-8")
    return salt_b64, _hash_password(password, salt_b64, PBKDF2_ITERS)


def _verify_password(password: str, salt_b64: str, expected_hash: str, iters: int = PBKDF2_ITERS) -> bool:
    return secrets.compare_digest(_hash_password(password, salt_b64, iters), expected_hash)


# ---------------------------------------------------------------------------
# Username / Telegram helpers
# ---------------------------------------------------------------------------

def _normalize_username(username: str) -> str:
    return "".join(ch for ch in (username or "").strip().lower() if ch.isalnum() or ch in "._-")[:32]


def _normalize_tg_username(raw: str) -> str:
    stripped = (raw or "").strip().lstrip("@")
    return "".join(ch for ch in stripped.lower() if ch.isalnum() or ch == "_")[:32]


def _tg_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ---------------------------------------------------------------------------
# Telegram API
# ---------------------------------------------------------------------------

async def _tg_send(chat_id: Any, text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        logger.debug("TELEGRAM_BOT_TOKEN not set — skipping tg_send")
        return False
    url = _TG_API.format(token=TELEGRAM_BOT_TOKEN, method="sendMessage")
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
            if resp.status_code != 200:
                logger.warning("tg_send failed: status=%s body=%s", resp.status_code, resp.text[:200])
                return False
        return True
    except Exception as exc:
        logger.warning("tg_send exception: %s", exc)
        return False


async def _tg_resolve_chat_id(tg_username: str) -> Optional[int]:
    if not TELEGRAM_BOT_TOKEN or not tg_username:
        return None
    url = _TG_API.format(token=TELEGRAM_BOT_TOKEN, method="getChat")
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(url, json={"chat_id": f"@{tg_username}"})
            data = resp.json()
            if data.get("ok"):
                return int(data["result"]["id"])
    except Exception as exc:
        logger.debug("tg_resolve_chat_id(%s) failed: %s", tg_username, exc)
    return None


# ---------------------------------------------------------------------------
# RSA encryption (client → server password transport)
# ---------------------------------------------------------------------------

AUTH_CIPHER = Fernet(_get_or_create_auth_key())
RSA_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
RSA_PUBLIC_PEM = RSA_PRIVATE_KEY.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode("utf-8")


def _decrypt_client_field(value: str) -> str:
    if not isinstance(value, str) or not value:
        return ""
    decoded = base64.b64decode(value.encode("utf-8"))
    plain = RSA_PRIVATE_KEY.decrypt(
        decoded,
        padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None),
    )
    return plain.decode("utf-8")


def _extract_auth_credentials(payload: Dict[str, Any]) -> Tuple[str, str, str]:
    username = _normalize_username(str(payload.get("username") or ""))
    password = str(payload.get("password") or "")
    tg_username = _normalize_tg_username(str(payload.get("tg_username") or ""))
    return username, password, tg_username


def _do_login_verify(payload: Dict[str, Any], users_snapshot: Dict[str, Any]) -> Optional[Tuple[str, str, str, bool]]:
    """Run in a thread: RSA decrypt + PBKDF2 verify in ONE call.
    Returns (username, password, tg, needs_hash_upgrade) or None if invalid credentials."""
    username, password, tg = _extract_auth_credentials(payload)
    user = users_snapshot.get(username)
    if not user:
        return None
    stored_iters = int(user.get("pbkdf2_iters", 250_000))
    if not _verify_password(password, user.get("salt", ""), user.get("password_hash", ""), stored_iters):
        return None
    needs_upgrade = stored_iters != PBKDF2_ITERS
    return username, password, tg, needs_upgrade


# ---------------------------------------------------------------------------
# User database (Fernet-encrypted JSON)
# ---------------------------------------------------------------------------

def _save_users(users: Dict[str, Any]) -> None:
    raw = json.dumps(users, ensure_ascii=False).encode("utf-8")
    token = AUTH_CIPHER.encrypt(raw)
    with open(USERS_DB_PATH, "wb") as fh:
        fh.write(token)


def _seed_admin(users: Dict[str, Any]) -> None:
    if "admin" not in users:
        salt, pwh = _make_password_record("salimonenkodima")
        users["admin"] = {
            "username": "admin",
            "salt": salt,
            "password_hash": pwh,
            "pbkdf2_iters": PBKDF2_ITERS,
            "is_admin": True,
            "subscription_approved": True,
            "created_at": int(time.time()),
        }
    if "adminegor" not in users:
        salt2, pwh2 = _make_password_record("egorkorotkov96!")
        users["adminegor"] = {
            "username": "adminegor",
            "salt": salt2,
            "password_hash": pwh2,
            "pbkdf2_iters": PBKDF2_ITERS,
            "is_admin": True,
            "subscription_approved": True,
            "created_at": int(time.time()),
        }


def _load_users() -> Dict[str, Any]:
    users: Dict[str, Any] = {}
    if os.path.exists(USERS_DB_PATH):
        try:
            with open(USERS_DB_PATH, "rb") as fh:
                users = json.loads(AUTH_CIPHER.decrypt(fh.read()).decode("utf-8"))
        except Exception:
            users = {}
    _seed_admin(users)
    _save_users(users)
    return users


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

USERS: Dict[str, Any] = _load_users()
USERS_LOCK = asyncio.Lock()
SESSIONS: Dict[str, Dict[str, Any]] = {}
_BOT_CHECK_RL: Dict[str, int] = {}
_TG_LINK_CODES: Dict[str, Dict[str, Any]] = {}
_LOGIN_FAIL: Dict[str, int] = {}
_AUTH_RATE: Dict[str, int] = {}


def _make_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = {"username": username, "expires": time.time() + SESSION_TTL_SEC}
    import app as _app_pkg
    redis = _app_pkg._REDIS
    if redis is not None:
        try:
            asyncio.get_running_loop().create_task(
                redis.setex(f"arb:sess:{token}", SESSION_TTL_SEC, username)
            )
        except RuntimeError:
            pass
    return token


def _is_subscription_active(user: Dict[str, Any]) -> bool:
    if not user.get("subscription_approved"):
        return False
    expires_at = user.get("subscription_expires")
    if expires_at is not None:
        try:
            if time.time() > float(expires_at):
                user["subscription_approved"] = False
                user["subscription_expires"] = None
                return False
        except (TypeError, ValueError):
            pass
    return True


def _session_user(request: Any) -> Optional[Dict[str, Any]]:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:]
    rec = SESSIONS.get(token)
    if not rec:
        return None
    if rec["expires"] < time.time():
        SESSIONS.pop(token, None)
        return None
    user = USERS.get(rec["username"])
    if not user:
        return None
    return user


async def _session_user_async(request: Any) -> Optional[Dict[str, Any]]:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:]
    rec = SESSIONS.get(token)
    if rec:
        if rec["expires"] < time.time():
            SESSIONS.pop(token, None)
        else:
            return USERS.get(rec["username"])
    import app as _app_pkg
    redis = _app_pkg._REDIS
    if redis is not None:
        try:
            username = await redis.get(f"arb:sess:{token}")
            if username:
                SESSIONS[token] = {"username": username, "expires": time.time() + SESSION_TTL_SEC}
                return USERS.get(username)
        except Exception:
            pass
    return None


def _limit_rows_for_access(
    rows: Any, user: Optional[Dict[str, Any]]
) -> Tuple[Any, Optional[float], bool, bool]:
    from app.config import MAX_FREE_SPREAD
    is_admin = bool(user and user.get("is_admin"))
    is_logged = bool(user)
    is_paid = bool(user and _is_subscription_active(user))
    spread_limit: Optional[float] = None
    if not is_logged:
        spread_limit = MAX_FREE_SPREAD
        rows = [r for r in rows if float(r.get("spread") or 0.0) <= spread_limit]
    return rows, spread_limit, is_admin, is_paid


def _get_client_ip(request: Any) -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _rl_check(store: Dict[str, int], key: str, limit: int) -> bool:
    store[key] = store.get(key, 0) + 1
    for k in list(store):
        if k != key:
            store.pop(k, None)
    return store[key] <= limit
