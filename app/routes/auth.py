"""Auth routes: /api/auth/*, /api/admin/*, /api/bot/*, /api/user/*"""
import asyncio
import logging
import secrets
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.auth import (
    SESSIONS,
    USERS,
    USERS_LOCK,
    _AUTH_RATE,
    _BOT_CHECK_RL,
    _LOGIN_FAIL,
    _TG_LINK_CODES,
    _do_login_verify,
    _extract_auth_credentials,
    _get_client_ip,
    _is_subscription_active,
    _make_password_record,
    _make_session,
    _normalize_tg_username,
    _normalize_username,
    _rl_check,
    _save_users,
    _session_user,
    _tg_escape,
    _tg_resolve_chat_id,
    _tg_send,
    AUTH_CIPHER,
    PBKDF2_ITERS,
    RSA_PUBLIC_PEM,
)
from app.config import TELEGRAM_BOT_USERNAME

logger = logging.getLogger("arb_dashboard")
router = APIRouter()


# ---------------------------------------------------------------------------
# Background helpers
# ---------------------------------------------------------------------------

async def _resolve_and_store_tg_chat_id(username: str, tg_username: str) -> None:
    chat_id = await _tg_resolve_chat_id(tg_username)
    if chat_id is not None:
        async with USERS_LOCK:
            if username in USERS:
                USERS[username]["tg_chat_id"] = chat_id
                await asyncio.to_thread(_save_users, USERS)
        logger.info("Resolved Telegram chat_id=%s for user=%s (@%s)", chat_id, username, tg_username)


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@router.get("/api/auth/pubkey")
async def api_auth_pubkey():
    return JSONResponse({"public_key": RSA_PUBLIC_PEM})


@router.post("/api/auth/register")
async def api_auth_register(request: Request, payload: Dict[str, Any]):
    ip = _get_client_ip(request)
    if not _rl_check(_AUTH_RATE, f"{ip}:{int(time.time() // 60)}:reg", 10):
        return JSONResponse({"ok": False, "error": "too_many_requests"}, status_code=429)

    username, password, tg_username = await asyncio.to_thread(
        _extract_auth_credentials, payload
    )

    if len(username) < 3 or len(password) < 6:
        return JSONResponse({"ok": False, "error": "invalid_credentials"}, status_code=400)

    async with USERS_LOCK:
        if username in USERS:
            return JSONResponse({"ok": False, "error": "user_exists"}, status_code=400)
        salt, pwh = await asyncio.to_thread(_make_password_record, password)
        USERS[username] = {
            "username": username,
            "salt": salt,
            "password_hash": pwh,
            "pbkdf2_iters": PBKDF2_ITERS,
            "is_admin": False,
            "subscription_approved": False,
            "tg_username": tg_username,
            "tg_chat_id": None,
            "created_at": int(time.time()),
        }
        await asyncio.to_thread(_save_users, USERS)

    if tg_username:
        asyncio.create_task(_resolve_and_store_tg_chat_id(username, tg_username))

    return JSONResponse({"ok": True})


@router.post("/api/auth/login")
async def api_auth_login(request: Request, payload: Dict[str, Any]):
    ip = _get_client_ip(request)
    minute_key = f"{ip}:{int(time.time() // 60)}"

    if not _rl_check(_AUTH_RATE, f"{minute_key}:login", 20):
        return JSONResponse({"ok": False, "error": "too_many_requests"}, status_code=429)

    fail_key = f"fail:{ip}:{int(time.time() // 60)}"
    if _LOGIN_FAIL.get(fail_key, 0) >= 5:
        return JSONResponse({"ok": False, "error": "too_many_failures"}, status_code=429)

    result = await asyncio.to_thread(_do_login_verify, payload, dict(USERS))
    if result is None:
        _LOGIN_FAIL[fail_key] = _LOGIN_FAIL.get(fail_key, 0) + 1
        for k in list(_LOGIN_FAIL):
            if k != fail_key:
                _LOGIN_FAIL.pop(k, None)
        return JSONResponse({"ok": False, "error": "bad_login"}, status_code=401)

    username, password, _tg, needs_upgrade = result

    _LOGIN_FAIL.pop(fail_key, None)

    if needs_upgrade:
        async def _upgrade_hash() -> None:
            new_salt, new_hash = await asyncio.to_thread(_make_password_record, password)
            async with USERS_LOCK:
                if username in USERS:
                    USERS[username]["salt"] = new_salt
                    USERS[username]["password_hash"] = new_hash
                    USERS[username]["pbkdf2_iters"] = PBKDF2_ITERS
                    await asyncio.to_thread(_save_users, USERS)
        asyncio.create_task(_upgrade_hash())

    user = USERS.get(username, {})
    token = _make_session(username)
    return JSONResponse(
        {
            "ok": True,
            "token": token,
            "user": {
                "username": user["username"],
                "is_admin": bool(user.get("is_admin")),
                "subscription_approved": _is_subscription_active(user),
                "subscription_expires": user.get("subscription_expires"),
                "tg_username": user.get("tg_username") or "",
                "tg_chat_id": user.get("tg_chat_id"),
            },
        }
    )


@router.get("/api/auth/me")
async def api_auth_me(request: Request):
    user = _session_user(request)
    if not user:
        return JSONResponse({"ok": False, "user": None}, status_code=401)
    return JSONResponse(
        {
            "ok": True,
            "user": {
                "username": user["username"],
                "is_admin": bool(user.get("is_admin")),
                "subscription_approved": _is_subscription_active(user),
                "subscription_expires": user.get("subscription_expires"),
                "tg_username": user.get("tg_username") or "",
                "tg_chat_id": user.get("tg_chat_id"),
            },
        }
    )


@router.post("/api/auth/logout")
async def api_auth_logout(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        SESSIONS.pop(token, None)
        import app as _app_pkg
        redis = _app_pkg._REDIS
        if redis is not None:
            try:
                await redis.delete(f"arb:sess:{token}")
            except Exception:
                pass
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

@router.get("/api/admin/users")
async def api_admin_users(request: Request):
    user = _session_user(request)
    if not user or not user.get("is_admin"):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    items = []
    for u in USERS.values():
        items.append({
            "username": u.get("username"),
            "is_admin": bool(u.get("is_admin")),
            "subscription_approved": _is_subscription_active(u),
            "subscription_expires": u.get("subscription_expires"),
            "tg_username": u.get("tg_username") or "",
            "tg_chat_id": u.get("tg_chat_id"),
            "created_at": u.get("created_at"),
        })
    items.sort(key=lambda x: (not x["is_admin"], x["username"]))
    return JSONResponse({"ok": True, "users": items})


@router.post("/api/admin/subscription")
async def api_admin_subscription(request: Request, payload: Dict[str, Any]):
    admin = _session_user(request)
    if not admin or not admin.get("is_admin"):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    username = _normalize_username(str(payload.get("username") or ""))
    approved = bool(payload.get("approved"))
    VALID_DAYS = {0, 30, 60, 90, 180, 365}
    try:
        days = int(payload.get("days") or 0)
    except (TypeError, ValueError):
        days = 0
    if days not in VALID_DAYS:
        days = 0
    if not username or username not in USERS:
        return JSONResponse({"ok": False, "error": "user_not_found"}, status_code=404)
    if USERS[username].get("is_admin"):
        return JSONResponse({"ok": False, "error": "cant_change_admin"}, status_code=400)
    expires_at: Optional[float] = (time.time() + days * 86400) if (approved and days > 0) else None
    async with USERS_LOCK:
        USERS[username]["subscription_approved"] = approved
        USERS[username]["subscription_expires"] = expires_at
        _save_users(USERS)
        chat_id = USERS[username].get("tg_chat_id")
        tg_user = USERS[username].get("tg_username") or ""

    if chat_id or tg_user:
        safe_bot = _tg_escape(TELEGRAM_BOT_USERNAME)
        if approved:
            period_str = f" на {days} дней" if days else " (бессрочно)"
            msg = (
                f"✅ <b>Подписка активирована{period_str}!</b>\n"
                f"Теперь вы можете видеть все спреды на сайте.\n"
                f"🤖 Бот @{safe_bot} активен для вашего аккаунта."
            )
        else:
            msg = (
                f"❌ <b>Подписка отключена.</b>\n"
                f"Доступ ограничен до спредов ≤2%.\n"
                f"🤖 Бот @{safe_bot} приостановлен."
            )
        target = chat_id or f"@{tg_user}"
        asyncio.create_task(_tg_send(target, msg))

    return JSONResponse({"ok": True, "expires_at": expires_at})


@router.post("/api/admin/delete-user")
async def api_admin_delete_user(request: Request, payload: Dict[str, Any]):
    admin = _session_user(request)
    if not admin or not admin.get("is_admin"):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    username = _normalize_username(str(payload.get("username") or ""))
    if not username or username not in USERS:
        return JSONResponse({"ok": False, "error": "user_not_found"}, status_code=404)
    if USERS[username].get("is_admin"):
        return JSONResponse({"ok": False, "error": "cant_delete_admin"}, status_code=400)
    if username == admin.get("username"):
        return JSONResponse({"ok": False, "error": "cant_delete_self"}, status_code=400)
    user_rec = USERS[username]
    chat_id = user_rec.get("tg_chat_id")
    tg_user = user_rec.get("tg_username") or ""
    if chat_id or tg_user:
        msg = "⚠️ <b>Ваш аккаунт был удалён администратором.</b>"
        target = chat_id or f"@{tg_user}"
        asyncio.create_task(_tg_send(target, msg))
    async with USERS_LOCK:
        USERS.pop(username, None)
        await asyncio.to_thread(_save_users, USERS)
    logger.info("Admin %s deleted user %s", admin.get("username"), username)
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Bot / user link routes
# ---------------------------------------------------------------------------

@router.get("/api/bot/check-subscription")
async def api_bot_check_subscription(request: Request):
    client_ip = request.client.host if request.client else "unknown"
    now_min = int(time.time() // 60)
    rl_key = f"botcheck:{client_ip}:{now_min}"
    _BOT_CHECK_RL[rl_key] = _BOT_CHECK_RL.get(rl_key, 0) + 1
    for k in list(_BOT_CHECK_RL):
        if k.split(":")[-1] != str(now_min) and k.split(":")[-1] != str(now_min - 1):
            _BOT_CHECK_RL.pop(k, None)
    if _BOT_CHECK_RL[rl_key] > 120:
        return JSONResponse({"ok": False, "error": "rate_limited"}, status_code=429)

    tg_username = _normalize_tg_username(request.query_params.get("tg_username", ""))
    chat_id_raw = request.query_params.get("chat_id", "")
    chat_id_int: Optional[int] = None
    if chat_id_raw:
        try:
            chat_id_int = int(chat_id_raw)
        except (ValueError, TypeError):
            pass

    matched_user = None
    for u in USERS.values():
        if chat_id_int is not None and u.get("tg_chat_id") == chat_id_int:
            matched_user = u
            break
        if tg_username and _normalize_tg_username(u.get("tg_username", "")) == tg_username:
            matched_user = u
            break

    if not matched_user and chat_id_int is not None:
        for u in USERS.values():
            if _normalize_tg_username(u.get("tg_username", "")) == tg_username and tg_username:
                matched_user = u
                break

    if not matched_user:
        return JSONResponse({"ok": True, "approved": False, "username": None})

    return JSONResponse({
        "ok": True,
        "approved": _is_subscription_active(matched_user),
        "username": matched_user.get("username"),
        "tg_linked": matched_user.get("tg_chat_id") is not None,
    })


@router.get("/api/user/link-code")
async def api_user_link_code(request: Request):
    user = _session_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    username = user["username"]
    now = time.time()

    for k in list(_TG_LINK_CODES):
        if _TG_LINK_CODES[k]["expires_at"] < now:
            _TG_LINK_CODES.pop(k, None)

    code = secrets.token_hex(16)
    _TG_LINK_CODES[code] = {"username": username, "expires_at": now + 900}

    bot = TELEGRAM_BOT_USERNAME.lstrip("@")
    link = f"https://t.me/{bot}?start=link_{code}"
    return JSONResponse({"ok": True, "code": code, "link": link})


@router.post("/api/bot/link-telegram")
async def api_bot_link_telegram(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    code = str(payload.get("code", "")).strip()
    try:
        chat_id = int(payload.get("chat_id", 0))
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "invalid chat_id"}, status_code=400)

    if not code or not chat_id:
        return JSONResponse({"ok": False, "error": "code and chat_id required"}, status_code=400)

    now = time.time()
    entry = _TG_LINK_CODES.get(code)
    if not entry or entry["expires_at"] < now:
        return JSONResponse({"ok": False, "error": "invalid or expired code"}, status_code=400)

    username = entry["username"]
    _TG_LINK_CODES.pop(code, None)

    async with USERS_LOCK:
        if username not in USERS:
            return JSONResponse({"ok": False, "error": "user not found"}, status_code=404)
        USERS[username]["tg_chat_id"] = chat_id
        await asyncio.to_thread(_save_users, USERS)

    logger.info("Telegram chat_id=%s linked to user=%s via deep-link code", chat_id, username)
    asyncio.create_task(_tg_send(chat_id, "✅ Ваш Telegram успешно привязан к аккаунту на сайте!"))

    return JSONResponse({"ok": True, "username": username})
