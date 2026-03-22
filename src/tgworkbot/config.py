from __future__ import annotations

import socket
from dataclasses import dataclass
from os import getenv


def _running_on_pythonanywhere() -> bool:
    try:
        fqdn = socket.getfqdn().lower().rstrip(".")
        return fqdn.endswith("pythonanywhere.com")
    except Exception:
        return False


def _good_news_try_lemediapositif() -> bool:
    """Sur PA gratuit, lemediapositif.com est en 403 proxy : désactivé par défaut."""
    use = (getenv("GOOD_NEWS_USE_LEMEDIAPOSITIF") or "").strip().lower()
    if use in ("1", "true", "yes", "on"):
        return True
    skip = (getenv("GOOD_NEWS_SKIP_LEMEDIAPOSITIF") or "").strip().lower()
    if skip in ("1", "true", "yes", "on"):
        return False
    if _running_on_pythonanywhere():
        return False
    return True


@dataclass(frozen=True)
class Config:
    telegram_bot_token: str
    bot_timezone: str
    idfm_prim_api_key: str | None
    db_path: str
    enable_internal_notif_scheduler: bool
    # Telegram user id (numeric) allowed to run /purge_db YES (full DB wipe)
    bot_admin_telegram_id: int | None
    # RSS 2.0 (URL https), ex. raw.githubusercontent.com/.../feed.xml — PythonAnywhere gratuit bloque lemediapositif.com
    good_news_rss_url: str | None
    # False sur PythonAnywhere sauf GOOD_NEWS_USE_LEMEDIAPOSITIF=1 (Internet sortant illimité)
    good_news_try_lemediapositif: bool


def load_config() -> Config:
    token = (getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in environment.")

    tz = (getenv("BOT_TIMEZONE") or "Europe/Paris").strip() or "Europe/Paris"

    prim_key = (getenv("IDFM_PRIM_API_KEY") or "").strip() or None
    db_path = (getenv("DB_PATH") or "bot.db").strip() or "bot.db"
    enable_internal_notif_scheduler_raw = (getenv("ENABLE_INTERNAL_NOTIF_SCHEDULER") or "1").strip()
    enable_internal_notif_scheduler = enable_internal_notif_scheduler_raw not in {"0", "false", "False", "FALSE", "no", "NO"}

    admin_raw = (getenv("BOT_ADMIN_TELEGRAM_ID") or "").strip()
    bot_admin_telegram_id: int | None = None
    if admin_raw:
        try:
            bot_admin_telegram_id = int(admin_raw)
        except ValueError:
            bot_admin_telegram_id = None

    good_news_rss = (getenv("GOOD_NEWS_RSS_URL") or "").strip() or None

    return Config(
        telegram_bot_token=token,
        bot_timezone=tz,
        idfm_prim_api_key=prim_key,
        db_path=db_path,
        enable_internal_notif_scheduler=enable_internal_notif_scheduler,
        bot_admin_telegram_id=bot_admin_telegram_id,
        good_news_rss_url=good_news_rss,
        good_news_try_lemediapositif=_good_news_try_lemediapositif(),
    )

