from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(frozen=True)
class Config:
    telegram_bot_token: str
    bot_timezone: str
    idfm_prim_api_key: str | None
    db_path: str
    enable_internal_notif_scheduler: bool
    # Telegram user id (numeric) allowed to run /purge_db YES (full DB wipe)
    bot_admin_telegram_id: int | None
    # Actu du jour (Vercel Métropolis) : OpenAPI Bearer, JSON {title, url}. Variables d’env : GOODNEWS_*.
    daily_news_api_url: str
    daily_news_swagger_token: str | None
    # Repli : https://newsapi.org (top-headlines France)
    newsapiorg_key: str | None
    # Transports: allow fallback to theoretical timetable when realtime is unavailable
    allow_planning_fallback: bool
    # Number of retries for realtime departures fetch (in addition to first attempt)
    realtime_departures_retries: int


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

    daily_news_token = (getenv("GOODNEWS_SWAGGER_AUTH_TOKEN") or "").strip() or None
    daily_news_url = (getenv("GOODNEWS_API_URL") or "").strip() or "https://metropolis-swagger.vercel.app/getGoodNewsOfTheDay"
    newsapi_key = (getenv("NEWSAPIORG_KEY") or "").strip() or None
    allow_planning_fallback_raw = (getenv("ALLOW_PLANNING_FALLBACK") or "1").strip()
    allow_planning_fallback = allow_planning_fallback_raw not in {"0", "false", "False", "FALSE", "no", "NO"}
    retries_raw = (getenv("REALTIME_DEPARTURES_RETRIES") or "2").strip()
    try:
        realtime_departures_retries = max(0, min(5, int(retries_raw)))
    except ValueError:
        realtime_departures_retries = 2

    return Config(
        telegram_bot_token=token,
        bot_timezone=tz,
        idfm_prim_api_key=prim_key,
        db_path=db_path,
        enable_internal_notif_scheduler=enable_internal_notif_scheduler,
        bot_admin_telegram_id=bot_admin_telegram_id,
        daily_news_api_url=daily_news_url,
        daily_news_swagger_token=daily_news_token,
        newsapiorg_key=newsapi_key,
        allow_planning_fallback=allow_planning_fallback,
        realtime_departures_retries=realtime_departures_retries,
    )
