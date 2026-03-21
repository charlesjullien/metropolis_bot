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

    return Config(
        telegram_bot_token=token,
        bot_timezone=tz,
        idfm_prim_api_key=prim_key,
        db_path=db_path,
        enable_internal_notif_scheduler=enable_internal_notif_scheduler,
        bot_admin_telegram_id=bot_admin_telegram_id,
    )

