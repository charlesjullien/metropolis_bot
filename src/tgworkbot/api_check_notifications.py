from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot

from tgworkbot.bot import process_one_webhook_update
from tgworkbot.citation_inspirante import format_citation_notification_html
from tgworkbot.config import load_config
from tgworkbot.http_logging import quiet_http_client_loggers
from tgworkbot.db import Db
from tgworkbot.finance_snapshot import get_finance_block_for_user_preferences, parse_finance_selection
from tgworkbot.segment_prefs import format_departures_block, segment_destination_label, segment_direction_hints
from tgworkbot.telegram_text import RATPSTATUS_FOOTER_HTML, escape_telegram_html
from tgworkbot.transit.providers import make_provider
from tgworkbot.weather import format_rain_summary, geocode_first, get_rain_summary_today


LOG = logging.getLogger("tgworkbot.api")

# Fenêtre après l'heure programmée : si le cron externe arrive en retard (scrap / file d'attente),
# on envoie encore jusqu'à N minutes après (ex. notif 17:10, appel HTTP à 17:13).
# Avec un cron toutes les 5 minutes, 4 minutes évite les trous tout en restant strict.
NOTIF_SLACK_AFTER_MINUTES = 4
_NOTIF_DAY_KEYS = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
_WEEKDAY_KEY_BY_INDEX = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def _minutes_since_midnight(hour: int, minute: int) -> int:
    return hour * 60 + minute


def _notif_days_from_user(user) -> set[str]:
    raw_val = getattr(user, "notif_days", None)
    if raw_val is None:
        return set(_NOTIF_DAY_KEYS)
    raw = str(raw_val).strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip() in _NOTIF_DAY_KEYS}


def notification_due_now(
    *, now_local: datetime, notif_time_hhmm: str, slack_after_minutes: int = NOTIF_SLACK_AFTER_MINUTES
) -> bool:
    """
    True si l'heure locale courante est entre l'heure notif (incluse) et notif + slack_after_minutes (incluse).
    Ne déclenche pas avant l'heure choisie.
    """
    raw = (notif_time_hhmm or "").strip()
    if not raw:
        return False
    try:
        hp, mp = raw.split(":", 1)
        nh, nm = int(hp), int(mp)
    except (ValueError, AttributeError):
        return False
    if not (0 <= nh <= 23 and 0 <= nm <= 59):
        return False
    sched = _minutes_since_midnight(nh, nm)
    cur = _minutes_since_midnight(now_local.hour, now_local.minute)
    return sched <= cur <= sched + slack_after_minutes


def _format_finance_block_html(fin: str) -> str:
    rendered = escape_telegram_html(fin or "")
    return rendered.replace(
        "Cours des indices ce matin :",
        "<b><u>Cours des indices ce matin :</u></b>",
        1,
    )


def _read_wsgi_body(environ: dict) -> bytes:
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        length = 0
    if length <= 0:
        return b""
    return environ["wsgi.input"].read(length)


def _telegram_webhook_wsgi(environ: dict, start_response: Callable) -> list[bytes]:
    """
    POST JSON Telegram → traitement par python-telegram-bot (mode PythonAnywhere sans console).
    Sécurité : en-tête X-Telegram-Bot-Api-Secret-Token = TELEGRAM_WEBHOOK_SECRET.
    """
    if environ.get("REQUEST_METHOD", "GET").upper() != "POST":
        return _json_response(start_response, status="405 Method Not Allowed", body={"error": "POST only"})

    _load_dotenv_for_wsgi()
    try:
        cfg = load_config()
    except Exception as e:
        return _json_response(start_response, status="500 Internal Server Error", body={"error": str(e)})

    if not cfg.telegram_webhook_secret:
        return _json_response(
            start_response,
            status="503 Service Unavailable",
            body={"error": "TELEGRAM_WEBHOOK_SECRET manquant dans .env"},
        )

    token_hdr = (environ.get("HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN") or "").strip()
    if token_hdr != cfg.telegram_webhook_secret:
        return _json_response(start_response, status="403 Forbidden", body={"error": "invalid webhook secret"})

    raw = _read_wsgi_body(environ)
    try:
        body = json.loads(raw.decode("utf-8") if raw else "{}")
    except json.JSONDecodeError:
        return _json_response(start_response, status="400 Bad Request", body={"error": "invalid JSON"})

    if not isinstance(body, dict):
        return _json_response(start_response, status="400 Bad Request", body={"error": "JSON object expected"})

    db = Db(cfg.db_path)
    provider = make_provider(
        idfm_prim_api_key=cfg.idfm_prim_api_key,
        allow_planning_fallback=cfg.allow_planning_fallback,
        realtime_departures_retries=cfg.realtime_departures_retries,
    )
    try:
        asyncio.run(
            process_one_webhook_update(cfg=cfg, db=db, provider=provider, update_body=body),
        )
    except Exception as e:
        LOG.exception("telegram webhook failed")
        return _json_response(start_response, status="500 Internal Server Error", body={"error": str(e)})

    return _json_response(start_response, status="200 OK", body={"ok": True})


def _json_response(start_response: Callable, *, status: str, body: dict) -> list[bytes]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    headers = [
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(data))),
    ]
    start_response(status, headers)
    return [data]


def _parse_segments_json(user) -> list[dict]:
    raw = getattr(user, "segments_json", None)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


async def _render_meteo_for_user(*, cfg, user) -> str | None:
    if user.meteo_lat is None or user.meteo_lon is None or not user.meteo_label:
        return None
    try:
        summary = await get_rain_summary_today(
            label=user.meteo_label,
            lat=float(user.meteo_lat),
            lon=float(user.meteo_lon),
            timezone=cfg.bot_timezone,
        )
        return format_rain_summary(summary)
    except (httpx.HTTPStatusError, httpx.RequestError):
        return "🌥️ <b><u>Météo</u></b> — service temporairement indisponible."


async def _render_notification_text_for_user(*, cfg, provider, db, user) -> str | None:
    parts: list[str] = []

    meteo = await _render_meteo_for_user(cfg=cfg, user=user)
    if meteo:
        parts.append(meteo)

    segments = _parse_segments_json(user)
    seg_parts: list[str] = []

    for seg in sorted(segments, key=lambda s: s.get("key", "")):
        sa_id = seg.get("stop_area_id")
        sa_label = seg.get("stop_area_label") or sa_id
        if not sa_id or not sa_label:
            continue

        line_label = seg.get("line_label")
        dest_display = segment_destination_label(seg)
        legacy_dir = (seg.get("direction_label") or "").strip()
        hints = segment_direction_hints(seg)

        # Prochains départs
        lines_block = ""
        if hasattr(provider, "get_next_departures"):
            line_id = seg.get("line_id")
            try:
                dep_lines = await provider.get_next_departures(  # type: ignore[attr-defined]
                    stop_area_id=sa_id,
                    line_id=line_id,
                    destination_stop_area_id=seg.get("destination_stop_area_id"),
                    direction_hints=hints if hints else None,
                    direction_label=None if hints else legacy_dir or None,
                    count=3,
                )
            except Exception:
                dep_lines = []
            if dep_lines:
                lines_block = format_departures_block(
                    dep_lines=dep_lines,
                    line_label=line_label,
                    origin_stop_label=sa_label,
                    destination_label=dest_display,
                    html=True,
                )

        # Perturbations temporaires
        pert_block = ""
        try:
            line_id = seg.get("line_id")
            if line_id and hasattr(provider, "get_disruptions_for_line"):
                st = await provider.get_disruptions_for_line(  # type: ignore[attr-defined]
                    line_id=str(line_id),
                    direction_label=None if hints else legacy_dir or None,
                    allowed_modes=user.allowed_modes,
                    direction_hints=hints if hints else None,
                )
                if not lines_block and not st.details and st.ok:
                    st = await provider.get_disruptions_for_line(  # type: ignore[attr-defined]
                        line_id=str(line_id),
                        direction_label=None,
                        allowed_modes=user.allowed_modes,
                        direction_hints=None,
                    )
            else:
                st = await provider.get_trip_status(  # type: ignore[attr-defined]
                    depart_sa_id=sa_id,
                    depart_sa_label=sa_label,
                    arrivee_sa_id=None,
                    arrivee_sa_label=None,
                    allowed_modes=user.allowed_modes,
                )
            if st.details:
                pert_block = "<b><u>Perturbations temporaires:</u></b>\n" + st.details
            elif not st.ok:
                pert_block = "<b><u>Perturbations temporaires:</u></b>\n" + st.headline
        except Exception:
            pass

        if not lines_block:
            lines_block = format_departures_block(
                dep_lines=["Aucun départ imminent."],
                line_label=line_label,
                origin_stop_label=sa_label,
                destination_label=dest_display,
                html=True,
            )
        body_parts = [p for p in [lines_block, pert_block] if p]
        if body_parts:
            seg_parts.append("\n\n".join(body_parts))

    if seg_parts:
        parts.append("\n\n".join(seg_parts) + "\n\n" + RATPSTATUS_FOOTER_HTML)

    if parse_finance_selection(getattr(user, "finance_selection", None)):
        try:
            fin = await get_finance_block_for_user_preferences(
                cfg=cfg, db=db, finance_selection_csv=user.finance_selection
            )
            if fin:
                parts.append(_format_finance_block_html(fin))
        except Exception:
            LOG.exception("finance snapshot failed for %s", user.chat_id)

    if user.recevoir_evenement_historique:
        try:
            from tgworkbot.historical_event import (
                get_historical_event_text_for_today,
                historical_event_notification_heading,
            )

            histo = await get_historical_event_text_for_today(cfg=cfg, db=db)
            if histo:
                title = escape_telegram_html(historical_event_notification_heading(cfg=cfg))
                parts.append(f"<b><u>{title}</u></b>\n" + escape_telegram_html(histo))
        except Exception:
            LOG.exception("historical_event failed for %s", user.chat_id)

    if user.recevoir_citation_inspirante:
        try:
            block = format_citation_notification_html(cfg=cfg)
            if block:
                parts.append(block)
        except Exception:
            LOG.exception("citation inspirante failed for %s", user.chat_id)

    if not parts:
        return None
    return "\n\n".join(parts)


async def check_and_send_notifications(*, cfg) -> dict:
    provider = make_provider(
        idfm_prim_api_key=cfg.idfm_prim_api_key,
        allow_planning_fallback=cfg.allow_planning_fallback,
        realtime_departures_retries=cfg.realtime_departures_retries,
    )
    db = Db(cfg.db_path)

    bot = Bot(token=cfg.telegram_bot_token)
    tz = ZoneInfo(cfg.bot_timezone)
    now = datetime.now(tz)
    wall_hhmm = f"{now.hour:02d}:{now.minute:02d}"
    today_key = _WEEKDAY_KEY_BY_INDEX[now.weekday()]
    sent_key_date = now.date().isoformat()

    sent = 0
    errors = 0

    for user in db.iter_users():
        if not user.notif_time:
            continue
        if today_key not in _notif_days_from_user(user):
            continue
        if not notification_due_now(now_local=now, notif_time_hhmm=user.notif_time):
            continue
        # Une clé par (jour, heure notif utilisateur) pour éviter les doublons si le cron tape 2× dans la fenêtre.
        sent_key = f"{sent_key_date} {user.notif_time}"
        if db.should_send_notif(user=user, sent_key=sent_key) is False:
            continue
        text = await _render_notification_text_for_user(cfg=cfg, provider=provider, db=db, user=user)
        if not text:
            continue
        try:
            # Avoid HTML parse if your formatting evolves; we currently don't send entities anyway.
            await bot.send_message(
                chat_id=user.chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            sent += 1
            db.set_last_notif_sent_key(user.chat_id, sent_key)
        except Exception:
            errors += 1
            LOG.exception("check_for_notifications send failed for %s", user.chat_id)
        await asyncio.sleep(0.1)

    return {
        "wall_time": wall_hhmm,
        "timezone": cfg.bot_timezone,
        "slack_after_minutes": NOTIF_SLACK_AFTER_MINUTES,
        "sent": sent,
        "errors": errors,
    }


def _load_dotenv_for_wsgi() -> None:
    """
    Sous WSGI (PythonAnywhere), les variables ne viennent pas du shell : il faut charger .env
    comme le fait run.py pour le bot. Sans cela, TELEGRAM_BOT_TOKEN manque → 500.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env")
    # Si le working directory PA pointe vers le projet, second chargement (sans écraser l’existant)
    load_dotenv(Path.cwd() / ".env", override=False)


def _wsgi_request_path(environ: dict) -> str:
    """
    Chemin demandé (sans query string). Certains hébergeurs laissent PATH_INFO vide
    et exposent seulement REQUEST_URI.
    """
    pi = (environ.get("PATH_INFO") or "").strip()
    if pi:
        return pi.split("?", 1)[0]
    uri = (environ.get("REQUEST_URI") or environ.get("RAW_URI") or "").strip()
    if uri:
        path = uri.split("?", 1)[0]
        return path if path.startswith("/") else f"/{path}"
    script = (environ.get("SCRIPT_NAME") or "").rstrip("/")
    if script:
        return script
    return ""


def application(environ, start_response):
    """
    WSGI app for PythonAnywhere.

    Endpoints:
      - GET/POST /check_for_notifications — envoi des notifs (cron-job.org, etc.)
      - POST /telegram_webhook — mises à jour du bot (mode webhook, sans console)
    """
    quiet_http_client_loggers()

    path = _wsgi_request_path(environ).rstrip("/") or "/"
    if path == "/telegram_webhook":
        return _telegram_webhook_wsgi(environ, start_response)
    if path != "/check_for_notifications":
        return _json_response(
            start_response,
            status="404 Not Found",
            body={
                "error": "not found",
                "path_received": path,
                "hint": "Attendu: /check_for_notifications ou /telegram_webhook — vérifie l’URL et le WSGI.",
            },
        )

    # Optional API key protection
    expected = environ.get("HTTP_X_API_KEY") or environ.get("QUERY_STRING", "")
    expected = None  # API key optional; keep simple for now.

    _load_dotenv_for_wsgi()

    try:
        cfg = load_config()
    except Exception as e:
        return _json_response(start_response, status="500 Internal Server Error", body={"error": str(e)})

    result = asyncio.run(check_and_send_notifications(cfg=cfg))
    return _json_response(start_response, status="200 OK", body=result)

