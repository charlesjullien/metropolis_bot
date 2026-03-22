from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, time
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from tgworkbot.config import load_config
from tgworkbot.db import Db
from tgworkbot.finance_snapshot import get_finance_block_for_user_preferences, parse_finance_selection
from tgworkbot.http_logging import quiet_http_client_loggers
from tgworkbot.segment_prefs import (
    format_departures_block,
    line_is_rer,
    segment_destination_label,
    segment_direction_hints,
    segment_is_rer_destination,
)
from tgworkbot.telegram_text import RATPSTATUS_FOOTER_HTML, RATPSTATUS_FOOTER_PLAIN, escape_telegram_html
from tgworkbot.transit.providers import make_provider
from tgworkbot.weather import format_rain_summary, geocode_first, get_rain_summary_today


LOG: Final = logging.getLogger("tgworkbot")


def _is_bot_admin(update: Update, cfg) -> bool:
    if cfg.bot_admin_telegram_id is None:
        return False
    u = update.effective_user
    if u is None:
        return False
    return int(u.id) == int(cfg.bot_admin_telegram_id)


def _start_menu_text(*, is_admin: bool) -> str:
    """Emojis sur les lignes de description uniquement, pas sur les commandes /."""
    lines = [
        "Bonjour. Tu peux définir toutes tes préférences avec la commande /setup",
        "",
        "Une fois que c'est fait, tu peux modifier certains choix avec les commandes suivantes :",
        "",
        "🔄 Tout changer :",
        "/setup",
        "",
        "♻️ Tout réinitialiser :",
        "/reset_all",
        "",
        "🚆 Transports :",
        "/depart <station>",
        "/changement_1 <station>",
        "/changement_2 <station>",
        "/changement_3 <station>",
        "",
        "🌤 Météo :",
        "/lieuMeteo <ville> ou <latitude,longitude>",
        "",
        "📰 News :",
        "/recevoir_bonne_nouvelle",
        "/recevoir_news_finance",
        "",
        "⏰ Définir l'heure de réception de notification :",
        "/heure_notif",
        "",
        "🔔 Recevoir toute la notif :",
        "/simul_notif",
        "",
        "📍 Recevoir infos filtrées :",
        "/infos_transports",
        "",
        "👁 Voir mon setup actuel :",
        "/status",
        "",
        "🛤 Modes de transport (optionnel) :",
        "/modes",
    ]
    if is_admin:
        lines.extend(
            [
                "",
                "🔧 Admin — remise à zéro de toute la base :",
                "/purge_db YES",
            ]
        )
    return "\n".join(lines)


MODE_OPTIONS = ["Metro", "RER", "Train", "Tram", "Bus"]

FINANCE_OPTIONS = [
    ("sp500", "S＆P 500"),  # U+FF06, pas « & » ASCII (Telegram HTML)
    ("cac40", "CAC 40"),
    ("btc", "BTC"),
    ("gold", "Kg Or"),
]
_FINANCE_KEY_SET = {k for k, _ in FINANCE_OPTIONS}


def _arg_text(update: Update) -> str:
    if not update.message or not update.message.text:
        return ""
    parts = update.message.text.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def _segment_key_for_command(command: str) -> str | None:
    if command == "depart":
        return "segment0"
    if command == "changement_1":
        return "segment1"
    if command == "changement_2":
        return "segment2"
    if command == "changement_3":
        return "segment3"
    return None


def _parse_segments_json(user) -> list[dict]:
    raw = getattr(user, "segments_json", None)
    if not raw:
        return []
    import json

    try:
        data = json.loads(raw)
    except Exception:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def _store_segments_json(db: Db, chat_id: int, segments: list[dict]) -> None:
    import json

    db.set_segments_json(chat_id, json.dumps(segments, ensure_ascii=False))


def _reset_segments_for_depart(db: Db, chat_id: int) -> None:
    # When the user changes /depart, we must clear the rest of the itinerary.
    # New rule: keep only segment0; segments1..3 are removed.
    _store_segments_json(db, chat_id, [{"key": "segment0"}])


def _set_segment_station(db: Db, chat_id: int, user, seg_key: str, *, sa_id: str, sa_label: str) -> None:
    segments = _parse_segments_json(user)
    found = False
    for seg in segments:
        if seg.get("key") == seg_key:
            seg["stop_area_id"] = sa_id
            seg["stop_area_label"] = sa_label
            found = True
            break
    if not found:
        segments.append(
            {
                "key": seg_key,
                "stop_area_id": sa_id,
                "stop_area_label": sa_label,
            }
        )
    _store_segments_json(db, chat_id, segments)


def _set_segment_line(db: Db, chat_id: int, user, seg_key: str, *, line_id: str, line_label: str, commercial_mode: str = "") -> None:
    segments = _parse_segments_json(user)
    cm = (commercial_mode or "").strip()
    for seg in segments:
        if seg.get("key") == seg_key:
            seg["line_id"] = line_id
            seg["line_label"] = line_label
            seg["line_commercial_mode"] = cm
            break
    else:
        segments.append(
            {
                "key": seg_key,
                "line_id": line_id,
                "line_label": line_label,
                "line_commercial_mode": cm,
            }
        )
    _store_segments_json(db, chat_id, segments)


def _set_segment_direction(
    db: Db,
    chat_id: int,
    user,
    seg_key: str,
    *,
    direction_id: str,
    direction_label: str,
) -> None:
    segments = _parse_segments_json(user)
    for seg in segments:
        if seg.get("key") == seg_key:
            seg["direction_id"] = direction_id
            seg["direction_label"] = direction_label
            seg.pop("destination_stop_area_id", None)
            seg.pop("destination_stop_area_label", None)
            seg.pop("direction_hints_json", None)
            break
    else:
        segments.append(
            {
                "key": seg_key,
                "direction_id": direction_id,
                "direction_label": direction_label,
            }
        )
    _store_segments_json(db, chat_id, segments)


def _set_segment_destination(
    db: Db,
    chat_id: int,
    user,
    seg_key: str,
    *,
    dest_sa_id: str,
    dest_sa_label: str,
    direction_hints: list[str],
) -> None:
    segments = _parse_segments_json(user)
    hints = [str(h).strip() for h in direction_hints if str(h).strip()]
    if not hints:
        hints = [dest_sa_label]
    found = False
    for seg in segments:
        if seg.get("key") == seg_key:
            seg["destination_stop_area_id"] = dest_sa_id
            seg["destination_stop_area_label"] = dest_sa_label
            seg["direction_hints_json"] = json.dumps(hints, ensure_ascii=False)
            seg["direction_label"] = dest_sa_label
            seg.pop("direction_id", None)
            found = True
            break
    if not found:
        segments.append(
            {
                "key": seg_key,
                "destination_stop_area_id": dest_sa_id,
                "destination_stop_area_label": dest_sa_label,
                "direction_hints_json": json.dumps(hints, ensure_ascii=False),
                "direction_label": dest_sa_label,
            }
        )
    _store_segments_json(db, chat_id, segments)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    cfg = context.application.bot_data["cfg"]
    if update.effective_chat:
        db.upsert_user(update.effective_chat.id)
    await update.message.reply_text(_start_menu_text(is_admin=_is_bot_admin(update, cfg)))


def _setup_flow(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    flow = context.user_data.get("setup_flow")
    if isinstance(flow, dict) and flow.get("active"):
        return flow
    return None


def _setup_set_step(context: ContextTypes.DEFAULT_TYPE, step: str) -> None:
    flow = _setup_flow(context) or {}
    flow["active"] = True
    flow["step"] = step
    context.user_data["setup_flow"] = flow


def _setup_finish(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("setup_flow", None)


def _parse_yes_no(text: str) -> bool | None:
    v = (text or "").strip().lower()
    yes = {"oui", "o", "yes", "y", "1"}
    no = {"non", "n", "no", "0"}
    if v in yes:
        return True
    if v in no:
        return False
    return None


def _parse_finance_text(text: str) -> set[str] | None:
    raw = (text or "").strip().lower()
    if not raw:
        return None
    if raw in {"aucun", "rien", "none", "no"}:
        return set()
    if raw in {"tout", "all"}:
        return {k for k, _ in FINANCE_OPTIONS}
    tokens = {
        t.strip()
        for t in raw.replace("&", " ").replace("+", " ").replace(";", ",").split(",")
        if t.strip()
    }
    selected: set[str] = set()
    alias = {
        "sp500": "sp500",
        "s&p500": "sp500",
        "s&p": "sp500",
        "sp": "sp500",
        "cac40": "cac40",
        "cac": "cac40",
        "btc": "btc",
        "bitcoin": "btc",
        "gold": "gold",
        "or": "gold",
        "kgor": "gold",
        "kg": "gold",
    }
    for t in tokens:
        key = alias.get(t.replace(" ", ""))
        if key:
            selected.add(key)
    return selected if selected or raw in {"aucun", "rien", "none", "no"} else None


def _parse_notif_time_input(text: str) -> str | None:
    raw = (text or "").strip().lower().replace("h", ":").replace(" ", "")
    if raw.isdigit() and len(raw) in (3, 4):
        raw = raw.zfill(4)
        raw = f"{raw[0:2]}:{raw[2:4]}"
    m = __import__("re").match(r"^(\d{2}):(\d{2})$", raw)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    if mm % 15 != 0:
        return None
    return f"{hh:02d}:{mm:02d}"


def _format_finance_block_html(fin: str) -> str:
    rendered = escape_telegram_html(fin or "")
    return rendered.replace(
        "Cours des indices ce matin :",
        "<b><u>Cours des indices ce matin :</u></b>",
        1,
    )


async def _setup_after_segment_completed(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    message,
    seg_key: str,
) -> None:
    if not _setup_flow(context):
        return
    if seg_key == "segment0":
        _setup_set_step(context, "ask_change_1")
        await message.reply_text("As-tu un changement ? (oui/non)", reply_markup=ForceReply(selective=True))
        return
    if seg_key == "segment1":
        _setup_set_step(context, "ask_change_2")
        await message.reply_text("As-tu un 2e changement ? (oui/non)", reply_markup=ForceReply(selective=True))
        return
    if seg_key == "segment2":
        _setup_set_step(context, "ask_change_3")
        await message.reply_text("As-tu un 3e changement ? (oui/non)", reply_markup=ForceReply(selective=True))
        return
    if seg_key == "segment3":
        _setup_set_step(context, "await_meteo")
        await message.reply_text(
            "Lieu météo (équivalent /lieuMeteo) : envoie une ville ou lat,lon.",
            reply_markup=ForceReply(selective=True, input_field_placeholder="Paris ou 48.8566,2.3522"),
        )


async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)
    if not user:
        db.upsert_user(chat_id)
        user = db.get_user(chat_id)
    if not user:
        await update.message.reply_text("Impossible d'initialiser ton profil, réessaie.")
        return

    _setup_set_step(context, "await_depart")
    await update.message.reply_text(
        "Setup guidé démarré.\n"
        "Étape 1/6: envoie ta station de départ (je lance l'équivalent de /depart automatiquement).",
        reply_markup=ForceReply(selective=True, input_field_placeholder="/depart Bastille"),
    )


async def _start_segment_flow_from_text(
    *,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    seg_key: str,
    arg: str,
    is_depart: bool,
) -> None:
    db: Db = context.application.bot_data["db"]
    provider = context.application.bot_data["transit_provider"]
    chat_id = update.effective_chat.id

    if is_depart:
        _reset_segments_for_depart(db, chat_id)
        db.set_depart(chat_id, arg)
    else:
        db.set_direction(chat_id, arg)

    if not hasattr(provider, "suggest_stop_areas"):
        await update.message.reply_text(f"OK. Station enregistrée: {arg} (texte)")
        await _setup_after_segment_completed(context=context, message=update.message, seg_key=seg_key)
        return
    try:
        items = await provider.suggest_stop_areas(query=arg)  # type: ignore[attr-defined]
    except Exception:
        items = []
    if not items:
        await update.message.reply_text(f"OK. Station enregistrée: {arg} (texte)")
        await _setup_after_segment_completed(context=context, message=update.message, seg_key=seg_key)
        return

    key = f"{seg_key}_station_suggestions"
    context.user_data[key] = {it.id: it.label for it in items}
    keyboard = [
        [InlineKeyboardButton(it.label, callback_data=f"seg:{seg_key}:station:{it.id}")]
        for it in items
    ]
    await update.message.reply_text(
        "Choisis la station exacte :",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    if _setup_flow(context):
        _setup_set_step(context, f"await_{seg_key}_direction")


def _bonne_nouvelle_keyboard(*, enabled: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(("✅ " if enabled else "") + "Oui", callback_data="bonne_nouv:1"),
            InlineKeyboardButton(("✅ " if not enabled else "") + "Non", callback_data="bonne_nouv:0"),
        ]
    ]
    return InlineKeyboardMarkup(rows)


def _finance_from_user(user) -> set[str]:
    return parse_finance_selection(getattr(user, "finance_selection", None))


def _finance_from_context(context: ContextTypes.DEFAULT_TYPE, user) -> set[str]:
    draft = context.user_data.get("finance_draft")
    if isinstance(draft, list):
        return {str(x) for x in draft if str(x) in _FINANCE_KEY_SET}
    return _finance_from_user(user)


def _finance_keyboard(selected: set[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for key, label in FINANCE_OPTIONS:
        mark = "✅" if key in selected else "⬜"
        rows.append([InlineKeyboardButton(f"{mark} {label}", callback_data=f"fin:toggle:{key}")])
    rows.append(
        [
            InlineKeyboardButton("Tout", callback_data="fin:all"),
            InlineKeyboardButton("Rien", callback_data="fin:none"),
        ]
    )
    rows.append([InlineKeyboardButton("Valider", callback_data="fin:save")])
    return InlineKeyboardMarkup(rows)


async def cmd_recevoir_news_finance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    user = db.get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("Faites /start d'abord.")
        return
    selected = _finance_from_context(context, user)
    context.user_data["finance_draft"] = sorted(selected)
    await update.message.reply_text(
        "Choisis les cours à afficher dans la notif (données Yahoo Finance) :",
        reply_markup=_finance_keyboard(selected),
    )


async def cmd_recevoir_bonne_nouvelle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)
    enabled = bool(user.recevoir_bonne_nouvelle) if user else False
    await update.message.reply_text(
        "Veux-tu une bonne nouvelle (Le Média Positif) dans ta notification quotidienne ?",
        reply_markup=_bonne_nouvelle_keyboard(enabled=enabled),
    )


async def cmd_depart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    arg = _arg_text(update)
    if not arg:
        await update.message.reply_text("Usage: /depart <station> (ex: /depart Bastille)")
        return
    await _start_segment_flow_from_text(
        update=update,
        context=context,
        seg_key="segment0",
        arg=arg,
        is_depart=True,
    )


async def _cmd_changement(update: Update, context: ContextTypes.DEFAULT_TYPE, seg_key: str) -> None:
    arg = _arg_text(update)
    if not arg:
        await update.message.reply_text(
            f"Usage: /{seg_key.replace('segment', 'changement_')} <station> (ex: /changement_1 Bastille)"
        )
        return
    await _start_segment_flow_from_text(
        update=update,
        context=context,
        seg_key=seg_key,
        arg=arg,
        is_depart=False,
    )


async def cmd_changement_1(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _cmd_changement(update, context, "segment1")


async def cmd_changement_2(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _cmd_changement(update, context, "segment2")


async def cmd_changement_3(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _cmd_changement(update, context, "segment3")


def _parse_latlon(s: str) -> tuple[float, float] | None:
    s = s.strip()
    if "," not in s:
        return None
    a, b = s.split(",", 1)
    try:
        lat = float(a.strip())
        lon = float(b.strip())
    except ValueError:
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return lat, lon


def _is_in_ile_de_france(lat: float, lon: float) -> bool:
    # Approximate bounding box covering Ile-de-France.
    return 48.05 <= lat <= 49.30 and 1.40 <= lon <= 3.60


async def cmd_lieu_meteo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    arg = _arg_text(update)
    if not arg:
        await update.message.reply_text("Usage: /lieuMeteo <ville|lat,lon> (ex: /lieuMeteo Paris)")
        return
    await _set_meteo_from_arg(update=update, context=context, arg=arg)


async def _set_meteo_from_arg(update: Update, context: ContextTypes.DEFAULT_TYPE, *, arg: str) -> bool:
    db: Db = context.application.bot_data["db"]
    cfg = context.application.bot_data["cfg"]

    chat_id = update.effective_chat.id
    maybe = _parse_latlon(arg)
    if maybe:
        lat, lon = maybe
        if not _is_in_ile_de_france(lat, lon):
            await update.message.reply_text(
                "Lieu hors Ile-de-France. Utilise un lieu ou des coordonnees en IDF."
            )
            return False
        label = f"{lat:.4f},{lon:.4f}"
        db.set_meteo(chat_id, label, lat, lon)
        await update.message.reply_text(f"OK. Lieu météo enregistré: {label}")
        return True

    geo = await geocode_first(
        arg,
        language="fr",
        country_code="FR",
        admin1_contains="île-de-france",
    )
    if not geo:
        await update.message.reply_text("Je n'ai pas trouvé ce lieu. Essayez une ville plus précise ou 'lat,lon'.")
        return False

    label, lat, lon = geo
    if not _is_in_ile_de_france(lat, lon):
        await update.message.reply_text(
            "Lieu hors Ile-de-France. Indique une ville d'IDF (ex: Vincennes, Paris, Saint-Denis)."
        )
        return False
    db.set_meteo(chat_id, label, lat, lon)
    await update.message.reply_text(f"OK. Lieu météo enregistré: {label} ({lat:.4f},{lon:.4f})")

    # optional immediate summary
    try:
        summary = await get_rain_summary_today(label=label, lat=lat, lon=lon, timezone=cfg.bot_timezone)
        await update.message.reply_text(format_rain_summary(summary), parse_mode=ParseMode.HTML)
    except Exception:
        LOG.exception("meteo preview failed")
    return True


async def on_setup_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    if not text or text.startswith("/"):
        return

    if await _handle_segment_destination_query(update, context):
        return
    if await _handle_segment_line_direction_text(update, context):
        return
    if await _handle_rer_headsign_text(update, context):
        return

    flow = _setup_flow(context)
    if not flow:
        return
    step = str(flow.get("step") or "")
    db: Db = context.application.bot_data["db"]
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)
    if not user:
        db.upsert_user(chat_id)
        user = db.get_user(chat_id)
    if not user:
        return

    if step == "await_depart":
        await _start_segment_flow_from_text(
            update=update, context=context, seg_key="segment0", arg=text, is_depart=True
        )
        return
    if step == "ask_change_1":
        yn = _parse_yes_no(text)
        if yn is None:
            await update.message.reply_text("Réponds par oui ou non.")
            return
        if yn:
            _setup_set_step(context, "await_change_1")
            await update.message.reply_text("Indique la station du changement 1 :", reply_markup=ForceReply(selective=True))
        else:
            _setup_set_step(context, "await_meteo")
            await update.message.reply_text("Lieu météo (ville ou lat,lon) :", reply_markup=ForceReply(selective=True))
        return
    if step == "await_change_1":
        await _start_segment_flow_from_text(
            update=update, context=context, seg_key="segment1", arg=text, is_depart=False
        )
        return
    if step == "ask_change_2":
        yn = _parse_yes_no(text)
        if yn is None:
            await update.message.reply_text("Réponds par oui ou non.")
            return
        if yn:
            _setup_set_step(context, "await_change_2")
            await update.message.reply_text("Indique la station du changement 2 :", reply_markup=ForceReply(selective=True))
        else:
            _setup_set_step(context, "await_meteo")
            await update.message.reply_text("Lieu météo (ville ou lat,lon) :", reply_markup=ForceReply(selective=True))
        return
    if step == "await_change_2":
        await _start_segment_flow_from_text(
            update=update, context=context, seg_key="segment2", arg=text, is_depart=False
        )
        return
    if step == "ask_change_3":
        yn = _parse_yes_no(text)
        if yn is None:
            await update.message.reply_text("Réponds par oui ou non.")
            return
        if yn:
            _setup_set_step(context, "await_change_3")
            await update.message.reply_text("Indique la station du changement 3 :", reply_markup=ForceReply(selective=True))
        else:
            _setup_set_step(context, "await_meteo")
            await update.message.reply_text("Lieu météo (ville ou lat,lon) :", reply_markup=ForceReply(selective=True))
        return
    if step == "await_change_3":
        await _start_segment_flow_from_text(
            update=update, context=context, seg_key="segment3", arg=text, is_depart=False
        )
        return
    if step == "await_meteo":
        ok = await _set_meteo_from_arg(update=update, context=context, arg=text)
        if not ok:
            return
        _setup_set_step(context, "await_finance_click")
        selected = _finance_from_context(context, user)
        context.user_data["finance_draft"] = sorted(selected)
        await update.message.reply_text(
            "Choisis les cours à afficher (clique puis Valider) :",
            reply_markup=_finance_keyboard(selected),
        )
        return
    if step == "await_notif_time":
        value = _parse_notif_time_input(text)
        if value is None:
            await update.message.reply_text("Format invalide. Exemple attendu: 07:30 (minutes 00/15/30/45).")
            return
        db.set_notif_time(chat_id, value)
        _setup_finish(context)
        await update.message.reply_text(
            f"Setup terminé ✅ (heure: {value})\n"
            "Tu peux vérifier avec /status et tester avec /simul_notif."
        )
        return


async def _render_meteo_for_user(*, cfg, user) -> str | None:
    if user.meteo_lat is None or user.meteo_lon is None or not user.meteo_label:
        return None
    summary = await get_rain_summary_today(
        label=user.meteo_label,
        lat=float(user.meteo_lat),
        lon=float(user.meteo_lon),
        timezone=cfg.bot_timezone,
    )
    return format_rain_summary(summary)


async def _render_transit_for_user(*, provider, user) -> str | None:
    # Prefer resolved stop_area IDs if available
    if hasattr(provider, "get_trip_status") and user.depart_sa_id:
        try:
            st = await provider.get_trip_status(  # type: ignore[attr-defined]
                depart_sa_id=user.depart_sa_id,
                depart_sa_label=user.depart_sa_label or user.depart_sa_id,
                arrivee_sa_id=user.arrivee_sa_id,
                arrivee_sa_label=user.arrivee_sa_label,
                allowed_modes=user.allowed_modes,
            )
            if st.details:
                return f"{st.headline}\n{st.details}"
            return st.headline
        except Exception:
            # fallback below
            pass

    if not user.depart:
        return None
    st = await provider.get_status(depart=user.depart, direction=user.direction)
    if st.details:
        return f"{st.headline}\n{st.details}"
    return st.headline


async def cmd_perturbations(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    provider = context.application.bot_data["transit_provider"]
    user = db.get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("Faites /start d'abord.")
        return
    msg = await _render_transit_for_user(provider=provider, user=user)
    if not msg:
        await update.message.reply_text("Configurez d'abord /depart <station> (et éventuellement /changement_1..3).")
        return
    await update.message.reply_text(msg)


async def cmd_infos_transports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    provider = context.application.bot_data["transit_provider"]
    user = db.get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("Faites /start d'abord.")
        return
    segments = _parse_segments_json(user)
    if not segments:
        await update.message.reply_text(
            "Configurez d'abord /depart et éventuellement /changement_1, /changement_2, /changement_3."
        )
        return
    parts: list[str] = []
    for seg in sorted(segments, key=lambda s: s.get("key", "")):
        sa_id = seg.get("stop_area_id")
        sa_label = seg.get("stop_area_label") or sa_id
        if not sa_id or not sa_label:
            continue
        line_label = seg.get("line_label")
        dest_display = segment_destination_label(seg)
        legacy_dir = (seg.get("direction_label") or "").strip()
        hints = segment_direction_hints(seg)
        # prochains départs
        lines_block = ""
        if hasattr(provider, "get_next_departures"):
            line_id = seg.get("line_id")
            try:
                dep_lines = await provider.get_next_departures(  # type: ignore[attr-defined]
                    stop_area_id=sa_id,
                    line_id=line_id,
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
                    html=False,
                )
        # perturbations temporaires
        pert_block = ""
        try:
            # If the user selected a specific line for this segment,
            # filter disruptions by that line to prevent leaks across lines.
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
                pert_block = "Perturbations temporaires:\n" + st.details
            elif not st.ok:
                pert_block = "Perturbations temporaires:\n" + st.headline
        except Exception:
            pass
        if not lines_block:
            lines_block = format_departures_block(
                dep_lines=["Aucun départ imminent."],
                line_label=line_label,
                origin_stop_label=sa_label,
                destination_label=dest_display,
                html=False,
            )
        body_parts = [p for p in [lines_block, pert_block] if p]
        if not body_parts:
            continue
        parts.append("\n\n".join(body_parts))
    if not parts:
        await update.message.reply_text(
            "Aucune information disponible pour les stations configurées. Vérifiez /depart et /changement_1..3."
        )
        return
    await update.message.reply_text("\n\n".join(parts) + "\n\n" + RATPSTATUS_FOOTER_PLAIN)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    user = db.get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("Faites /start d'abord.")
        return

    modes = user.allowed_modes or "tous"
    meteo = user.meteo_label or "—"
    notif_time = user.notif_time or "—"
    news_pref = "Oui (Le Média Positif)" if user.recevoir_bonne_nouvelle else "Non"
    labels = dict(FINANCE_OPTIONS)
    fs = _finance_from_user(user)
    finance_pref = ", ".join(labels[k] for k, _ in FINANCE_OPTIONS if k in fs) if fs else "—"

    segments = _parse_segments_json(user)
    by_key = {s.get("key"): s for s in segments}

    def _fmt_seg(seg_key: str) -> str:
        seg = by_key.get(seg_key) or {}
        sa = seg.get("stop_area_label") or "—"
        line = seg.get("line_label") or "—"
        dest = segment_destination_label(seg)
        if dest:
            goal = f"vers {dest}" if segment_is_rer_destination(seg) else f"direction {dest}"
        else:
            goal = "—"
        return f"- {seg_key}: {sa} / {line} / {goal}"

    seg_block = "\n".join([_fmt_seg(k) for k in ("segment0", "segment1", "segment2", "segment3")])
    await update.message.reply_text(
        "Configuration:\n"
        f"- Modes: {modes}\n"
        f"- Lieu météo: {meteo}\n"
        f"- Segments:\n{seg_block}\n"
        f"- Heure notif: {notif_time}\n"
        f"- Bonne nouvelle: {news_pref}\n"
        f"- Cours / indices: {finance_pref}"
    )

def _modes_from_user(user) -> set[str]:
    raw = (user.allowed_modes or "").strip()
    if not raw:
        # If user never configured modes, start with nothing selected
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}

def _modes_from_context(context: ContextTypes.DEFAULT_TYPE, user) -> set[str]:
    draft = context.user_data.get("modes_draft")
    if isinstance(draft, list):
        return {str(x) for x in draft}
    return _modes_from_user(user)

def _modes_keyboard(selected: set[str]) -> InlineKeyboardMarkup:
    rows = []
    for m in MODE_OPTIONS:
        mark = "✅" if m in selected else "⬜"
        rows.append([InlineKeyboardButton(f"{mark} {m}", callback_data=f"mode:toggle:{m}")])
    rows.append(
        [
            InlineKeyboardButton("Tout", callback_data="mode:all"),
            InlineKeyboardButton("Rien", callback_data="mode:none"),
        ]
    )
    rows.append([InlineKeyboardButton("Valider", callback_data="mode:save")])
    return InlineKeyboardMarkup(rows)

async def cmd_modes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    user = db.get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("Faites /start d'abord.")
        return
    selected = _modes_from_context(context, user)
    context.user_data["modes_draft"] = sorted(selected)
    await update.message.reply_text("Choisis les modes de transport :", reply_markup=_modes_keyboard(selected))


async def _handle_segment_destination_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    pending = context.user_data.get("await_dest_segment")
    if not pending or not isinstance(pending, dict):
        return False
    if not update.message or not update.message.text:
        return False
    raw = update.message.text.strip()
    if raw.startswith("/"):
        return False
    seg_key = pending.get("seg_key")
    if not seg_key or not isinstance(seg_key, str):
        context.user_data.pop("await_dest_segment", None)
        return False
    provider = context.application.bot_data["transit_provider"]
    db: Db = context.application.bot_data["db"]
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)
    if not user or not hasattr(provider, "suggest_stop_areas"):
        context.user_data.pop("await_dest_segment", None)
        await update.message.reply_text("Impossible de chercher cette station pour le moment.")
        return True
    try:
        items = await provider.suggest_stop_areas(query=raw)  # type: ignore[attr-defined]
    except Exception:
        items = []
    if not items:
        await update.message.reply_text(
            "Je n'ai pas trouvé de station. Réessaie avec un autre nom (ex: ville ou gare)."
        )
        return True
    cache_key = f"{seg_key}_dest_station_suggestions"
    context.user_data[cache_key] = {it.id: it.label for it in items}
    context.user_data.pop("await_dest_segment", None)
    keyboard = [
        [InlineKeyboardButton(it.label, callback_data=f"seg:{seg_key}:dest_station:{it.id}")]
        for it in items
    ]
    await update.message.reply_text(
        "Choisis la station où tu veux aller sur cette ligne "
        "(plusieurs directions / branches peuvent y mener) :",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return True


async def _handle_rer_headsign_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Saisie manuelle de la tête de ligne RER si /journeys n'en a pas proposé."""
    seg_key = context.user_data.get("await_rer_headsign_segment")
    if not seg_key or not isinstance(seg_key, str):
        return False
    if not update.message or not update.message.text:
        return False
    raw = update.message.text.strip()
    if not raw or raw.startswith("/"):
        return False
    pending = context.user_data.get(f"{seg_key}_rer_pending_dest")
    if not isinstance(pending, dict):
        context.user_data.pop("await_rer_headsign_segment", None)
        return True
    dest_id = pending.get("id")
    dest_label = pending.get("label")
    if not dest_id or not dest_label:
        context.user_data.pop("await_rer_headsign_segment", None)
        return True
    db: Db = context.application.bot_data["db"]
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)
    if user is None:
        context.user_data.pop("await_rer_headsign_segment", None)
        return True
    _set_segment_destination(
        db,
        chat_id,
        user,
        seg_key,
        dest_sa_id=str(dest_id),
        dest_sa_label=str(dest_label),
        direction_hints=[raw],
    )
    context.user_data.pop("await_rer_headsign_segment", None)
    context.user_data.pop(f"{seg_key}_rer_pending_dest", None)
    context.user_data.pop(f"{seg_key}_rer_headsings", None)
    await update.message.reply_text(
        f"Tête de ligne enregistrée pour {seg_key} (vers {dest_label}) : {raw}"
    )
    await _setup_after_segment_completed(context=context, message=update.message, seg_key=seg_key)
    return True


async def _handle_segment_line_direction_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Saisie texte de la direction (métro / tram) quand les routes API n'ont rien listé."""
    seg_key = context.user_data.get("await_line_direction_segment")
    if not seg_key or not isinstance(seg_key, str):
        return False
    if not update.message or not update.message.text:
        return False
    raw = update.message.text.strip()
    if not raw or raw.startswith("/"):
        return False
    db: Db = context.application.bot_data["db"]
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)
    if user is None:
        context.user_data.pop("await_line_direction_segment", None)
        return True
    _set_segment_direction(
        db, chat_id, user, seg_key, direction_id="text", direction_label=raw
    )
    context.user_data.pop("await_line_direction_segment", None)
    await update.message.reply_text(f"Direction enregistrée pour {seg_key}: {raw}")
    await _setup_after_segment_completed(context=context, message=update.message, seg_key=seg_key)
    return True


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query:
        return
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    db: Db = context.application.bot_data["db"]
    chat_id = q.message.chat_id if q.message else (update.effective_chat.id if update.effective_chat else None)
    if chat_id is None:
        return
    user = db.get_user(chat_id) or None
    if user is None:
        db.upsert_user(chat_id)
        user = db.get_user(chat_id)
    if user is None:
        return

    if data.startswith("pick:"):
        # pick:depart:<id> or pick:arrivee:<id>
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        which, sa_id = parts[1], parts[2]

        label = sa_id
        if which == "depart":
            label = (context.user_data.get("depart_suggestions") or {}).get(sa_id, sa_id)
        elif which == "arrivee":
            label = (context.user_data.get("arrivee_suggestions") or {}).get(sa_id, sa_id)

        if which == "depart":
            db.set_depart_stop_area(chat_id, sa_id=sa_id, sa_label=label)
            await q.edit_message_text(f"Départ enregistré: {label}\n\nChoisis tes modes :")
            # reset draft on new selection
            context.user_data.pop("modes_draft", None)
            await q.message.reply_text("Choisis tes modes de transport :", reply_markup=_modes_keyboard(_modes_from_user(user)))
            return
        if which == "arrivee":
            db.set_arrivee_stop_area(chat_id, sa_id=sa_id, sa_label=label)
            await q.edit_message_text(f"Arrivée enregistrée: {label}\n\nChoisis tes modes :")
            context.user_data.pop("modes_draft", None)
            await q.message.reply_text("Choisis tes modes de transport :", reply_markup=_modes_keyboard(_modes_from_user(user)))
            return

    if data.startswith("seg:"):
        # seg:<segmentKey>:station|line|direction:<id>
        parts = data.split(":", 3)
        if len(parts) != 4:
            return
        _seg_prefix, seg_key, what, ident = parts
        # s'assurer qu'on a un user à jour
        db: Db = context.application.bot_data["db"]
        user = db.get_user(chat_id)
        if user is None:
            return

        provider = context.application.bot_data["transit_provider"]

        if what == "station":
            # retrouver le label depuis le cache user_data
            cache_key = f"{seg_key}_station_suggestions"
            label = (context.user_data.get(cache_key) or {}).get(ident, ident)
            _set_segment_station(db, chat_id, user, seg_key, sa_id=ident, sa_label=label)
            await q.edit_message_text(f"Station enregistrée pour {seg_key}: {label}")
            # Enchaîner avec le choix de la ligne
            if hasattr(provider, "list_lines_for_stop_area"):
                try:
                    lines = await provider.list_lines_for_stop_area(stop_area_id=ident)  # type: ignore[attr-defined]
                except Exception:
                    lines = []
                if lines:
                    line_cache_key = f"{seg_key}_line_labels"
                    context.user_data[line_cache_key] = {
                        lid: {"label": lab, "commercial_mode": cm} for lid, lab, cm in lines
                    }
                    keyboard = [
                        [
                            InlineKeyboardButton(
                                lab,
                                callback_data=f"seg:{seg_key}:line:{lid}",
                            )
                        ]
                        for lid, lab, cm in lines
                    ]
                    await q.message.reply_text(
                        "Choisis la ligne pour cette station :",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                    )
                else:
                    await _setup_after_segment_completed(context=context, message=q.message, seg_key=seg_key)
            return

        if what == "line":
            # On connaît déjà la station via segments_json
            segments = _parse_segments_json(user)
            seg = next((s for s in segments if s.get("key") == seg_key), None)
            if not seg or not seg.get("stop_area_id"):
                await q.edit_message_text("Erreur: station manquante pour ce segment.")
                return
            sa_id = seg["stop_area_id"]
            line_cache_key = f"{seg_key}_line_labels"
            meta = (context.user_data.get(line_cache_key) or {}).get(ident)
            if isinstance(meta, dict):
                label = str(meta.get("label") or ident)
                cm = str(meta.get("commercial_mode") or "")
            else:
                label = str(meta or ident)
                cm = ""
            _set_segment_line(db, chat_id, user, seg_key, line_id=ident, line_label=label, commercial_mode=cm)
            await q.edit_message_text(f"Ligne enregistrée pour {seg_key}: {label}")
            context.user_data.pop("await_line_direction_segment", None)
            if line_is_rer(commercial_mode=cm, line_label=label):
                context.user_data["await_dest_segment"] = {"seg_key": seg_key, "line_id": ident}
                await q.message.reply_text(
                    "Écris le nom de la station où tu veux aller sur cette ligne "
                    "(destination, pas seulement le texte affiché sur un écran « direction »).",
                    reply_markup=ForceReply(selective=True),
                )
            else:
                context.user_data.pop("await_dest_segment", None)
                dirs: list[str] = []
                if hasattr(provider, "list_directions_for_stop_area_line"):
                    try:
                        dirs = await provider.list_directions_for_stop_area_line(  # type: ignore[attr-defined]
                            stop_area_id=sa_id, line_id=ident
                        )
                    except Exception:
                        dirs = []
                if dirs:
                    dir_cache_key = f"{seg_key}_direction_labels"
                    context.user_data[dir_cache_key] = {str(i): d for i, d in enumerate(dirs)}
                    keyboard = [
                        [InlineKeyboardButton(d, callback_data=f"seg:{seg_key}:direction:{i}")]
                        for i, d in enumerate(dirs)
                    ]
                    await q.message.reply_text(
                        "Choisis la direction (terminus affiché sur le quai) :",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                    )
                else:
                    context.user_data["await_line_direction_segment"] = seg_key
                    await q.message.reply_text(
                        "Écris la direction (terminus affiché sur le quai), ex: Porte de Clignancourt",
                        reply_markup=ForceReply(selective=True),
                    )
            return

        if what == "dest_station":
            cache_key = f"{seg_key}_dest_station_suggestions"
            dest_label = (context.user_data.get(cache_key) or {}).get(ident, ident)
            segments = _parse_segments_json(user)
            seg = next((s for s in segments if s.get("key") == seg_key), None)
            if not seg or not seg.get("stop_area_id") or not seg.get("line_id"):
                await q.edit_message_text("Erreur: segment incomplet (station ou ligne manquante).")
                context.user_data.pop("await_dest_segment", None)
                return
            origin_id = str(seg["stop_area_id"])
            line_id = str(seg["line_id"])
            hints: list[str] = []
            if hasattr(provider, "headsings_toward_destination"):
                try:
                    hints = await provider.headsings_toward_destination(  # type: ignore[attr-defined]
                        stop_area_id=origin_id,
                        line_id=line_id,
                        destination_stop_area_id=ident,
                    )
                except Exception:
                    hints = []
            user = db.get_user(chat_id)
            if user is None:
                return
            context.user_data.pop("await_dest_segment", None)
            await q.edit_message_text(f"Destination choisie pour {seg_key}: {dest_label}")
            if len(hints) == 1:
                _set_segment_destination(
                    db,
                    chat_id,
                    user,
                    seg_key,
                    dest_sa_id=ident,
                    dest_sa_label=dest_label,
                    direction_hints=hints,
                )
                context.user_data.pop(f"{seg_key}_rer_headsings", None)
                context.user_data.pop(f"{seg_key}_rer_pending_dest", None)
                await q.edit_message_text(
                    f"Enregistré pour {seg_key} : vers {dest_label}, tête de ligne {hints[0]}"
                )
                await _setup_after_segment_completed(context=context, message=q.message, seg_key=seg_key)
                return
            if not hints:
                context.user_data[f"{seg_key}_rer_pending_dest"] = {"id": ident, "label": dest_label}
                context.user_data["await_rer_headsign_segment"] = seg_key
                await q.message.reply_text(
                    "Je n'ai pas pu lister les têtes de ligne automatiquement. "
                    "Écris celle affichée au quai pour aller vers cette destination "
                    "(ex: Juvisy, Versailles-Château Rive Gauche, …) :",
                    reply_markup=ForceReply(selective=True),
                )
                return
            context.user_data[f"{seg_key}_rer_headsings"] = hints
            context.user_data[f"{seg_key}_rer_pending_dest"] = {"id": ident, "label": dest_label}
            keyboard = [
                [InlineKeyboardButton(h[:64], callback_data=f"seg:{seg_key}:rer_headsign:{i}")]
                for i, h in enumerate(hints)
            ]
            await q.message.reply_text(
                f"Choisis la tête de ligne affichée au quai pour aller vers {dest_label} "
                "(plusieurs branches possibles sur le RER) :",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if what == "rer_headsign":
            headsigns = context.user_data.get(f"{seg_key}_rer_headsings") or []
            pending = context.user_data.get(f"{seg_key}_rer_pending_dest") or {}
            try:
                idx = int(ident)
            except ValueError:
                await q.edit_message_text("Erreur: choix de direction invalide.")
                return
            if idx < 0 or idx >= len(headsigns):
                await q.edit_message_text("Erreur: ce bouton a expiré, recommence le choix de ligne / destination.")
                return
            chosen = headsigns[idx]
            dest_id = pending.get("id")
            dest_label = pending.get("label")
            if not dest_id or not dest_label:
                await q.edit_message_text("Erreur: destination en attente perdue.")
                return
            user = db.get_user(chat_id)
            if user is None:
                return
            _set_segment_destination(
                db,
                chat_id,
                user,
                seg_key,
                dest_sa_id=str(dest_id),
                dest_sa_label=str(dest_label),
                direction_hints=[chosen],
            )
            context.user_data.pop(f"{seg_key}_rer_headsings", None)
            context.user_data.pop(f"{seg_key}_rer_pending_dest", None)
            await q.edit_message_text(f"Tête de ligne enregistrée pour {seg_key}: {chosen}")
            await _setup_after_segment_completed(context=context, message=q.message, seg_key=seg_key)
            return

        if what == "direction":
            dir_cache_key = f"{seg_key}_direction_labels"
            label = (context.user_data.get(dir_cache_key) or {}).get(ident, "")
            if not label:
                label = ident
            _set_segment_direction(
                db,
                chat_id,
                user,
                seg_key,
                direction_id=ident,
                direction_label=label,
            )
            await q.edit_message_text(f"Direction enregistrée pour {seg_key}: {label}")
            await _setup_after_segment_completed(context=context, message=q.message, seg_key=seg_key)
            return

    if data.startswith("notif:"):
        # notif:HH:MM
        value = data.split(":", 1)[1]
        db: Db = context.application.bot_data["db"]
        db.set_notif_time(chat_id, value)
        await q.edit_message_text(f"Heure de notification enregistrée: {value}")
        return

    if data.startswith("bonne_nouv:"):
        raw = data.split(":", 1)[1].strip()
        if raw not in {"0", "1"}:
            return
        enabled = raw == "1"
        db.set_recevoir_bonne_nouvelle(chat_id, enabled)
        label = "activée" if enabled else "désactivée"
        await q.edit_message_text(f"Bonne nouvelle du jour : option {label}.")
        flow = _setup_flow(context)
        if flow and str(flow.get("step") or "") == "await_goodnews_click" and q.message:
            _setup_set_step(context, "await_notif_time")
            await q.message.reply_text(
                "Heure de notification (HH:MM, multiple de 15), ex: 07:30",
                reply_markup=ForceReply(selective=True, input_field_placeholder="07:30"),
            )
        return

    if data.startswith("fin:toggle:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        key = parts[2]
        if key not in _FINANCE_KEY_SET:
            return
        selected = _finance_from_context(context, user)
        if key in selected:
            selected.remove(key)
        else:
            selected.add(key)
        context.user_data["finance_draft"] = sorted(selected)
        try:
            await q.edit_message_reply_markup(reply_markup=_finance_keyboard(selected))
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        return

    if data == "fin:all":
        selected = {k for k, _ in FINANCE_OPTIONS}
        context.user_data["finance_draft"] = sorted(selected)
        try:
            await q.edit_message_reply_markup(reply_markup=_finance_keyboard(selected))
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        return

    if data == "fin:none":
        selected: set[str] = set()
        context.user_data["finance_draft"] = []
        try:
            await q.edit_message_reply_markup(reply_markup=_finance_keyboard(selected))
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        return

    if data == "fin:save":
        selected = _finance_from_context(context, user)
        raw = ",".join(sorted(selected)) if selected else None
        db.set_finance_selection(chat_id, raw)
        context.user_data.pop("finance_draft", None)
        flow = _setup_flow(context)
        in_setup_finance = bool(flow and str(flow.get("step") or "") == "await_finance_click")
        if not selected:
            await q.edit_message_text("Aucun indice choisi : le bloc « cours des indices » est désactivé.")
            if in_setup_finance and q.message:
                _setup_set_step(context, "await_goodnews_click")
                await q.message.reply_text(
                    "Veux-tu une bonne nouvelle dans la notification ?",
                    reply_markup=_bonne_nouvelle_keyboard(enabled=bool(user.recevoir_bonne_nouvelle)),
                )
            return
        labels = dict(FINANCE_OPTIONS)
        await q.edit_message_text(
            "Cours enregistrés : " + ", ".join(labels[k] for k in sorted(selected))
        )
        if in_setup_finance and q.message:
            _setup_set_step(context, "await_goodnews_click")
            await q.message.reply_text(
                "Veux-tu une bonne nouvelle dans la notification ?",
                reply_markup=_bonne_nouvelle_keyboard(enabled=bool(user.recevoir_bonne_nouvelle)),
            )
        return

    if data.startswith("mode:toggle:"):
        mode = data.split(":", 2)[2]
        selected = _modes_from_context(context, user)
        if mode in selected:
            selected.remove(mode)
        else:
            selected.add(mode)
        # store draft in user_data
        context.user_data["modes_draft"] = sorted(selected)
        try:
            await q.edit_message_reply_markup(reply_markup=_modes_keyboard(selected))
        except BadRequest as e:
            # ignore "Message is not modified"
            if "Message is not modified" not in str(e):
                raise
        return

    if data == "mode:all":
        selected = set(MODE_OPTIONS)
        context.user_data["modes_draft"] = sorted(selected)
        try:
            await q.edit_message_reply_markup(reply_markup=_modes_keyboard(selected))
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        return

    if data == "mode:none":
        selected: set[str] = set()
        context.user_data["modes_draft"] = []
        try:
            await q.edit_message_reply_markup(reply_markup=_modes_keyboard(selected))
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        return

    if data == "mode:save":
        selected = _modes_from_context(context, user)
        if not selected:
            await q.edit_message_text("Choisis au moins 1 mode (ex: Metro), puis Valider.")
            return
        db.set_allowed_modes(chat_id, ",".join(sorted(selected)))
        context.user_data.pop("modes_draft", None)
        await q.edit_message_text(f"Modes enregistrés: {', '.join(sorted(selected))}")
        return

async def cmd_stations(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    provider = context.application.bot_data["transit_provider"]
    arg = _arg_text(update)
    if not arg:
        await update.message.reply_text("Usage: /stations <texte> (ex: /stations pont de saint cloud)")
        return
    if not hasattr(provider, "suggest_stop_areas"):
        await update.message.reply_text("Provider transports non configuré.")
        return
    try:
        items = await provider.suggest_stop_areas(query=arg)  # type: ignore[attr-defined]
    except Exception as e:
        try:
            import httpx

            if isinstance(e, httpx.HTTPStatusError) and e.response is not None:
                if e.response.status_code in (401, 403):
                    await update.message.reply_text(
                        "Accès PRIM refusé (401/403). Vérifie `IDFM_PRIM_API_KEY` et que tu es bien abonné à l’API Navitia sur PRIM."
                    )
                    return
        except Exception:
            pass
        LOG.exception("stations lookup failed")
        await update.message.reply_text("Impossible de rechercher des stations pour le moment.")
        return
    if not items:
        await update.message.reply_text("Aucune station trouvée. Essayez un autre texte (ex: 'pont', 'st cloud').")
        return
    lines = ["Stations trouvées (stop_area):"]
    for it in items:
        # Plain text to avoid Telegram Markdown entity parsing issues (':' '_' etc.)
        lines.append(f"- {it.label} (id: {it.id})")
    await update.message.reply_text("\n".join(lines))

async def cmd_primdebug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    provider = context.application.bot_data["transit_provider"]
    if not hasattr(provider, "auth_probe"):
        await update.message.reply_text("Provider PRIM non configuré.")
        return
    try:
        res = await provider.auth_probe()  # type: ignore[attr-defined]
    except Exception:
        LOG.exception("primdebug failed")
        await update.message.reply_text("Impossible de tester PRIM pour le moment.")
        return

    lines = ["PRIM auth probe (status HTTP):"]
    for k, v in res.items():
        lines.append(f"- {k}: {v}")
    await update.message.reply_text("\n".join(lines))


def _build_notif_time_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    hours = 6
    minutes = 30
    while (hours < 9) or (hours == 9 and minutes <= 30):
        label = f"{hours:02d}h{minutes:02d}"
        value = f"{hours:02d}:{minutes:02d}"
        rows.append([InlineKeyboardButton(label, callback_data=f"notif:{value}")])
        minutes += 15
        if minutes >= 60:
            minutes = 0
            hours += 1
    return InlineKeyboardMarkup(rows)


async def cmd_heure_notif(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    user = db.get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("Faites /start d'abord.")
        return
    arg = _arg_text(update)
    if arg:
        # Accept a direct time override: "17:00", "17h00", "1700"
        raw = arg.strip().lower().replace("h", ":").replace(" ", "")
        if raw.isdigit() and len(raw) in (3, 4):
            # 700 -> 07:00, 1700 -> 17:00
            raw = raw.zfill(4)
            raw = f"{raw[0:2]}:{raw[2:4]}"
        m = __import__("re").match(r"^(\\d{2}):(\\d{2})$", raw)
        if not m:
            await update.message.reply_text("Format invalide. Utilise par ex: /heure_notif 17:00")
            return
        hh = int(m.group(1))
        mm = int(m.group(2))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            await update.message.reply_text("Heure invalide. Format attendu: HH:MM")
            return
        if mm % 15 != 0:
            await update.message.reply_text("Minute invalide. Doit être multiple de 15 (ex: 17:00, 17:15).")
            return
        value = f"{hh:02d}:{mm:02d}"
        db.set_notif_time(update.effective_chat.id, value)
        await update.message.reply_text(f"Heure de notification enregistrée: {value}")
        return

    await update.message.reply_text(
        "Choisis l’heure de notification (via boutons) ou donne-la en argument: /heure_notif 17:00",
        reply_markup=_build_notif_time_keyboard(),
    )


async def cmd_simul_notif(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    app = context.application
    user = db.get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text("Faites /start d'abord.")
        return
    if not user.notif_time:
        await update.message.reply_text("Configure d'abord /heure_notif (HH:MM).")
        return
    text = await _render_notification_text_for_user(app=app, user=user)
    if not text:
        await update.message.reply_text("Rien à envoyer : configure /depart et /changement_1..3.")
        return
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_purge_db(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.application.bot_data["cfg"]
    if not _is_bot_admin(update, cfg):
        await update.message.reply_text("Accès refusé.")
        return
    db: Db = context.application.bot_data["db"]
    arg = _arg_text(update).strip()
    if arg.upper() != "YES":
        await update.message.reply_text("Commande dangereuse. Pour confirmer: /purge_db YES")
        return
    db.purge_users()
    await update.message.reply_text("Base de données entièrement réinitialisée (utilisateurs + caches).")


async def cmd_reset_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Db = context.application.bot_data["db"]
    chat_id = update.effective_chat.id
    db.upsert_user(chat_id)
    db.reset_user_profile(chat_id)
    context.user_data.clear()
    await update.message.reply_text(
        "Ton profil a été réinitialisé. Utilise /setup ou les commandes du menu pour reconfigurer."
    )


async def _render_notification_text_for_user(*, app: Application, user) -> str | None:
    cfg = app.bot_data["cfg"]
    provider = app.bot_data["transit_provider"]
    db: Db = app.bot_data["db"]
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

    if user.recevoir_bonne_nouvelle:
        try:
            from tgworkbot.good_news import get_good_news_text_for_today

            news = await get_good_news_text_for_today(cfg=cfg, db=db)
            if news:
                parts.append("<b><u>Bonne nouvelle du jour :</u></b>\n" + escape_telegram_html(news))
        except Exception:
            LOG.exception("good_news failed for %s", user.chat_id)

    if not parts:
        return None
    return "\n\n".join(parts)


async def send_daily_notifications(app: Application, *, target_time: str) -> None:
    cfg = app.bot_data["cfg"]
    db: Db = app.bot_data["db"]
    provider = app.bot_data["transit_provider"]

    notif_key_date = datetime.now(ZoneInfo(cfg.bot_timezone)).date().isoformat()
    # sent_key used to avoid duplicates within the same minute.
    sent_key = f"{notif_key_date} {target_time}"

    for user in db.iter_users():
        # Ne notifier que les utilisateurs ayant choisi cette heure
        if not user.notif_time or user.notif_time != target_time:
            continue
        if user.last_notif_sent_key == sent_key:
            continue
        text = await _render_notification_text_for_user(app=app, user=user)
        if not text:
            continue
        try:
            await app.bot.send_message(
                chat_id=user.chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            db.set_last_notif_sent_key(user.chat_id, sent_key)
        except Exception:
            LOG.exception("send failed for %s", user.chat_id)
        await asyncio.sleep(0.2)


async def _daily_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    cfg = app.bot_data["cfg"]
    now = asyncio.get_running_loop().time()  # placeholder to satisfy type checkers
    # On calcule l'heure locale à partir de la timezone du bot
    from datetime import datetime

    tz = ZoneInfo(cfg.bot_timezone)
    dt = datetime.now(tz)
    if dt.minute % 15 != 0:
        return
    target_time = f"{dt.hour:02d}:{dt.minute:02d}"
    await send_daily_notifications(app, target_time=target_time)


async def _notif_scheduler_loop(app: Application) -> None:
    """
    Fallback quand JobQueue n'est pas disponible:
    envoie les notifications toutes les 15 minutes,
    selon `user.notif_time` (HH:MM, minute multiple de 15).
    """
    cfg = app.bot_data["cfg"]
    tz = ZoneInfo(cfg.bot_timezone)
    last_key: str | None = None

    while True:
        now = datetime.now(tz)
        if now.minute % 15 == 0:
            target_time = f"{now.hour:02d}:{now.minute:02d}"
            key = f"{now.date().isoformat()} {target_time}"
            if last_key != key:
                last_key = key
                await send_daily_notifications(app, target_time=target_time)

        # polling léger
        await asyncio.sleep(10)


async def _post_init(app: Application) -> None:
    # If JobQueue isn't installed/configured, rely on an internal asyncio loop.
    if app.job_queue is None:
        if app.bot_data["cfg"].enable_internal_notif_scheduler:
            LOG.warning("JobQueue non disponible: fallback notifications via boucle asyncio (désactivable via ENABLE_INTERNAL_NOTIF_SCHEDULER=0).")
            app.create_task(_notif_scheduler_loop(app))
        else:
            LOG.info("JobQueue non disponible et scheduler interne désactivé (ENABLE_INTERNAL_NOTIF_SCHEDULER=0).")

async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOG.exception("Unhandled error", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(
            "Désolé, une erreur est survenue. Réessayez dans quelques secondes."
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    quiet_http_client_loggers()
    # Load .env from project root reliably (even if cwd differs, e.g. PythonAnywhere scheduled tasks)
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path)
    cfg = load_config()

    db = Db(cfg.db_path)
    provider = make_provider(idfm_prim_api_key=cfg.idfm_prim_api_key)

    app = Application.builder().token(cfg.telegram_bot_token).post_init(_post_init).build()
    app.bot_data["cfg"] = cfg
    app.bot_data["db"] = db
    app.bot_data["transit_provider"] = provider

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("setup", cmd_setup))
    app.add_handler(CommandHandler("depart", cmd_depart))
    app.add_handler(CommandHandler("changement_1", cmd_changement_1))
    app.add_handler(CommandHandler("changement_2", cmd_changement_2))
    app.add_handler(CommandHandler("changement_3", cmd_changement_3))
    app.add_handler(CommandHandler("infos_transports", cmd_infos_transports))
    app.add_handler(CommandHandler("infos", cmd_infos_transports))
    app.add_handler(CommandHandler("simul_notif", cmd_simul_notif))
    app.add_handler(CommandHandler("heure_notif", cmd_heure_notif))
    app.add_handler(CommandHandler("purge_db", cmd_purge_db))
    app.add_handler(CommandHandler("reset_all", cmd_reset_all))
    app.add_handler(CommandHandler("recevoir_bonne_nouvelle", cmd_recevoir_bonne_nouvelle))
    app.add_handler(CommandHandler("recevoir_news_finance", cmd_recevoir_news_finance))
    app.add_handler(CommandHandler("lieuMeteo", cmd_lieu_meteo))
    # /perturbations reste disponible pour compat.
    app.add_handler(CommandHandler("perturbations", cmd_perturbations))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stations", cmd_stations))
    app.add_handler(CommandHandler("modes", cmd_modes))
    app.add_handler(CommandHandler("primdebug", cmd_primdebug))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_setup_text))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_error_handler(_on_error)

    # Planification: on vérifie chaque minute si une notif doit partir (si JobQueue dispo)
    if app.job_queue is not None:
        app.job_queue.run_repeating(_daily_job, interval=60, first=0, name="daily")
        LOG.info(
            "Bot started. Notification scheduler (%s): each user is notified when wall time matches their DB notif_time (quarter hours).",
            cfg.bot_timezone,
        )
    else:
        LOG.warning(
            "JobQueue non disponible (python-telegram-bot[job-queue] non installé) – pas de notifications programmées."
        )

    app.run_polling(close_loop=False)

