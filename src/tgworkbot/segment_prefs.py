"""Lecture des champs destination / filtre direction dans segments_json."""

from __future__ import annotations

import json
import re

from tgworkbot.telegram_text import escape_telegram_html


def line_is_rer(*, commercial_mode: str | None, line_label: str | None) -> bool:
    """True si la ligne est un RER (destination par station autorisée côté bot)."""
    cm = (commercial_mode or "").strip().upper()
    if cm == "RER":
        return True
    lab = (line_label or "").upper()
    if any(x in lab for x in ("MÉTRO", "METRO", "TRAM", "TRAMWAY", "BUS")):
        return False
    return "RER" in lab


def segment_is_rer_destination(seg: dict) -> bool:
    """Destination + hints multi-branches uniquement pour le RER."""
    if not (seg.get("destination_stop_area_id") or "").strip():
        return False
    return line_is_rer(
        commercial_mode=seg.get("line_commercial_mode"),
        line_label=seg.get("line_label"),
    )


def segment_destination_label(seg: dict) -> str | None:
    if segment_is_rer_destination(seg):
        s = (seg.get("destination_stop_area_label") or "").strip()
        if s:
            return s
    s = (seg.get("direction_label") or "").strip()
    return s or None


def segment_direction_hints(seg: dict) -> list[str]:
    if segment_is_rer_destination(seg):
        raw = seg.get("direction_hints_json")
        if raw:
            try:
                data = json.loads(str(raw))
                if isinstance(data, list):
                    out = [str(x).strip() for x in data if str(x).strip()]
                    if out:
                        return out
            except (json.JSONDecodeError, TypeError):
                pass
        dl = (seg.get("direction_label") or "").strip()
        return [dl] if dl else []
    dl = (seg.get("direction_label") or "").strip()
    return [dl] if dl else []


def format_departures_block(
    *,
    dep_lines: list[str],
    line_label: str | None,
    origin_stop_label: str | None,
    destination_label: str | None,
    html: bool,
) -> str:
    line_txt = (line_label or "").strip()
    origin_txt = (origin_stop_label or "").strip()
    dest_txt = (destination_label or "").strip()
    if html:
        base = "<b><u>Prochains départs</u></b>"
        line_txt = escape_telegram_html(line_txt)
        origin_txt = escape_telegram_html(origin_txt)
        dest_txt = escape_telegram_html(dest_txt)
    else:
        base = "Prochains départs"

    if line_txt and origin_txt and dest_txt:
        header = f"{base} du {line_txt} à {origin_txt} vers {dest_txt} :"
    elif line_txt and dest_txt:
        header = f"{base} du {line_txt} vers {dest_txt} :"
    elif line_txt and origin_txt:
        header = f"{base} du {line_txt} à {origin_txt} :"
    elif line_txt:
        header = f"{base} du {line_txt} :"
    else:
        header = f"{base}:"

    bullets: list[str] = []
    for ln in dep_lines:
        raw = ln or ""
        uses_planning = "[PLANNING]" in raw
        raw = raw.replace("[PLANNING]", "").strip()
        m = re.search(r"\b(\d{1,2}:\d{2})\b", raw)
        if m:
            if uses_planning:
                suffix = " <b>(planning)</b>" if html else " (planning)"
                bullets.append(f"- {m.group(1)}{suffix}")
            else:
                bullets.append(f"- {m.group(1)}")
        else:
            base = escape_telegram_html(raw) if html else raw
            if uses_planning:
                suffix = " <b>(planning)</b>" if html else " (planning)"
                bullets.append(f"- {base}{suffix}")
            else:
                bullets.append(f"- {base}")
    return header + "\n" + "\n".join(bullets)
