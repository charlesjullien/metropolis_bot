from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from tgworkbot.telegram_text import escape_telegram_html


@lru_cache(maxsize=1)
def _load_quotes() -> list[dict[str, str]]:
    path = Path(__file__).resolve().parent / "data" / "citations_inspirantes_fr_365.json"
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    out: list[dict[str, str]] = []
    for x in data:
        if isinstance(x, dict):
            text = str(x.get("text") or "").strip()
            author = str(x.get("author") or "").strip()
            if text:
                out.append({"text": text, "author": author})
    return out


def get_citation_for_today(*, cfg) -> dict[str, str] | None:
    quotes = _load_quotes()
    if not quotes:
        return None
    day_idx = datetime.now(ZoneInfo(cfg.bot_timezone)).timetuple().tm_yday - 1
    return quotes[day_idx % len(quotes)]


def format_citation_notification_html(*, cfg) -> str | None:
    entry = get_citation_for_today(cfg=cfg)
    if not entry:
        return None
    text = entry.get("text", "").strip()
    author = entry.get("author", "").strip()
    if not text:
        return None
    q = f"« {escape_telegram_html(text)} »"
    if author:
        return f"{q}\n— {escape_telegram_html(author)}"
    return q
