from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from tgworkbot.config import Config, load_config
from tgworkbot.db import Db

LOG = logging.getLogger("tgworkbot.historical_event")

# Ordre : REST du wiki d’abord (souvent OK là où api.wikimedia.org renvoie 403).
_ONTHISDAY_URL_TEMPLATES = (
    "https://{lang}.wikipedia.org/api/rest_v1/feed/onthisday/{kind}/{mm}/{dd}",
    "https://api.wikimedia.org/feed/v1/wikipedia/{lang}/onthisday/{kind}/{mm}/{dd}",
)


def _http_headers(*, cfg: Config) -> dict[str, str]:
    contact = (cfg.wikipedia_http_contact or "").strip()
    if contact:
        ua = f"MetropolisBot/1.0 (Telegram bot; contact: {contact}) httpx"
    else:
        ua = (
            "MetropolisBot/1.0 (Telegram bot; set WIKIPEDIA_HTTP_CONTACT in .env — "
            "see https://meta.wikimedia.org/wiki/User-Agent_policy) httpx"
        )
    return {
        "User-Agent": ua,
        "Accept": "application/json",
    }


def _is_retryable_cached_error(headline: str) -> bool:
    h = (headline or "").strip()
    if not h.startswith("Erreur : "):
        return False
    return any(
        token in h
        for token in (
            "WIKIMEDIA_HTTP_403",
            "WIKIMEDIA_HTTP_429",
            "WIKIMEDIA_HTTP_502",
            "WIKIMEDIA_HTTP_503",
            "WIKIMEDIA_NETWORK",
            "WIKIMEDIA_UNAVAILABLE",
            "FETCH_EXCEPTION",
        )
    )

# Mots-clés français (sous-chaînes) — heuristique simple pour privilégier le positif / majeur.
_POSITIVE_HINTS = frozenset(
    {
        "paix",
        "traité",
        "accord",
        "abolition",
        "droit",
        "droits",
        "démocratie",
        "démocrat",
        "indépendance",
        "république",
        "constitution",
        "découverte",
        "invention",
        "nobel",
        "prix nobel",
        "science",
        "médecine",
        "vaccin",
        "lancement",
        "satellite",
        "espace",
        "unesco",
        "patrimoine",
        "humanitaire",
        "libération",
        "suffrage",
        "vote des femmes",
        "première femme",
        "ouverture",
        "reconnaissance",
    }
)

_NEGATIVE_HINTS = frozenset(
    {
        "guerre",
        "bataille",
        "massacre",
        "attentat",
        "assassinat",
        "meurtre",
        "exécution",
        "bombardement",
        "catastrophe",
        "tsunami",
        "séisme",
        "holocaust",
        "génocide",
        "invasion",
        "coup d'état",
        "coup d’état",
        "terrorisme",
        "fusillade",
        "crash",
        "accident mortel",
    }
)


def _today_daykey(*, tz: str) -> str:
    return datetime.now(ZoneInfo(tz)).date().isoformat()


def _month_day_parts(*, tz: str) -> tuple[str, str]:
    now = datetime.now(ZoneInfo(tz))
    return f"{now.month:02d}", f"{now.day:02d}"


def _page_url(pages: list[dict]) -> str | None:
    for p in pages or []:
        if not isinstance(p, dict):
            continue
        cu = p.get("content_urls")
        if isinstance(cu, dict):
            desk = cu.get("desktop")
            if isinstance(desk, dict):
                u = desk.get("page")
                if isinstance(u, str) and u.startswith("http"):
                    return u
    return None


def _normalize_event_items(data: dict, *, key: str) -> list[dict[str, Any]]:
    raw = data.get(key)
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for it in raw:
        if isinstance(it, dict) and isinstance(it.get("text"), str) and it["text"].strip():
            out.append(it)
    return out


def _score_candidate(*, text: str, year: int | None, from_selected: bool, current_year: int) -> float:
    t = text.lower()
    pos = sum(1 for h in _POSITIVE_HINTS if h in t)
    neg = sum(1 for h in _NEGATIVE_HINTS if h in t)
    score = pos * 4.0 - neg * 6.0
    if from_selected:
        score += 5.0
    if year is not None:
        if year >= current_year:
            score -= 12.0
        elif year <= current_year - 100:
            score += 3.0
        elif year <= current_year - 30:
            score += 1.0
    if neg >= 2 and pos == 0:
        score -= 8.0
    return score


def _pick_best_event(
    items: list[tuple[dict[str, Any], bool]],
    *,
    current_year: int,
) -> tuple[str, str | None] | None:
    if not items:
        return None
    best: tuple[float, dict[str, Any], bool] | None = None
    for ev, from_sel in items:
        text = str(ev.get("text") or "").strip()
        if not text:
            continue
        y = ev.get("year")
        year_i: int | None = None
        if isinstance(y, int):
            year_i = y
        elif isinstance(y, str) and y.isdigit():
            year_i = int(y)
        elif isinstance(y, str):
            m = re.match(r"^(\d{3,4})", y.strip())
            if m:
                try:
                    year_i = int(m.group(1))
                except ValueError:
                    year_i = None
        sc = _score_candidate(text=text, year=year_i, from_selected=from_sel, current_year=current_year)
        if best is None or sc > best[0]:
            best = (sc, ev, from_sel)
    if best is None:
        return None
    _, ev, _ = best
    text = str(ev.get("text") or "").strip()
    pages = ev.get("pages")
    pages_l = pages if isinstance(pages, list) else []
    url = _page_url(pages_l)
    return text, url


async def _fetch_onthisday_kind(
    *,
    client: httpx.AsyncClient,
    cfg: Config,
    lang: str,
    kind: str,
    mm: str,
    dd: str,
) -> dict[str, Any]:
    headers = _http_headers(cfg=cfg)
    last_status: int | None = None
    for tpl in _ONTHISDAY_URL_TEMPLATES:
        url = tpl.format(lang=lang, kind=kind, mm=mm, dd=dd)
        try:
            r = await client.get(url, headers=headers)
            last_status = r.status_code
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                return data
            return {}
        except httpx.HTTPStatusError as e:
            last_status = e.response.status_code if e.response is not None else last_status
            code = e.response.status_code if e.response is not None else 0
            if code in (403, 404, 429, 502, 503):
                LOG.warning(
                    "historical_event onthisday %s %s -> HTTP %s (essai suivant)",
                    kind,
                    url.split("/feed/")[-1] if "/feed/" in url else url,
                    code,
                )
                continue
            raise
        except httpx.RequestError as e:
            LOG.warning("historical_event onthisday %s network: %s", kind, e)
            continue
    LOG.error("historical_event onthisday %s: tous les points d’accès ont échoué (dernier HTTP %s)", kind, last_status)
    return {}


async def fetch_positive_historical_event(
    *, client: httpx.AsyncClient, cfg: Config
) -> tuple[str | None, str | None, str | None]:
    """
    Retourne (texte utilisateur, url wikipedia, code_erreur).
    """
    lang = (cfg.wikipedia_onthisday_lang or "fr").strip() or "fr"
    mm, dd = _month_day_parts(tz=cfg.bot_timezone)
    current_year = datetime.now(ZoneInfo(cfg.bot_timezone)).year
    selected_raw, events_raw, holidays_raw = await asyncio.gather(
        _fetch_onthisday_kind(client=client, cfg=cfg, lang=lang, kind="selected", mm=mm, dd=dd),
        _fetch_onthisday_kind(client=client, cfg=cfg, lang=lang, kind="events", mm=mm, dd=dd),
        _fetch_onthisday_kind(client=client, cfg=cfg, lang=lang, kind="holidays", mm=mm, dd=dd),
    )

    combined: list[tuple[dict[str, Any], bool]] = []
    for ev in _normalize_event_items(selected_raw, key="selected"):
        combined.append((ev, True))
    for ev in _normalize_event_items(events_raw, key="events"):
        combined.append((ev, False))
    for ev in _normalize_event_items(holidays_raw, key="holidays"):
        combined.append((ev, False))

    if not combined:
        return None, None, "WIKIMEDIA_UNAVAILABLE"

    picked = _pick_best_event(combined, current_year=current_year)
    if not picked:
        return None, None, "WIKIMEDIA_NO_EVENT"

    text, url = picked
    if url:
        return text, url, None
    return text, None, None


async def get_historical_event_text_for_today(*, cfg: Config | None = None, db: Db) -> str | None:
    """
    Un événement du jour (Wikipédia « Ce jour-là »), filtré vers le positif / majeur, cache journalier.
    """
    if cfg is None:
        cfg = load_config()
    day = _today_daykey(tz=cfg.bot_timezone)

    cached = db.get_history_day_cache_ready(day=day)
    if cached:
        headline, url = cached
        if headline and _is_retryable_cached_error(headline):
            db.delete_history_day_cache_row(day=day)
            cached = None
    if cached:
        headline, url = cached
        if not headline:
            return None
        if url:
            return f"{headline}\n{url}"
        return headline

    do_fetch = db.mark_history_day_pending(day=day)
    if not do_fetch:
        for _ in range(10):
            cached = db.get_history_day_cache_ready(day=day)
            if cached:
                headline, url = cached
                if not headline:
                    return None
                if url:
                    return f"{headline}\n{url}"
                return headline
            await asyncio.sleep(0.5)
        return "Erreur : HISTORY_CACHE_TIMEOUT"

    headline: str | None = None
    url: str | None = None
    fetch_err: str | None = None

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(45.0, connect=20.0),
        trust_env=True,
        limits=httpx.Limits(max_keepalive_connections=0, max_connections=10),
    ) as client:
        try:
            text, art_url, err = await fetch_positive_historical_event(client=client, cfg=cfg)
            if err:
                fetch_err = err
            elif text:
                headline = text
                url = art_url
        except Exception:
            LOG.exception("historical_event fetch failed")
            fetch_err = "FETCH_EXCEPTION"

    if fetch_err:
        headline = f"Erreur : {fetch_err}"
        url = None
    elif not headline:
        headline = "Erreur : NO_HISTORICAL_EVENT"
        url = None

    db.set_history_day_cache_ready(day=day, headline=headline, url=url)
    if url:
        return f"{headline}\n{url}"
    return headline
