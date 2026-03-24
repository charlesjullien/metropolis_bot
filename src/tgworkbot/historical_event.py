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


def _is_legacy_history_cache(headline: str) -> bool:
    """Ancien format sans date « Le {j} {mois} {année} » — on refetch."""
    h = (headline or "").strip()
    if not h or h.startswith("Erreur :"):
        return False
    return not h.startswith("Le ")


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

# Mots entiers (évite ex. « Stalingrad » si on exclut « staline »).
_HARD_EXCLUDE_WORDS = (
    "hitler",
    "nazi",
    "nazis",
    "nazisme",
    "nsdap",
    "staline",
    "weimar",
)

_HARD_EXCLUDE_PHRASES = (
    "shoah",
    "auschwitz",
    "gestapo",
    "third reich",
    "troisième reich",
    "3e reich",
    "khmer rouge",
    "djihad",
)

_MONTHS_FR = (
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
)


def _today_daykey(*, tz: str) -> str:
    return datetime.now(ZoneInfo(tz)).date().isoformat()


def _month_day_parts(*, tz: str) -> tuple[str, str]:
    now = datetime.now(ZoneInfo(tz))
    return f"{now.month:02d}", f"{now.day:02d}"


def _single_page_url(p: dict) -> str | None:
    cu = p.get("content_urls")
    if isinstance(cu, dict):
        desk = cu.get("desktop")
        if isinstance(desk, dict):
            u = desk.get("page")
            if isinstance(u, str) and u.startswith("http"):
                return u
    return None


def _best_page_url(*, event_text: str, pages: list[Any]) -> str | None:
    """Évite un lien générique (ex. « Allemagne ») quand une autre page colle mieux au texte."""
    plist = [p for p in (pages or []) if isinstance(p, dict)]
    if not plist:
        return None
    tl = event_text.lower()
    best_u: str | None = None
    best_s = -1
    for p in plist:
        u = _single_page_url(p)
        if not u:
            continue
        titles = p.get("titles") if isinstance(p.get("titles"), dict) else {}
        norm = str(titles.get("normalized") or titles.get("canonical") or p.get("title") or "")
        norm = norm.replace("_", " ").lower()
        words = re.findall(r"[a-zàâäéèêëïîôùûçœæ]{4,}", norm)
        score = sum(1 for w in words if w in tl)
        if score > best_s:
            best_s = score
            best_u = u
    if best_s > 0 and best_u:
        return best_u
    return _single_page_url(plist[0])


def _is_hard_excluded(text: str) -> bool:
    t = text.lower()
    if any(p in t for p in _HARD_EXCLUDE_PHRASES):
        return True
    for w in _HARD_EXCLUDE_WORDS:
        if re.search(rf"(?<![a-zàâäéèêëïîôùûçœæ]){re.escape(w)}(?![a-zàâäéèêëïîôùûçœæ])", t):
            return True
    return False


def _parse_event_year(ev: dict[str, Any]) -> int | None:
    y = ev.get("year")
    if isinstance(y, int) and 1 <= y <= 9999:
        return y
    if isinstance(y, str):
        s = y.strip()
        if s.isdigit() and len(s) in (3, 4):
            try:
                yi = int(s)
                if 1 <= yi <= 9999:
                    return yi
            except ValueError:
                pass
        m = re.match(r"^(\d{3,4})\b", s)
        if m:
            try:
                yi = int(m.group(1))
                if 1 <= yi <= 9999:
                    return yi
            except ValueError:
                pass
    text = str(ev.get("text") or "").strip()
    m = re.match(r"^(\d{3,4})\b", text)
    if m:
        try:
            yi = int(m.group(1))
            if 1000 <= yi <= 2100:
                return yi
        except ValueError:
            pass
    m = re.search(r"\b(1[4-9]\d{2}|20[0-2]\d)\b", text[:220])
    if m:
        try:
            yi = int(m.group(1))
            if 1400 <= yi <= 2035:
                return yi
        except ValueError:
            pass
    return None


def _is_recurring_no_year(*, text: str) -> bool:
    """Fêtes / journées internationales souvent sans champ year dans le JSON."""
    t = text.lower()
    return any(
        k in t
        for k in (
            "journée",
            "fête",
            "fete",
            "journee mondiale",
            "journée mondiale",
            "fête nationale",
            "commémor",
        )
    )


def _calendar_headline(*, cfg: Config, year: int | None, body: str) -> str:
    now = datetime.now(ZoneInfo(cfg.bot_timezone))
    day_n, mon_i = now.day, now.month - 1
    mo = _MONTHS_FR[mon_i]
    if year is not None:
        return f"Le {day_n} {mo} {year} : {body}"
    return f"Le {day_n} {mo} (chaque année) : {body}"


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
    cfg: Config,
    current_year: int,
) -> tuple[str, str | None] | None:
    if not items:
        return None
    best: tuple[float, dict[str, Any], bool] | None = None
    for ev, from_sel in items:
        text = str(ev.get("text") or "").strip()
        if not text or _is_hard_excluded(text):
            continue
        year_i = _parse_event_year(ev)
        recurring = _is_recurring_no_year(text)
        if year_i is None and not recurring:
            continue
        sc = _score_candidate(text=text, year=year_i, from_selected=from_sel, current_year=current_year)
        if best is None or sc > best[0]:
            best = (sc, ev, from_sel)
    if best is None:
        for ev, from_sel in items:
            text = str(ev.get("text") or "").strip()
            if not text or _is_hard_excluded(text):
                continue
            year_i = _parse_event_year(ev)
            sc = _score_candidate(
                text=text, year=year_i, from_selected=from_sel, current_year=current_year
            )
            sc -= 4.0
            if best is None or sc > best[0]:
                best = (sc, ev, from_sel)
    if best is None:
        return None
    _, ev, _ = best
    text = str(ev.get("text") or "").strip()
    year_i = _parse_event_year(ev)
    pages = ev.get("pages")
    pages_l = pages if isinstance(pages, list) else []
    url = _best_page_url(event_text=text, pages=pages_l)
    headline = _calendar_headline(cfg=cfg, year=year_i, body=text)
    return headline, url


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

    picked = _pick_best_event(combined, cfg=cfg, current_year=current_year)
    if not picked:
        return None, None, "WIKIMEDIA_NO_EVENT"

    headline, url = picked
    if url:
        return headline, url, None
    return headline, None, None


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
        if headline and (
            _is_retryable_cached_error(headline) or _is_legacy_history_cache(headline)
        ):
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
