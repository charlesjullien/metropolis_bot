from __future__ import annotations

import asyncio
import html as html_module
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from tgworkbot.config import load_config
from tgworkbot.db import Db


LOG = logging.getLogger("tgworkbot.news")

LM_POSITIF_LIST_URL = "https://lemediapositif.com/category/nos-articles/"
_HTTP_HEADERS = {
    # User-Agent « navigateur » : certains hébergeurs / WAF bloquent les clients identifiables comme bots.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

_ENTRY_TITLE_RE = re.compile(
    r'<h2 class="entry-title"[^>]*itemprop="headline"[^>]*>\s*'
    r'<a\s+href="([^"]+)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)


def _today_daykey(*, tz: str) -> str:
    return datetime.now(ZoneInfo(tz)).date().isoformat()


def _strip_inner_html(fragment: str) -> str:
    return re.sub(r"<[^>]+>", "", fragment)


def _fetch_exception_code(exc: BaseException) -> str:
    if isinstance(exc, httpx.TimeoutException):
        return "TIMEOUT"
    if isinstance(exc, httpx.HTTPStatusError):
        st = exc.response.status_code if exc.response is not None else 0
        return f"HTTP_{st}"
    if isinstance(exc, httpx.RequestError):
        return "REQUEST_ERROR"
    return type(exc).__name__.upper()


async def fetch_latest_lemediapositif_article(
    *, client: httpx.AsyncClient, max_attempts: int = 4
) -> tuple[str | None, str | None, str | None]:
    """
    Retourne (titre, url, code_erreur). Si code_erreur est non None, ignorer titre/url.
    Plusieurs tentatives : proxy / longues latences (ex. PythonAnywhere) provoquent souvent des coupures TCP.
    """
    last_code: str | None = None
    for attempt in range(max_attempts):
        try:
            r = await client.get(LM_POSITIF_LIST_URL, headers=_HTTP_HEADERS, timeout=30, follow_redirects=True)
        except httpx.TimeoutException:
            last_code = "TIMEOUT"
        except httpx.RequestError:
            last_code = "REQUEST_ERROR"
        except OSError:
            return None, None, "OS_ERROR"
        else:
            if r.status_code != 200:
                return None, None, f"HTTP_{r.status_code}"

            body = r.text
            m = _ENTRY_TITLE_RE.search(body)
            if not m:
                return None, None, "SCRAPE_NO_ARTICLE"
            url = m.group(1).strip()
            title = html_module.unescape(_strip_inner_html(m.group(2))).strip()
            if not title or not url:
                return None, None, "SCRAPE_EMPTY_TITLE_OR_URL"
            return title, url, None

        if attempt + 1 < max_attempts:
            await asyncio.sleep(1.0 * (2**attempt))

    return None, None, last_code or "REQUEST_ERROR"


async def get_good_news_text_for_today(*, cfg=None, db: Db) -> str | None:
    """
    Retourne le texte « bonne nouvelle » pour aujourd’hui (fuseau bot), cache journalier.
    En cas d’échec : message « Erreur : CODE » (pas de texte de repli arbitraire).
    """
    if cfg is None:
        cfg = load_config()
    day = _today_daykey(tz=cfg.bot_timezone)

    cached = db.get_news_cache_ready(day=day)
    if cached:
        headline, url = cached
        if not headline:
            return None
        if url:
            return f"{headline}\n{url}"
        return headline

    do_fetch = db.mark_news_pending(day=day)
    if not do_fetch:
        for _ in range(10):
            cached = db.get_news_cache_ready(day=day)
            if cached:
                headline, url = cached
                if not headline:
                    return None
                if url:
                    return f"{headline}\n{url}"
                return headline
            await asyncio.sleep(0.5)
        return "Erreur : NEWS_CACHE_TIMEOUT"

    headline: str | None = None
    url: str | None = None
    fetch_err: str | None = None

    async with httpx.AsyncClient(timeout=35, trust_env=True) as client:
        try:
            headline, url, fetch_err = await fetch_latest_lemediapositif_article(client=client)
        except Exception as e:
            LOG.exception("lemediapositif fetch failed")
            fetch_err = _fetch_exception_code(e)

    if fetch_err:
        headline = f"Erreur : {fetch_err}"
        url = None
    elif not headline:
        headline = "Erreur : SCRAPE_NO_ARTICLE"
        url = None

    db.set_news_cache_ready(day=day, headline=headline, url=url)
    if url:
        return f"{headline}\n{url}"
    return headline
