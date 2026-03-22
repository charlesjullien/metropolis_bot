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

# Flux RSS : beaucoup moins de données que la page catégorie → souvent ok là où le HTML échoue (proxy PA, timeouts).
LM_POSITIF_RSS_URL = "https://lemediapositif.com/category/nos-articles/feed/"
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
_RSS_HEADERS = {
    **_HTTP_HEADERS,
    "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
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
    if isinstance(exc, httpx.ConnectError):
        return "CONNECT_ERROR"
    if isinstance(exc, httpx.RemoteProtocolError):
        return "REMOTE_PROTOCOL"
    if isinstance(exc, httpx.RequestError):
        return "REQUEST_ERROR"
    return type(exc).__name__.upper()


def _parse_first_rss_item(body: str) -> tuple[str | None, str | None, str | None]:
    """Premier <item> RSS 2.0 : (titre, url, code_erreur_parse)."""
    m = re.search(r"<item\b[^>]*>(.*?)</item>", body, re.IGNORECASE | re.DOTALL)
    if not m:
        return None, None, "RSS_NO_ITEM"
    block = m.group(1)
    tm = re.search(
        r"<title\b[^>]*>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</title>",
        block,
        re.IGNORECASE | re.DOTALL,
    )
    lm = re.search(r"<link\b[^>]*>\s*([^<\s]+)\s*</link>", block, re.IGNORECASE)
    if not tm or not lm:
        return None, None, "RSS_PARSE_INCOMPLETE"
    title = html_module.unescape(re.sub(r"<[^>]+>", "", tm.group(1))).strip()
    url = lm.group(1).strip()
    if not title or not url:
        return None, None, "RSS_EMPTY_TITLE_OR_URL"
    return title, url, None


async def _get_with_retries(
    client: httpx.AsyncClient,
    *,
    url: str,
    headers: dict[str, str],
    max_attempts: int,
) -> tuple[httpx.Response | None, str | None]:
    """(réponse 200 avec .text utilisable, code_erreur)."""
    last_code: str | None = None
    for attempt in range(max_attempts):
        try:
            r = await client.get(url, headers=headers, timeout=40, follow_redirects=True)
        except httpx.TimeoutException:
            last_code = "TIMEOUT"
        except httpx.RequestError as e:
            last_code = _fetch_exception_code(e)
            LOG.warning("good_news GET %s attempt %s/%s: %s", url, attempt + 1, max_attempts, e)
        except OSError:
            return None, "OS_ERROR"
        else:
            if r.status_code == 200:
                return r, None
            last_code = f"HTTP_{r.status_code}"

        if attempt + 1 < max_attempts:
            await asyncio.sleep(1.0 * (2**attempt))

    return None, last_code or "REQUEST_ERROR"


async def fetch_latest_lemediapositif_article(
    *, client: httpx.AsyncClient, max_attempts: int = 4
) -> tuple[str | None, str | None, str | None]:
    """
    Retourne (titre, url, code_erreur). Si code_erreur est non None, ignorer titre/url.
    1) Flux RSS (léger). 2) Page HTML catégorie. Re-tentatives pour proxy / coupures TCP.
    """
    r, err = await _get_with_retries(
        client, url=LM_POSITIF_RSS_URL, headers=_RSS_HEADERS, max_attempts=max_attempts
    )
    if r is not None:
        title, art_url, parse_err = _parse_first_rss_item(r.text)
        if not parse_err and title and art_url:
            return title, art_url, None
        if parse_err:
            LOG.warning("good_news RSS parse: %s", parse_err)

    r2, err2 = await _get_with_retries(
        client, url=LM_POSITIF_LIST_URL, headers=_HTTP_HEADERS, max_attempts=max_attempts
    )
    if r2 is None:
        return None, None, err2 or err or "REQUEST_ERROR"

    body = r2.text
    m = _ENTRY_TITLE_RE.search(body)
    if not m:
        return None, None, "SCRAPE_NO_ARTICLE"
    url = m.group(1).strip()
    title = html_module.unescape(_strip_inner_html(m.group(2))).strip()
    if not title or not url:
        return None, None, "SCRAPE_EMPTY_TITLE_OR_URL"
    return title, url, None


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

    # Pas de keep-alive : évite « connection died » sur certains proxies (ex. PythonAnywhere).
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(50.0, connect=25.0),
        trust_env=True,
        limits=httpx.Limits(max_keepalive_connections=0, max_connections=10),
    ) as client:
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
