from __future__ import annotations

import asyncio
import html as html_module
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from tgworkbot.config import Config, load_config
from tgworkbot.db import Db


LOG = logging.getLogger("tgworkbot.news")

# Flux RSS : beaucoup moins de données que la page catégorie → souvent ok là où le HTML échoue (proxy PA, timeouts).
LM_POSITIF_RSS_URL = "https://lemediapositif.com/category/nos-articles/feed/"
LM_POSITIF_LIST_URL = "https://lemediapositif.com/category/nos-articles/"
# PythonAnywhere gratuit : proxy 403 sur la plupart des sites hors whitelist ; .wikinews.org est autorisé.
WIKINEWS_RECENT_ATOM_URL = (
    "https://fr.wikinews.org/w/index.php?title=Sp%C3%A9cial:Modifications_r%C3%A9centes&feed=atom"
)

# Codes HTTP où retenter ne sert pas (ex. 403 « site interdit » sur le proxy PA).
_HTTP_NO_RETRY_STATUS = frozenset({401, 403, 404})
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


def _nonretryable_request_error(exc: BaseException) -> bool:
    """403 proxy CONNECT, 407, ou réponse HTTP « définitive » : retenter ne sert pas."""
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        return exc.response.status_code in _HTTP_NO_RETRY_STATUS
    low = str(exc).lower()
    if "403" in low and ("proxy" in low or "forbidden" in low or "connect" in low):
        return True
    if "407" in low and "proxy" in low:
        return True
    return False


def _final_error_code_from_request_error(exc: BaseException, fallback: str) -> str:
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        return f"HTTP_{exc.response.status_code}"
    if "403" in str(exc):
        return "HTTP_403"
    return fallback


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


def _wikinews_entry_is_noise(*, title: str, article_url: str) -> bool:
    t = title.strip()
    if not t:
        return True
    bad_starts = (
        "Utilisateur:",
        "Discussion utilisateur:",
        "Discussion:",
        "Spécial:",
        "Fichier:",
        "Portail:",
        "Modèle:",
        "MediaWiki:",
        "Wikinews  - Modifications récentes",
    )
    if any(t.startswith(p) for p in bad_starts):
        return True
    if "Utilisateur:" in article_url or "Discussion_utilisateur:" in article_url:
        return True
    return False


def _parse_first_good_wikinews_atom(body: str) -> tuple[str | None, str | None, str | None]:
    """Atom (modifs récentes) : premier article d’actualité exploitable."""
    for m in re.finditer(r"<entry>(.*?)</entry>", body, re.IGNORECASE | re.DOTALL):
        block = m.group(1)
        tm = re.search(
            r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
            block,
            re.IGNORECASE | re.DOTALL,
        )
        lm = re.search(
            r'<link[^>]*\brel=["\']alternate["\'][^>]*\bhref=["\']([^"\']+)["\']',
            block,
            re.IGNORECASE,
        )
        if not lm:
            lm = re.search(
                r'<link[^>]*\bhref=["\']([^"\']+)["\'][^>]*\brel=["\']alternate["\']',
                block,
                re.IGNORECASE,
            )
        if not tm or not lm:
            continue
        title = html_module.unescape(re.sub(r"<[^>]+>", "", tm.group(1))).strip()
        url = html_module.unescape(lm.group(1)).strip()
        if not title or not url:
            continue
        if _wikinews_entry_is_noise(title=title, article_url=url):
            continue
        return title, url, None
    return None, None, "ATOM_NO_ARTICLE"


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
            if _nonretryable_request_error(e):
                final = _final_error_code_from_request_error(e, last_code)
                LOG.info("good_news GET %s: %s (pas de nouvelle tentative)", url, final)
                return None, final
        except OSError:
            return None, "OS_ERROR"
        else:
            if r.status_code == 200:
                return r, None
            last_code = f"HTTP_{r.status_code}"
            if r.status_code in _HTTP_NO_RETRY_STATUS:
                LOG.info("good_news GET %s: %s (arrêt des tentatives)", url, last_code)
                return None, last_code

        if attempt + 1 < max_attempts:
            await asyncio.sleep(1.0 * (2**attempt))

    return None, last_code or "REQUEST_ERROR"


async def fetch_good_news_article(
    *, client: httpx.AsyncClient, cfg: Config, max_attempts: int = 4
) -> tuple[str | None, str | None, str | None]:
    """
    Retourne (titre, url, code_erreur). Si code_erreur est non None, ignorer titre/url.

    Ordre : GOOD_NEWS_RSS_URL (optionnel) → Le Média Positif (hors PythonAnywhere par défaut)
    → Wikinews Atom. Sur PA : définir GOOD_NEWS_RSS_URL (ex. raw.githubusercontent.com) si Wikinews répond 403.
    """
    last_err: str | None = None

    if cfg.good_news_rss_url:
        r0, e0 = await _get_with_retries(
            client,
            url=cfg.good_news_rss_url,
            headers=_RSS_HEADERS,
            max_attempts=max_attempts,
        )
        last_err = e0 or last_err
        if r0 is not None:
            title, art_url, parse_err = _parse_first_rss_item(r0.text)
            if not parse_err and title and art_url:
                return title, art_url, None
            if parse_err:
                LOG.warning("good_news GOOD_NEWS_RSS_URL parse: %s", parse_err)

    if cfg.good_news_try_lemediapositif:
        r, err = await _get_with_retries(
            client, url=LM_POSITIF_RSS_URL, headers=_RSS_HEADERS, max_attempts=max_attempts
        )
        last_err = err or last_err
        if r is not None:
            title, art_url, parse_err = _parse_first_rss_item(r.text)
            if not parse_err and title and art_url:
                return title, art_url, None
            if parse_err:
                LOG.warning("good_news RSS parse: %s", parse_err)

        r2, err2 = await _get_with_retries(
            client, url=LM_POSITIF_LIST_URL, headers=_HTTP_HEADERS, max_attempts=max_attempts
        )
        last_err = err2 or last_err
        if r2 is not None:
            body = r2.text
            m = _ENTRY_TITLE_RE.search(body)
            if m:
                url = m.group(1).strip()
                title = html_module.unescape(_strip_inner_html(m.group(2))).strip()
                if title and url:
                    return title, url, None
            LOG.warning("good_news HTML scrape: SCRAPE_NO_ARTICLE")
    else:
        LOG.debug(
            "good_news: Le Média Positif ignoré (PythonAnywhere ou GOOD_NEWS_SKIP_LEMEDIAPOSITIF). "
            "Pour le réactiver sur un compte à accès Internet complet : GOOD_NEWS_USE_LEMEDIAPOSITIF=1"
        )

    r3, err3 = await _get_with_retries(
        client,
        url=WIKINEWS_RECENT_ATOM_URL,
        headers=_RSS_HEADERS,
        max_attempts=max_attempts,
    )
    last_err = err3 or last_err
    if r3 is not None:
        title, art_url, parse_err = _parse_first_good_wikinews_atom(r3.text)
        if not parse_err and title and art_url:
            return f"[Wikinews] {title}", art_url, None
        if parse_err:
            LOG.warning("good_news Wikinews Atom: %s", parse_err)

    return None, None, last_err or "REQUEST_ERROR"


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
            headline, url, fetch_err = await fetch_good_news_article(client=client, cfg=cfg)
        except Exception as e:
            LOG.exception("good_news fetch failed")
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
