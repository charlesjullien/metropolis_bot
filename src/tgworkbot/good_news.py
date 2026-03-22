from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx

from tgworkbot.config import Config, load_config
from tgworkbot.db import Db


LOG = logging.getLogger("tgworkbot.news")

_HTTP_NO_RETRY_STATUS = frozenset({401, 403, 404, 429})
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}


def _today_daykey(*, tz: str) -> str:
    return datetime.now(ZoneInfo(tz)).date().isoformat()


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


def _first_usable_newsapi_article(data: dict) -> tuple[str | None, str | None]:
    """Premier article avec titre + URL exploitables (évite entrées [Removed] du plan gratuit)."""
    if data.get("status") != "ok":
        return None, None
    articles = data.get("articles")
    if not isinstance(articles, list):
        return None, None
    for a in articles:
        if not isinstance(a, dict):
            continue
        title = str(a.get("title") or "").strip()
        art_url = str(a.get("url") or "").strip()
        if not title or not art_url:
            continue
        if title.lower().startswith("[removed]"):
            continue
        return title, art_url
    return None, None


async def _get_with_retries(
    client: httpx.AsyncClient,
    *,
    url: str,
    headers: dict[str, str],
    max_attempts: int,
    url_for_log: str | None = None,
) -> tuple[httpx.Response | None, str | None]:
    """(réponse 200 avec .text utilisable, code_erreur). url_for_log : libellé sans secret."""
    log_target = url_for_log or url
    last_code: str | None = None
    for attempt in range(max_attempts):
        try:
            r = await client.get(url, headers=headers, timeout=40, follow_redirects=True)
        except httpx.TimeoutException:
            last_code = "TIMEOUT"
        except httpx.RequestError as e:
            last_code = _fetch_exception_code(e)
            LOG.warning(
                "good_news GET %s attempt %s/%s: %s", log_target, attempt + 1, max_attempts, e
            )
            if _nonretryable_request_error(e):
                final = _final_error_code_from_request_error(e, last_code)
                LOG.info(
                    "good_news GET %s: %s (pas de nouvelle tentative)", log_target, final
                )
                return None, final
        except OSError:
            return None, "OS_ERROR"
        else:
            if r.status_code == 200:
                return r, None
            last_code = f"HTTP_{r.status_code}"
            if r.status_code in _HTTP_NO_RETRY_STATUS:
                LOG.info("good_news GET %s: %s (arrêt des tentatives)", log_target, last_code)
                return None, last_code

        if attempt + 1 < max_attempts:
            await asyncio.sleep(1.0 * (2**attempt))

    return None, last_code or "REQUEST_ERROR"


async def _fetch_newsapi_fr_article(
    *, client: httpx.AsyncClient, cfg: Config, max_attempts: int
) -> tuple[str | None, str | None, str | None]:
    """
    Top headlines France.
    https://newsapi.org/docs/endpoints/top-headlines
    """
    if not cfg.newsapiorg_key:
        return None, None, None
    q = urlencode(
        {
            "country": "fr",
            "pageSize": "20",
            "apiKey": cfg.newsapiorg_key,
        }
    )
    url = f"https://newsapi.org/v2/top-headlines?{q}"
    headers = {
        "Accept": "application/json",
        "User-Agent": _HTTP_HEADERS["User-Agent"],
    }
    r, err = await _get_with_retries(
        client,
        url=url,
        headers=headers,
        max_attempts=max_attempts,
        url_for_log="https://newsapi.org/v2/top-headlines?country=fr&pageSize=20&apiKey=***",
    )
    if r is None:
        return None, None, err or "REQUEST_ERROR"
    try:
        data = r.json()
    except ValueError:
        return None, None, "JSON_ERROR"
    if data.get("status") == "error":
        msg = data.get("message") or data.get("code") or "NEWSAPI_ERROR"
        LOG.warning("good_news NewsAPI: %s", msg)
        return None, None, "NEWSAPI_ERROR"
    title, art_url = _first_usable_newsapi_article(data)
    if not title or not art_url:
        return None, None, "NEWSAPI_NO_ARTICLE"
    return title, art_url, None


async def _fetch_goodnews_metropolis_swagger(
    *, client: httpx.AsyncClient, cfg: Config, max_attempts: int
) -> tuple[str | None, str | None, str | None]:
    """GET JSON {title, url} avec Bearer (OpenAPI Métropolis)."""
    if not cfg.goodnews_swagger_token:
        return None, None, None
    headers = {
        "Authorization": f"Bearer {cfg.goodnews_swagger_token}",
        "Accept": "application/json",
        "User-Agent": _HTTP_HEADERS["User-Agent"],
    }
    r, err = await _get_with_retries(
        client,
        url=cfg.goodnews_api_url,
        headers=headers,
        max_attempts=max_attempts,
    )
    if r is None:
        return None, None, err or "REQUEST_ERROR"
    try:
        data = r.json()
    except ValueError:
        LOG.warning("good_news API: réponse non-JSON")
        return None, None, "JSON_ERROR"
    title = str(data.get("title") or "").strip()
    art_url = str(data.get("url") or "").strip()
    if not title or not art_url:
        return None, None, "API_BAD_PAYLOAD"
    return title, art_url, None


async def fetch_good_news_article(
    *, client: httpx.AsyncClient, cfg: Config, max_attempts: int = 4
) -> tuple[str | None, str | None, str | None]:
    """
    Retourne (titre, url, code_erreur).

    1) API Vercel (GOODNEWS_SWAGGER_AUTH_TOKEN)
    2) NewsAPI.org France (NEWSAPIORG_KEY)
    """
    last_err: str | None = None
    n = min(3, max_attempts)

    api_title, api_url, api_err = await _fetch_goodnews_metropolis_swagger(
        client=client, cfg=cfg, max_attempts=n
    )
    if api_title and api_url:
        return api_title, api_url, None
    if api_err is not None:
        last_err = api_err

    na_title, na_url, na_err = await _fetch_newsapi_fr_article(
        client=client, cfg=cfg, max_attempts=n
    )
    if na_title and na_url:
        return na_title, na_url, None
    if na_err is not None:
        last_err = na_err

    return None, None, last_err or "NO_GOOD_NEWS_SOURCE"


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
