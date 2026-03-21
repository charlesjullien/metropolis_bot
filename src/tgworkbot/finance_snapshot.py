from __future__ import annotations

import asyncio
import logging
import re
import urllib.parse
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from tgworkbot.config import load_config
from tgworkbot.db import Db


LOG = logging.getLogger("tgworkbot.finance")

_YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1d"
_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; tgworkbot/1.0)",
    "Accept": "application/json,text/plain,*/*",
}

_TROY_OZ_PER_KG = 1000 / 31.1034768

FINANCE_KEYS = ("sp500", "cac40", "btc", "gold")
# «＆» (U+FF06) : évite « & » ASCII + escape HTML → affichage littéral « &amp; » sur certains clients.
FINANCE_LABELS = {
    "sp500": "S＆P 500",
    "cac40": "CAC 40",
    "btc": "Bitcoin",
    "gold": "Kg Or",
}

_SNAPSHOT_VERSION = 2


def _today_daykey(*, tz: str) -> str:
    return datetime.now(ZoneInfo(tz)).date().isoformat()


def parse_finance_selection(raw: str | None) -> set[str]:
    if not raw or not str(raw).strip():
        return set()
    return {x.strip() for x in str(raw).split(",") if x.strip() in FINANCE_KEYS}


def _yahoo_chart_url(symbol: str) -> str:
    enc = urllib.parse.quote(symbol, safe="")
    return _YAHOO_CHART.format(symbol=enc)


def _safe_error_token(s: str, *, max_len: int = 48) -> str:
    s = re.sub(r"[^\w\-+.]", "_", (s or "").strip())[:max_len]
    return s or "UNKNOWN"


def _float_meta(meta: dict[str, Any], key: str) -> float | None:
    v = meta.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


async def _yahoo_chart_meta(*, client: httpx.AsyncClient, symbol: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        r = await client.get(_yahoo_chart_url(symbol), headers=_HTTP_HEADERS, timeout=20)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        st = e.response.status_code if e.response is not None else 0
        return None, f"HTTP_{st}"
    except httpx.TimeoutException:
        return None, "TIMEOUT"
    except httpx.RequestError:
        return None, "REQUEST_ERROR"
    except OSError:
        return None, "OS_ERROR"

    try:
        data = r.json()
    except ValueError:
        return None, "JSON_DECODE_ERROR"

    chart = data.get("chart") or {}
    err = chart.get("error")
    if err:
        if isinstance(err, dict):
            desc = err.get("description") or err.get("code") or "YAHOO"
            return None, "YAHOO_" + _safe_error_token(str(desc))
        return None, "YAHOO_ERROR"

    results = chart.get("result")
    if not isinstance(results, list) or not results:
        return None, "YAHOO_NO_RESULT"

    meta = results[0].get("meta")
    if not isinstance(meta, dict):
        return None, "YAHOO_NO_META"
    return meta, None


def _build_index_instrument(*, meta: dict[str, Any] | None, err: str | None) -> dict[str, Any]:
    if err:
        return {"ok": False, "code": err}
    assert meta is not None
    price = _float_meta(meta, "regularMarketPrice")
    prev = _float_meta(meta, "chartPreviousClose")
    if price is None:
        return {"ok": False, "code": "NO_REGULAR_PRICE"}
    pct: float | None = None
    pct_err: str | None = None
    if prev is None:
        pct_err = "NO_PREV_CLOSE"
    elif prev == 0:
        pct_err = "PREV_CLOSE_ZERO"
    else:
        pct = (price - prev) / prev * 100.0
    return {
        "ok": True,
        "kind": "pts",
        "price": price,
        "pct": pct,
        "pct_err": pct_err,
    }


def _build_btc_instrument(*, meta: dict[str, Any] | None, err: str | None) -> dict[str, Any]:
    if err:
        return {"ok": False, "code": err}
    assert meta is not None
    price = _float_meta(meta, "regularMarketPrice")
    prev = _float_meta(meta, "chartPreviousClose")
    if price is None:
        return {"ok": False, "code": "NO_REGULAR_PRICE"}
    pct: float | None = None
    pct_err: str | None = None
    if prev is None:
        pct_err = "NO_PREV_CLOSE"
    elif prev == 0:
        pct_err = "PREV_CLOSE_ZERO"
    else:
        pct = (price - prev) / prev * 100.0
    return {"ok": True, "kind": "eur", "price": price, "pct": pct, "pct_err": pct_err}


def _build_gold_instrument(
    *,
    meta_g: dict[str, Any] | None,
    err_g: str | None,
    meta_e: dict[str, Any] | None,
    err_e: str | None,
) -> dict[str, Any]:
    code = err_g or err_e
    if code:
        return {"ok": False, "code": code}
    assert meta_g is not None and meta_e is not None
    gp = _float_meta(meta_g, "regularMarketPrice")
    gprev = _float_meta(meta_g, "chartPreviousClose")
    ep = _float_meta(meta_e, "regularMarketPrice")
    eprev = _float_meta(meta_e, "chartPreviousClose")
    if gp is None or ep is None or ep <= 0:
        return {"ok": False, "code": "NO_GOLD_OR_FX_PRICE"}
    now_kg = (gp / ep) * _TROY_OZ_PER_KG
    pct: float | None = None
    pct_err: str | None = None
    if gprev is None or eprev is None:
        pct_err = "NO_PREV_CLOSE"
    elif gprev <= 0 or eprev <= 0:
        pct_err = "PREV_CLOSE_ZERO"
    else:
        prev_kg = (gprev / eprev) * _TROY_OZ_PER_KG
        if prev_kg == 0:
            pct_err = "PREV_KG_ZERO"
        else:
            pct = (now_kg - prev_kg) / prev_kg * 100.0
    return {"ok": True, "kind": "eur_kg", "price": now_kg, "pct": pct, "pct_err": pct_err}


def _fmt_pts(n: float) -> str:
    return f"{n:,.2f}".replace(",", "\u202f").replace(".", ",") + " pts"


def _fmt_eur(n: float) -> str:
    return f"{n:,.0f}".replace(",", "\u202f") + " \u20ac"


def _fmt_pct(p: float) -> str:
    sign = "+" if p > 0 else ""
    return f"{sign}{p:,.2f}".replace(",", "\u202f").replace(".", ",") + " %"


def _pill_for_pct(p: float) -> str:
    if p > 0:
        return "\U0001f7e2"  # vert
    if p < 0:
        return "\U0001f534"  # rouge
    return "\u26aa"  # blanc / neutre


def _format_one_instrument(*, key: str, inst: Any) -> str:
    label = FINANCE_LABELS[key]
    if not isinstance(inst, dict):
        return f"• {label} : Erreur BAD_CACHE_SHAPE"
    if not inst.get("ok"):
        code = inst.get("code") or "UNKNOWN"
        return f"• {label} : Erreur {code}"

    price = inst.get("price")
    if price is None:
        return f"• {label} : Erreur NO_PRICE"

    kind = inst.get("kind")
    if kind == "pts":
        price_part = _fmt_pts(float(price))
    elif kind == "eur":
        price_part = _fmt_eur(float(price))
    elif kind == "eur_kg":
        price_part = _fmt_eur(float(price))
    else:
        return f"• {label} : Erreur BAD_KIND"

    pct = inst.get("pct")
    pct_err = inst.get("pct_err")
    extra = ""
    if pct is not None:
        try:
            pf = float(pct)
            extra = f" {_pill_for_pct(pf)} {_fmt_pct(pf)}"
        except (TypeError, ValueError):
            extra = " \u26aa Erreur BAD_PCT"
    elif pct_err:
        extra = f" \u26aa Erreur {pct_err}"
    return f"• {label} : {price_part}{extra}"


def _format_legacy_snapshot(*, snapshot: dict[str, Any], selection: set[str]) -> str:
    """Ancien cache (nombres plats) : pas de % fiable, on signale explicitement."""
    lines = ["Cours des indices ce matin :"]
    key_to_snap = {"sp500": "sp500", "cac40": "cac40", "btc": "btc_eur", "gold": "gold_eur_kg"}
    order = ("sp500", "cac40", "btc", "gold")
    for key in order:
        if key not in selection:
            continue
        sk = key_to_snap[key]
        val = snapshot.get(sk)
        label = FINANCE_LABELS[key]
        if val is None:
            lines.append(f"• {label} : Erreur LEGACY_NO_VALUE")
            continue
        try:
            v = float(val)
        except (TypeError, ValueError):
            lines.append(f"• {label} : Erreur LEGACY_BAD_VALUE")
            continue
        if key in ("sp500", "cac40"):
            lines.append(
                f"• {label} : {_fmt_pts(v)} \u26aa Erreur {LEGACY_NO_VARIATION_CODE}"
            )
        else:
            lines.append(f"• {label} : {_fmt_eur(v)} \u26aa Erreur {LEGACY_NO_VARIATION_CODE}")
    return "\n".join(lines)


LEGACY_NO_VARIATION_CODE = "LEGACY_CACHE_NO_VARIATION"


def format_finance_block(*, snapshot: dict[str, Any], selection: set[str]) -> str:
    if snapshot.get("v") != _SNAPSHOT_VERSION:
        return _format_legacy_snapshot(snapshot=snapshot, selection=selection)
    lines = ["Cours des indices ce matin :"]
    key_to_snap = {
        "sp500": "sp500",
        "cac40": "cac40",
        "btc": "btc_eur",
        "gold": "gold_eur_kg",
    }
    order = ("sp500", "cac40", "btc", "gold")
    for key in order:
        if key not in selection:
            continue
        lines.append(_format_one_instrument(key=key, inst=snapshot.get(key_to_snap[key])))
    return "\n".join(lines)


def _all_instruments_error(code: str) -> dict[str, Any]:
    err = {"ok": False, "code": code}
    return {
        "v": _SNAPSHOT_VERSION,
        "sp500": err,
        "cac40": err,
        "btc_eur": err,
        "gold_eur_kg": err,
    }


def _exception_code(exc: BaseException) -> str:
    if isinstance(exc, httpx.TimeoutException):
        return "TIMEOUT"
    if isinstance(exc, httpx.HTTPStatusError):
        st = exc.response.status_code if exc.response is not None else 0
        return f"HTTP_{st}"
    if isinstance(exc, httpx.RequestError):
        return "REQUEST_ERROR"
    return _safe_error_token(type(exc).__name__.upper())


async def fetch_market_snapshot(*, client: httpx.AsyncClient) -> dict[str, Any]:
    (ms, es), (mc, ec), (mb, eb), (mg, eg), (mr, er) = await asyncio.gather(
        _yahoo_chart_meta(client=client, symbol="^GSPC"),
        _yahoo_chart_meta(client=client, symbol="^FCHI"),
        _yahoo_chart_meta(client=client, symbol="BTC-EUR"),
        _yahoo_chart_meta(client=client, symbol="GC=F"),
        _yahoo_chart_meta(client=client, symbol="EURUSD=X"),
    )
    return {
        "v": _SNAPSHOT_VERSION,
        "sp500": _build_index_instrument(meta=ms, err=es),
        "cac40": _build_index_instrument(meta=mc, err=ec),
        "btc_eur": _build_btc_instrument(meta=mb, err=eb),
        "gold_eur_kg": _build_gold_instrument(meta_g=mg, err_g=eg, meta_e=mr, err_e=er),
    }


def format_finance_wait_timeout(*, selection: set[str]) -> str:
    lines = ["Cours des indices ce matin :"]
    order = ("sp500", "cac40", "btc", "gold")
    for key in order:
        if key not in selection:
            continue
        lines.append(f"• {FINANCE_LABELS[key]} : Erreur FINANCE_CACHE_TIMEOUT")
    return "\n".join(lines)


async def get_finance_block_for_user_preferences(*, cfg=None, db: Db, finance_selection_csv: str | None) -> str | None:
    selection = parse_finance_selection(finance_selection_csv)
    if not selection:
        return None
    if cfg is None:
        cfg = load_config()
    day = _today_daykey(tz=cfg.bot_timezone)

    snap = db.get_finance_cache_ready(day=day)
    if snap is not None:
        return format_finance_block(snapshot=snap, selection=selection)

    do_fetch = db.mark_finance_pending(day=day)
    if not do_fetch:
        for _ in range(10):
            snap = db.get_finance_cache_ready(day=day)
            if snap is not None:
                return format_finance_block(snapshot=snap, selection=selection)
            await asyncio.sleep(0.5)
        return format_finance_wait_timeout(selection=selection)

    snapshot: dict[str, Any]
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            snapshot = await fetch_market_snapshot(client=client)
    except Exception as e:
        LOG.exception("finance snapshot fetch failed")
        snapshot = _all_instruments_error(_exception_code(e))

    db.set_finance_cache_ready(day=day, payload=snapshot)
    return format_finance_block(snapshot=snapshot, selection=selection)
