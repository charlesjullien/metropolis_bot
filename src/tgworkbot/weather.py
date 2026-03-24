from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import httpx


@dataclass(frozen=True)
class RainWindow:
    start_hour: int
    end_hour_exclusive: int
    total_mm: float
    max_mm_per_h: float


@dataclass(frozen=True)
class WeatherSummary:
    label: str
    day: date
    windows: list[RainWindow]
    min_temp_8_20_c: float | None
    max_temp_8_20_c: float | None
    umbrella_sure: bool
    emoji: str
    is_fallback_cache: bool = False


OPEN_METEO_GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
_WEATHER_CACHE_TTL = timedelta(hours=6)
_WEATHER_RETRIES_ON_429 = 2
_WEATHER_BACKOFF_SECONDS = 0.7
_weather_cache: dict[str, tuple[datetime, WeatherSummary]] = {}


def _weather_cache_key(*, label: str, lat: float, lon: float, timezone: str) -> str:
    return f"{label.strip().lower()}|{lat:.4f}|{lon:.4f}|{timezone.strip()}"


def _get_cached_summary(*, cache_key: str) -> WeatherSummary | None:
    cached = _weather_cache.get(cache_key)
    if not cached:
        return None
    fetched_at, summary = cached
    if datetime.now(timezone.utc) - fetched_at > _WEATHER_CACHE_TTL:
        return None
    return WeatherSummary(
        label=summary.label,
        day=summary.day,
        windows=summary.windows,
        min_temp_8_20_c=summary.min_temp_8_20_c,
        max_temp_8_20_c=summary.max_temp_8_20_c,
        umbrella_sure=summary.umbrella_sure,
        emoji=summary.emoji,
        is_fallback_cache=True,
    )


async def geocode_first(
    query: str,
    *,
    language: str = "fr",
    country_code: str | None = None,
    admin1_contains: str | None = None,
) -> tuple[str, float, float] | None:
    params = {"name": query, "count": 8, "language": language, "format": "json"}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(OPEN_METEO_GEOCODE, params=params)
        r.raise_for_status()
        data = r.json()
    results = data.get("results") or []
    if not results:
        return None
    selected = None
    cc = (country_code or "").strip().upper()
    admin1_filter = (admin1_contains or "").strip().lower()
    for it in results:
        if cc and str(it.get("country_code") or "").upper() != cc:
            continue
        if admin1_filter and admin1_filter not in str(it.get("admin1") or "").lower():
            continue
        selected = it
        break
    it = selected or results[0]
    label = ", ".join([p for p in [it.get("name"), it.get("admin1"), it.get("country")] if p])
    return label, float(it["latitude"]), float(it["longitude"])


def _group_windows(hours: list[int], mm: list[float], *, threshold: float = 0.1) -> list[RainWindow]:
    windows: list[RainWindow] = []
    start = None
    acc = 0.0
    maxh = 0.0

    def flush(end_hour: int) -> None:
        nonlocal start, acc, maxh
        if start is None:
            return
        windows.append(
            RainWindow(
                start_hour=int(start),
                end_hour_exclusive=int(end_hour),
                total_mm=float(round(acc, 2)),
                max_mm_per_h=float(round(maxh, 2)),
            )
        )
        start = None
        acc = 0.0
        maxh = 0.0

    for h, v in zip(hours, mm, strict=False):
        if v >= threshold:
            if start is None:
                start = h
            acc += float(v)
            maxh = max(maxh, float(v))
        else:
            flush(h)

    if hours:
        flush(hours[-1] + 1)
    return windows


async def get_rain_summary_today(
    *,
    label: str,
    lat: float,
    lon: float,
    timezone: str,
) -> WeatherSummary:
    cache_key = _weather_cache_key(label=label, lat=lat, lon=lon, timezone=timezone)
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation,temperature_2m",
        "forecast_days": 1,
        "timezone": timezone,
    }
    data: dict
    attempts = _WEATHER_RETRIES_ON_429 + 1
    last_err: Exception | None = None
    for idx in range(attempts):
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(OPEN_METEO_FORECAST, params=params)
                r.raise_for_status()
                data = r.json()
            break
        except httpx.HTTPStatusError as e:
            last_err = e
            is_429 = e.response is not None and e.response.status_code == 429
            if is_429 and idx < attempts - 1:
                await asyncio.sleep(_WEATHER_BACKOFF_SECONDS * (idx + 1))
                continue
            cached = _get_cached_summary(cache_key=cache_key)
            if cached is not None:
                return cached
            raise
        except httpx.RequestError as e:
            last_err = e
            cached = _get_cached_summary(cache_key=cache_key)
            if cached is not None:
                return cached
            raise
    else:
        cached = _get_cached_summary(cache_key=cache_key)
        if cached is not None:
            return cached
        if last_err is not None:
            raise last_err
        raise RuntimeError("weather fetch failed without explicit error")

    hourly = data.get("hourly") or {}
    times: Iterable[str] = hourly.get("time") or []
    mm: list[float] = [float(x) for x in (hourly.get("precipitation") or [])]
    temps: list[float] = [float(x) for x in (hourly.get("temperature_2m") or [])]

    hours: list[int] = []
    for t in times:
        # format: YYYY-MM-DDTHH:MM
        try:
            hh = int(t.split("T")[1].split(":")[0])
        except Exception:
            continue
        hours.append(hh)

    d = date.fromisoformat((data.get("daily", {}).get("time", [date.today().isoformat()]) or [])[0])
    windows = _group_windows(hours, mm)
    # Temperatures between 8h and 20h inclusive
    min_t: float | None = None
    max_t: float | None = None
    umbrella_threshold_mm = 0.3  # "pluie à coup sûr" (best-effort deterministic forecast)
    umbrella_sure = False

    for h, p, t in zip(hours, mm, temps, strict=False):
        if 8 <= h <= 20:
            if t is not None:
                if min_t is None or t < min_t:
                    min_t = float(t)
                if max_t is None or t > max_t:
                    max_t = float(t)
            if p >= umbrella_threshold_mm:
                umbrella_sure = True

    emoji = "🌧️" if umbrella_sure else ("🌦️" if windows else "☀️")
    summary = WeatherSummary(
        label=label,
        day=d,
        windows=windows,
        min_temp_8_20_c=min_t,
        max_temp_8_20_c=max_t,
        umbrella_sure=umbrella_sure,
        emoji=emoji,
    )
    _weather_cache[cache_key] = (datetime.now(timezone.utc), summary)
    return summary


def format_rain_summary(summary: WeatherSummary) -> str:
    title = "<b><u>Météo</u></b>"
    fallback_note = (
        "\n- <b>Données de secours</b> : dernière météo valide (API temporairement limitée)."
        if summary.is_fallback_cache
        else ""
    )
    if not summary.windows:
        temps_part = ""
        if summary.min_temp_8_20_c is not None and summary.max_temp_8_20_c is not None:
            temps_part = f"\n- Températures (8h-20h) : {summary.min_temp_8_20_c:.0f}°C / {summary.max_temp_8_20_c:.0f}°C"
        umbrella_part = "" if not summary.umbrella_sure else "\n- Parapluie : oui"
        return f"{summary.emoji} {title} ({summary.label}) — aujourd’hui: pas de pluie prévue.{umbrella_part}{temps_part}{fallback_note}"

    parts = [f"{summary.emoji} {title} ({summary.label}) — pluie prévue:"]
    if summary.umbrella_sure:
        parts.append("- Parapluie : oui (pluie prévue à coup sûr)")
    elif summary.windows:
        parts.append("- Parapluie : peut-être (pluie possible)")
    if summary.min_temp_8_20_c is not None and summary.max_temp_8_20_c is not None:
        parts.append(
            f"- Températures (8h-20h) : {summary.min_temp_8_20_c:.0f}°C / {summary.max_temp_8_20_c:.0f}°C"
        )
    for w in summary.windows:
        parts.append(
            f"- {w.start_hour:02d}h–{w.end_hour_exclusive:02d}h: ~{w.total_mm} mm (pic {w.max_mm_per_h} mm/h)"
        )
    if fallback_note:
        parts.append(fallback_note.strip())
    return "\n".join(parts)

