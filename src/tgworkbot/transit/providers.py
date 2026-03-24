from __future__ import annotations

import asyncio
from dataclasses import dataclass
import html
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx


@dataclass(frozen=True)
class TransitStatus:
    ok: bool
    headline: str
    details: str | None = None


class TransitProvider:
    async def get_status(self, *, depart: str, direction: str | None) -> TransitStatus:
        raise NotImplementedError


class NotConfiguredProvider(TransitProvider):
    async def get_status(self, *, depart: str, direction: str | None) -> TransitStatus:
        return TransitStatus(
            ok=True,
            headline="Transports: provider non configuré.",
            details="Ajoutez une clé (ex: IDFM_PRIM_API_KEY) pour activer la vérification des perturbations.",
        )


class IdFmPrimNavitiaProvider(TransitProvider):
    """
    Uses PRIM Navitia marketplace (coverage: idfm).

    Base URL documented by PRIM:
    https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/${API_NAVITIA}/${PATH}/${FEATURE}?${QUERY_PARAMS}
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia",
        coverage: str = "idfm",
        allow_planning_fallback: bool = True,
        realtime_departures_retries: int = 2,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.coverage = coverage
        self.allow_planning_fallback = bool(allow_planning_fallback)
        self.realtime_departures_retries = max(0, min(5, int(realtime_departures_retries)))

    def _enc(self, uri: str) -> str:
        # Navitia URIs contain ":" and must be percent-encoded in path segments
        # (e.g. stop_area%3AIDFM%3A71274)
        return quote(uri, safe="")

    @dataclass(frozen=True)
    class StopArea:
        id: str
        label: str

    async def _get(self, path: str, params: dict[str, str] | None = None) -> dict:
        # PRIM expects an API Key header.
        # Important: some API gateways misbehave if multiple auth headers are sent at once.
        # We therefore try one header at a time, with a small fallback.
        url = f"{self.base_url}{path}"

        async with httpx.AsyncClient(timeout=20) as client:
            # 1) documented header
            r = await client.get(url, params=params, headers={"apiKey": self.api_key})
            if r.status_code in (401, 403):
                # 2) fallback header variant
                r = await client.get(url, params=params, headers={"apikey": self.api_key})
            r.raise_for_status()
            return r.json()

    async def auth_probe(self) -> dict[str, int]:
        """
        Returns HTTP status codes for several auth styles, without exposing the token.
        Useful to debug PRIM auth differences between environments.
        """
        url = f"{self.base_url}/pt_objects"
        params = {"q": "invalides", "count": "1"}
        variants: list[tuple[str, dict[str, str]]] = [
            ("no_header", {}),
            ("apiKey", {"apiKey": self.api_key}),
            ("apikey", {"apikey": self.api_key}),
            ("Authorization_raw", {"Authorization": self.api_key}),
            ("Authorization_Bearer", {"Authorization": f"Bearer {self.api_key}"}),
            ("apiKey+apikey", {"apiKey": self.api_key, "apikey": self.api_key}),
        ]
        out: dict[str, int] = {}
        async with httpx.AsyncClient(timeout=20) as client:
            for name, headers in variants:
                try:
                    r = await client.get(url, params=params, headers=headers)
                    out[name] = int(r.status_code)
                except Exception:
                    out[name] = -1
        return out

    async def _resolve_stop_area(self, query: str) -> StopArea | None:
        items = await self.suggest_stop_areas(query=query)
        return items[0] if items else None

    async def suggest_stop_areas(self, *, query: str) -> list[StopArea]:
        # PRIM Navitia "accès générique" exposes /pt_objects directly (no /coverage/{uri} prefix),
        # as shown in the official PRIM page and playground.
        data = await self._get("/pt_objects", {"q": query, "count": "20"})

        out: list[IdFmPrimNavitiaProvider.StopArea] = []
        for obj in (data.get("pt_objects") or []):
            if obj.get("embedded_type") != "stop_area":
                continue
            sa = obj.get("stop_area") or {}
            sid = sa.get("id") or obj.get("id")
            # Prefer the "label" (often includes city), fallback to name.
            name = sa.get("label") or obj.get("name") or sa.get("name")
            if sid and name:
                out.append(self.StopArea(id=str(sid), label=str(name)))

        # Deduplicate while keeping order
        seen: set[str] = set()
        uniq: list[IdFmPrimNavitiaProvider.StopArea] = []
        for it in out:
            if it.id in seen:
                continue
            seen.add(it.id)
            uniq.append(it)
            if len(uniq) >= 5:
                break
        return uniq

    async def _get_disruptions_for_stop_area(self, stop_area_id: str) -> list[dict]:
        # Depending on PRIM subscription/products, some endpoints may be unavailable.
        # Try several known paths.
        sa = self._enc(stop_area_id)
        def _normalize_disruption(d: dict) -> dict:
            # Some PRIM payloads nest disruption under a "disruption" key.
            inner = d.get("disruption")
            if isinstance(inner, dict):
                return inner
            return d

        def _extract_disruptions(data: dict) -> list[dict]:
            disruptions = data.get("disruptions")
            if isinstance(disruptions, list):
                return [_normalize_disruption(x) for x in disruptions if isinstance(x, dict)]
            # Some endpoints return traffic_reports instead of a flat disruptions list.
            traffic_reports = data.get("traffic_reports")
            if isinstance(traffic_reports, list):
                out: list[dict] = []
                for tr in traffic_reports:
                    ds = tr.get("disruptions")
                    if isinstance(ds, list):
                        out.extend([_normalize_disruption(x) for x in ds if isinstance(x, dict)])
                return out
            return []

        candidate_paths: list[tuple[str, dict[str, str] | None]] = [
            # Direct endpoints (per your PRIM "accès générique" doc)
            (f"/stop_areas/{sa}/disruptions", {"depth": "2"}),
            (f"/stop_areas/{sa}/traffic_reports", {"depth": "2"}),
            # Coverage-scoped variants (some PRIM products use them)
            (f"/coverage/{self.coverage}/stop_areas/{sa}/disruptions", {"depth": "2"}),
            (f"/coverage/{self.coverage}/stop_areas/{sa}/traffic_reports", {"depth": "2"}),
            # Global fallbacks
            ("/disruptions", {"depth": "1"}),
            ("/traffic_reports", {"depth": "1"}),
            (f"/coverage/{self.coverage}/disruptions", {"depth": "1"}),
            (f"/coverage/{self.coverage}/traffic_reports", {"depth": "1"}),
        ]
        last_exc: Exception | None = None
        for path, params in candidate_paths:
            try:
                data = await self._get(path, params or None)
                return _extract_disruptions(data)
            except httpx.HTTPStatusError as e:
                last_exc = e
                # 404: endpoint not available. 400: endpoint exists but params/path not accepted for this product.
                if e.response.status_code in (400, 404):
                    continue
                raise
        if last_exc:
            raise last_exc
        return []

    async def _get_journeys(self, *, from_id: str, to_id: str) -> dict:
        # PRIM exposes /journeys directly under the Navitia base path (no coverage prefix needed)
        return await self._get(
            "/journeys",
            {
                "from": from_id,
                "to": to_id,
                "count": "3",
                "depth": "1",
            },
        )

    def _journey_section_line_id(self, s: dict) -> str | None:
        """L'id ligne est parfois seulement dans `links` ou dans display_informations."""
        lo = s.get("line")
        if isinstance(lo, dict) and lo.get("id"):
            return str(lo["id"])
        for lk in s.get("links") or []:
            if not isinstance(lk, dict):
                continue
            if str(lk.get("type") or "").lower() in ("line", "lines") and lk.get("id"):
                return str(lk["id"])
        di = s.get("display_informations") or s.get("pt_display_informations") or {}
        if isinstance(di, dict):
            lo2 = di.get("line")
            if isinstance(lo2, dict) and lo2.get("id"):
                return str(lo2["id"])
        return None

    def _journey_section_headsign(self, s: dict) -> str | None:
        di = s.get("display_informations") or s.get("pt_display_informations") or {}
        if not isinstance(di, dict):
            return None
        for key in ("direction", "headsign", "heading"):
            v = di.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    def _headsigns_from_journeys_payload(self, data: dict, *, line_id: str) -> list[str]:
        hints: list[str] = []
        seen: set[str] = set()
        for j in data.get("journeys") or []:
            if not isinstance(j, dict):
                continue
            for s in j.get("sections") or []:
                if not isinstance(s, dict):
                    continue
                if (s.get("type") or "").lower() != "public_transport":
                    continue
                sid = self._journey_section_line_id(s)
                if not sid or sid != line_id:
                    continue
                direction = self._journey_section_headsign(s)
                if direction:
                    dlow = direction.lower()
                    if dlow not in seen:
                        seen.add(dlow)
                        hints.append(direction)
        return hints

    async def headsings_toward_destination(
        self,
        *,
        stop_area_id: str,
        line_id: str,
        destination_stop_area_id: str,
    ) -> list[str]:
        """
        Collecte les têtes de ligne possibles pour rejoindre `destination` depuis `stop_area`
        sur `line_id`, d'abord via /journeys, puis repli sur les routes à l'arrêt.

        Peut être vide si l'API ne propose aucun itinéraire (horaires, travaux, paramètres).
        """
        base_q: dict[str, str] = {
            "from": stop_area_id,
            "to": destination_stop_area_id,
            "count": "50",
            "depth": "2",
        }
        variants: list[dict[str, str]] = [
            {**base_q, "allowed_id[]": line_id},
            dict(base_q),
        ]
        paths = ["/journeys", f"/coverage/{self.coverage}/journeys"]

        for path in paths:
            for q in variants:
                try:
                    data = await self._get(path, q)
                except httpx.HTTPStatusError:
                    continue
                hints = self._headsigns_from_journeys_payload(data, line_id=line_id)
                if hints:
                    return hints

        # Repli : toutes les directions (terminus) des routes de cette ligne à cette station.
        # Moins précis que /journeys mais évite l'échec quand PRIM ne calcule pas d'itinéraire.
        try:
            routes_dirs = await self.list_directions_for_stop_area_line(
                stop_area_id=stop_area_id, line_id=line_id
            )
            if routes_dirs:
                return routes_dirs
        except Exception:
            pass

        # Dernier repli : têtes observées sur les prochains départs (temps réel).
        try:
            sa = self._enc(stop_area_id)
            data = await self._get(
                f"/stop_areas/{sa}/departures",
                {"depth": "2", "count": "80"},
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                try:
                    data = await self._get(
                        f"/coverage/{self.coverage}/stop_areas/{sa}/departures",
                        {"depth": "2", "count": "80"},
                    )
                except httpx.HTTPStatusError:
                    return []
            else:
                raise
        seen2: set[str] = set()
        out2: list[str] = []
        for dep in (data.get("departures") or []):
            if not isinstance(dep, dict):
                continue
            di = dep.get("display_informations") or {}
            if not isinstance(di, dict):
                continue
            dep_line = di.get("line")
            lid = None
            if isinstance(dep_line, dict) and dep_line.get("id"):
                lid = str(dep_line["id"])
            if lid != line_id:
                continue
            h = di.get("direction") or di.get("headsign") or di.get("to")
            if isinstance(h, str) and h.strip():
                k = h.strip().lower()
                if k not in seen2:
                    seen2.add(k)
                    out2.append(h.strip())
        return out2

    async def list_lines_for_stop_area(self, *, stop_area_id: str) -> list[tuple[str, str, str]]:
        """
        Returns [(line_id, label, commercial_mode_name)] for lines serving the given stop_area.
        Label is a human-readable "code name" when available (e.g. "M 12", "Bus 389").
        """
        sa = self._enc(stop_area_id)
        try:
            data = await self._get(f"/stop_areas/{sa}/lines", {"depth": "1"})
        except httpx.HTTPStatusError as e:
            # Fallback: some products require coverage prefix
            if e.response.status_code in (400, 404):
                data = await self._get(f"/coverage/{self.coverage}/stop_areas/{sa}/lines", {"depth": "1"})
            else:
                raise
        out: list[tuple[str, str, str]] = []
        for ln in (data.get("lines") or []):
            if not isinstance(ln, dict):
                continue
            lid = ln.get("id")
            if not lid:
                continue
            name = str(ln.get("name") or "").strip()
            code = str(ln.get("code") or "").strip()
            commercial = (ln.get("commercial_mode") or {}).get("name") if isinstance(ln.get("commercial_mode"), dict) else None
            commercial_s = str(commercial or "").strip()
            label_parts = []
            if commercial:
                label_parts.append(str(commercial))
            if code:
                label_parts.append(code)
            if name and name not in label_parts:
                label_parts.append(name)
            label = " ".join(label_parts) or name or code or str(lid)
            out.append((str(lid), label, commercial_s))
        # de-dupe
        seen: set[str] = set()
        uniq: list[tuple[str, str, str]] = []
        for lid, lab, cm in out:
            if lid in seen:
                continue
            seen.add(lid)
            uniq.append((lid, lab, cm))
        return uniq

    async def list_directions_for_stop_area_line(
        self, *, stop_area_id: str, line_id: str
    ) -> list[str]:
        """
        Returns a list of possible directions (headsigns) for a given line at a stop_area.

        Important: we must not rely only on upcoming departures, because when a direction
        is temporarily interrupted there might be *zero* departures, which would hide
        that direction. We therefore use `/stop_areas/{sa}/routes` which lists routes
        independently from real-time departures.
        """
        sa = self._enc(stop_area_id)
        try:
            data = await self._get(f"/stop_areas/{sa}/routes", {"depth": "2", "count": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                data = await self._get(
                    f"/coverage/{self.coverage}/stop_areas/{sa}/routes",
                    {"depth": "2", "count": "200"},
                )
            else:
                raise

        routes = data.get("routes") or []
        directions: list[str] = []
        for r in routes:
            if not isinstance(r, dict):
                continue
            line_obj = r.get("line")
            if not isinstance(line_obj, dict):
                continue
            rid = line_obj.get("id")
            if not rid or str(rid) != line_id:
                continue
            direction_obj = r.get("direction") or {}
            if not isinstance(direction_obj, dict):
                continue
            name = direction_obj.get("name")
            if not isinstance(name, str):
                continue
            h = name.strip()
            if h:
                directions.append(h)

        # unique, keep order
        seen_h: set[str] = set()
        uniq_h: list[str] = []
        for h in directions:
            if h in seen_h:
                continue
            seen_h.add(h)
            uniq_h.append(h)
        return uniq_h

    async def get_next_departures(
        self,
        *,
        stop_area_id: str,
        line_id: str | None = None,
        destination_stop_area_id: str | None = None,
        direction_label: str | None = None,
        direction_hints: list[str] | None = None,
        count: int = 3,
    ) -> list[str]:
        """
        Returns a list of human-readable strings for the next departures
        at a given stop_area, optionally filtered by line and direction.
        """
        sa = self._enc(stop_area_id)
        count_param = str(max(count * 20, 80))

        async def _fetch_departures_payload(*, data_freshness: str) -> dict:
            params = {"depth": "2", "count": count_param, "data_freshness": data_freshness}
            try:
                return await self._get(f"/stop_areas/{sa}/departures", params)
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (400, 404):
                    return await self._get(f"/coverage/{self.coverage}/stop_areas/{sa}/departures", params)
                raise

        async def _fetch_realtime_with_retry() -> dict | None:
            # Retry on transient API failures and occasional empty payloads.
            attempts = self.realtime_departures_retries + 1
            for idx in range(attempts):
                try:
                    payload = await _fetch_departures_payload(data_freshness="realtime")
                    if payload.get("departures"):
                        return payload
                    if idx == attempts - 1:
                        return payload
                except (httpx.RequestError, httpx.HTTPStatusError):
                    if idx == attempts - 1:
                        return None
                await asyncio.sleep(0.35 * (idx + 1))
            return None

        realtime_data = await _fetch_realtime_with_retry()
        hints_raw = [str(h).strip() for h in (direction_hints or []) if str(h).strip()]
        legacy_dir = (direction_label or "").strip()
        now_local = datetime.now(ZoneInfo("Europe/Paris"))

        def _dir_matches(blob_txt: str, label: str) -> bool:
            blob_norm = blob_txt.lower().replace("-", " ")
            core = label.split("(", 1)[0].strip().lower().replace("-", " ")
            parts = [p.strip() for p in re.split(r"[-/]", core) if p.strip()]
            tokens: list[str] = []
            for p in parts:
                for t in p.split():
                    t = t.strip().lower()
                    if t in {"saint", "st"}:
                        continue
                    if len(t) >= 3:
                        tokens.append(t)
            return any(tok in blob_norm for tok in tokens)

        def _any_hint_matches(blob_norm: str, hints: list[str]) -> bool:
            return any(_dir_matches(blob_norm, h) for h in hints)

        def _headsign_matches_user_hints(hints: list[str], direction_board: str) -> bool:
            """Alignement quai / config : sous-chaîne ou tokens (ex. Juvisy vs Juvisy-sur-Orge)."""
            dnorm = direction_board.strip().lower()
            for h in hints:
                hn = h.strip().lower()
                if not hn:
                    continue
                if hn in dnorm or dnorm in hn:
                    return True
                if _dir_matches(dnorm, h):
                    return True
            return False

        wanted_dest_id = (destination_stop_area_id or "").strip().lower()

        def _departure_matches_destination_id(dep: dict) -> bool:
            if not wanted_dest_id:
                return False

            def _collect_ids(obj: object) -> list[str]:
                out: list[str] = []
                if isinstance(obj, dict):
                    maybe_id = obj.get("id")
                    if isinstance(maybe_id, str) and maybe_id.strip():
                        out.append(maybe_id.strip().lower())
                    for v in obj.values():
                        out.extend(_collect_ids(v))
                elif isinstance(obj, list):
                    for it in obj:
                        out.extend(_collect_ids(it))
                return out

            # Prefer route/direction areas, but keep a broad fallback to tolerate
            # shape differences across PRIM products.
            scoped: list[object] = [
                dep.get("route"),
                dep.get("stop_point"),
                dep.get("display_informations"),
                dep.get("links"),
            ]
            ids: list[str] = []
            for s in scoped:
                ids.extend(_collect_ids(s))
            if not ids:
                ids = _collect_ids(dep)
            return wanted_dest_id in set(ids)

        labels_for_resume = hints_raw if hints_raw else ([legacy_dir] if legacy_dir else [])

        # If an active disruption indicates a traffic interruption with an estimated
        # resume time, we suppress departures before that resume time.
        resume_dt_local: datetime | None = None
        if line_id and labels_for_resume:
            try:
                disruptions = await self._get_disruptions_for_line(line_id)
                if disruptions:
                    # keep only active disruptions
                    disruptions = [d for d in disruptions if self._is_active_disruption(d)]
                for d in disruptions or []:
                    # Build a text blob to regex "Reprise estimée : HH:MM"
                    msgs = d.get("messages")
                    texts: list[str] = []
                    if isinstance(msgs, list):
                        for m in msgs:
                            if isinstance(m, dict) and m.get("text"):
                                texts.append(str(m["text"]))
                    blob = "\n".join(texts).lower()
                    blob_norm = blob.replace("-", " ")
                    whole_line = any(
                        phrase in blob_norm
                        for phrase in [
                            "sur toute la ligne",
                            "toute la ligne",
                            "dans les deux sens",
                            "dans les 2 sens",
                            "en deux sens",
                            "en 2 sens",
                            "in both directions",
                        ]
                    )
                    # We only apply suppression when disruption text indicates
                    # an interruption and we have a matching direction.
                    if "interromp" not in blob_norm and "reprise estim" not in blob_norm:
                        continue

                    if not whole_line and not _headsign_matches_user_hints(labels_for_resume, blob_norm):
                        continue

                    # 1) "Reprise estimée : HH:MM"
                    m = re.search(r"reprise\s*estim[^0-9]{0,25}(\d{1,2}:\d{2})", blob_norm)
                    if m:
                        hhmm = m.group(1)
                        hh, mm = hhmm.split(":", 1)
                        hh_i = int(hh)
                        mm_i = int(mm)
                        resume_dt_local = now_local.replace(hour=hh_i, minute=mm_i, second=0, microsecond=0)
                        if resume_dt_local < now_local - timedelta(hours=1):
                            resume_dt_local = resume_dt_local + timedelta(days=1)
                        break

                    # 2) "interrompu de 14h à 15h30" (minutes optional)
                    # Use end of range as suppression boundary.
                    m2 = re.search(
                        r"(\d{1,2})h(?:(\d{2}))?\s*(?:à|a)\s*(\d{1,2})h(?:(\d{2}))?",
                        blob_norm,
                    )
                    if m2:
                        # start_h, start_m, end_h, end_m
                        sh = int(m2.group(1))
                        sm = int(m2.group(2) or "00")
                        eh = int(m2.group(3))
                        em = int(m2.group(4) or "00")
                        # end of interruption
                        resume_dt_local = now_local.replace(hour=eh, minute=em, second=0, microsecond=0)
                        if resume_dt_local < now_local - timedelta(hours=1):
                            resume_dt_local = resume_dt_local + timedelta(days=1)
                        break
            except Exception:
                resume_dt_local = None

        def _parse_hhmm(v: str) -> str:
            """
            Tries to extract "HH:MM" from various Navitia formats:
            - "HHMMSS" (e.g. 081530)
            - "HH:MM", "HH:MM:SS"
            - "08h45" / "8h45"
            - ISO datetime strings containing a time part
            """
            s = v.strip()
            if not s:
                return ""
            if s.isdigit():
                if len(s) == 4:  # HHMM
                    return f"{s[0:2]}:{s[2:4]}"
                if len(s) == 5:  # best-effort, e.g. HMMSS without leading 0
                    s2 = "0" + s
                    return f"{s2[0:2]}:{s2[2:4]}"
                if len(s) == 6:  # HHMMSS
                    return f"{s[0:2]}:{s[2:4]}"
            # Direct HH:MM
            m = re.search(r"(?<!\d)(\d{2}):(\d{2})(?!\d)", s)
            if m:
                return f"{m.group(1)}:{m.group(2)}"
            # HHhMM
            m = re.search(r"(?<!\d)(\d{1,2})h(\d{2})(?!\d)", s, flags=re.IGNORECASE)
            if m:
                hh = int(m.group(1))
                mm = m.group(2)
                return f"{hh:02d}:{mm}"
            # HHMMSS / HHMM
            m = re.search(r"(?<!\d)(\d{6})(?!\d)", s)
            if m:
                blob = m.group(1)
                return f"{blob[0:2]}:{blob[2:4]}"
            m = re.search(r"(?<!\d)(\d{4})(?!\d)", s)
            if m:
                blob = m.group(1)
                return f"{blob[0:2]}:{blob[2:4]}"
            return ""

        def scan(
            *,
            payload: dict,
            apply_direction_filter: bool,
            used_schedule_fallback: bool,
        ) -> list[str]:
            seen_local: set[str] = set()
            acc: list[str] = []
            for dep in (payload.get("departures") or []):
                if not isinstance(dep, dict):
                    continue
                di = dep.get("display_informations") or {}
                if not isinstance(di, dict):
                    di = {}
                # line filter
                dep_line_id: str | None = None
                maybe_line = di.get("line")
                if isinstance(maybe_line, dict) and maybe_line.get("id"):
                    dep_line_id = str(maybe_line["id"])

                # Fallback extraction when display_informations doesn't include line.id
                if not dep_line_id:
                    route_obj = dep.get("route") or {}
                    if isinstance(route_obj, dict):
                        line_obj = route_obj.get("line") or {}
                        if isinstance(line_obj, dict) and line_obj.get("id"):
                            dep_line_id = str(line_obj["id"])
                if not dep_line_id:
                    for lk in dep.get("links") or []:
                        if isinstance(lk, dict) and lk.get("id") and lk.get("type") in ("line", "lines"):
                            dep_line_id = str(lk["id"])
                            break

                if line_id:
                    # strict: if we can't infer the departure line, skip it
                    if not dep_line_id or dep_line_id != line_id:
                        continue
                # direction filter
                direction = di.get("direction") or di.get("headsign") or di.get("to")
                direction_str = str(direction).strip() if isinstance(direction, str) else ""
                if apply_direction_filter:
                    if wanted_dest_id:
                        # For branched lines (RER), keep all missions that truly head
                        # toward the configured destination stop area.
                        if not _departure_matches_destination_id(dep):
                            # Fallback to textual hint if destination IDs are absent.
                            if hints_raw:
                                if not direction_str or not _headsign_matches_user_hints(hints_raw, direction_str):
                                    continue
                            elif legacy_dir:
                                if not direction_str or not _dir_matches(direction_str, legacy_dir):
                                    continue
                    else:
                        if hints_raw:
                            if not direction_str or not _headsign_matches_user_hints(hints_raw, direction_str):
                                continue
                        elif legacy_dir:
                            if not direction_str or not _dir_matches(direction_str, legacy_dir):
                                continue
                # time — priorité à l'heure temps réel puis théorique
                dt = dep.get("stop_date_time") or {}
                if isinstance(dt, dict):
                    time_str = dt.get("departure_time") or dt.get("arrival_time") or ""
                    # Try to parse full datetime to compute "dans X minutes"
                    dep_dt = None
                    # Realtime first. Only include base_* fields when we explicitly
                    # switched to base_schedule fallback.
                    dt_candidates: list[object] = [
                        dep.get("amended_departure_date_time"),
                        dep.get("amended_departureDateTime"),
                        dep.get("amended_departure_datetime"),
                        dep.get("amended_departureDatetime"),
                        dep.get("departure_date_time"),
                        dep.get("departureDateTime"),
                        dep.get("departure_datetime"),
                        dep.get("departureDatetime"),
                        dt.get("amended_departure_date_time"),
                        dt.get("amended_departureDateTime"),
                        dt.get("amended_departure_datetime"),
                        dt.get("amended_departureDatetime"),
                        dt.get("departure_date_time"),
                        dt.get("departureDateTime"),
                        dt.get("departure_datetime"),
                        dt.get("departureDatetime"),
                        dt.get("utc_departure_date_time"),
                        dt.get("date_time"),
                    ]
                    if used_schedule_fallback:
                        dt_candidates.extend(
                            [
                                dep.get("base_departure_date_time"),
                                dep.get("base_departureDateTime"),
                                dep.get("base_departure_datetime"),
                                dt.get("base_departure_date_time"),
                                dt.get("base_departureDateTime"),
                                dt.get("base_departure_datetime"),
                            ]
                        )
                    dt_candidates.append(time_str)
                    for cand in dt_candidates:
                        dep_dt = self._parse_navitia_datetime(cand)
                        if dep_dt is not None:
                            break
                else:
                    time_str = ""
                    dep_dt = None

                human_time = ""
                time_part = ""
                if isinstance(time_str, str) and time_str.strip():
                    human_time = _parse_hhmm(time_str)
                elif time_str not in ("", None):
                    # Sometimes Navitia returns a non-string here; stringify best-effort.
                    human_time = _parse_hhmm(str(time_str))

                dep_dt_local_cmp: datetime | None = None
                if dep_dt is not None and dep_dt.tzinfo is not None:
                    dep_dt_local_cmp = dep_dt.astimezone(ZoneInfo("Europe/Paris"))
                elif human_time:
                    try:
                        hh_i, mm_i = [int(x) for x in human_time.split(":")]
                        dep_dt_local_cmp = now_local.replace(hour=hh_i, minute=mm_i, second=0, microsecond=0)
                    except Exception:
                        dep_dt_local_cmp = None
                if dep_dt_local_cmp is not None and dep_dt_local_cmp < now_local - timedelta(seconds=45):
                    continue

                if dep_dt is not None and dep_dt.tzinfo is not None:
                    # Prefer absolute time for readability
                    local_hhmm = dep_dt.astimezone(ZoneInfo("Europe/Paris")).strftime("%H:%M")
                    time_part = f"à {local_hhmm}"

                    # Suppress departures before estimated resume time
                    if resume_dt_local is not None:
                        dep_dt_local = dep_dt.astimezone(ZoneInfo("Europe/Paris"))
                        if dep_dt_local < resume_dt_local:
                            continue
                if not time_part:
                    # Fallback: use parsed time string if available
                    if human_time:
                        time_part = f"à {human_time}"
                    else:
                        time_part = ""
                mode = di.get("physical_mode") or di.get("commercial_mode") or ""
                line_code = di.get("code") or di.get("label") or ""
                desc_parts = [
                    part
                    for part in [
                        time_part,
                        str(mode).strip(),
                        str(line_code).strip(),
                        direction_str,
                    ]
                    if part
                ]
                if not desc_parts:
                    continue
                rendered = " ".join(desc_parts)
                if used_schedule_fallback:
                    rendered = f"{rendered} [PLANNING]"
                if rendered in seen_local:
                    continue
                seen_local.add(rendered)
                acc.append(rendered)
                if len(acc) >= count:
                    break
            return acc

        realtime_results: list[str] = []
        if realtime_data and realtime_data.get("departures"):
            realtime_results = scan(
                payload=realtime_data,
                apply_direction_filter=True,
                used_schedule_fallback=False,
            )
            if realtime_results:
                return realtime_results

        if not self.allow_planning_fallback:
            return []

        planning_data = await _fetch_departures_payload(data_freshness="base_schedule")
        return scan(
            payload=planning_data,
            apply_direction_filter=True,
            used_schedule_fallback=True,
        )

    def _mode_bucket(self, physical_mode: str | None, commercial_mode: str | None) -> str | None:
        blob = f"{physical_mode or ''} {commercial_mode or ''}".lower()
        if "metro" in blob:
            return "Metro"
        if "rer" in blob:
            return "RER"
        if "train" in blob:
            return "Train"
        if "tram" in blob:
            return "Tram"
        if "bus" in blob:
            return "Bus"
        return None

    def _extract_lines_from_journeys(self, data: dict) -> list[tuple[str, str]]:
        """
        Returns [(line_id, mode_bucket)] from journeys sections.
        """
        out: list[tuple[str, str]] = []
        for j in (data.get("journeys") or []):
            for s in (j.get("sections") or []):
                if not isinstance(s, dict):
                    continue
                # Only public transport legs
                stype = (s.get("type") or "").lower()
                if stype and stype not in ("public_transport", "crow_fly", "street_network"):
                    # keep only PT
                    pass
                di = s.get("display_informations") or s.get("pt_display_informations") or {}
                if not isinstance(di, dict):
                    di = {}
                physical = di.get("physical_mode")
                commercial = di.get("commercial_mode")
                bucket = self._mode_bucket(
                    physical if isinstance(physical, str) else None,
                    commercial if isinstance(commercial, str) else None,
                )

                # Find line id in links
                links = []
                for k in ("links",):
                    v = s.get(k)
                    if isinstance(v, list):
                        links.extend(v)
                v2 = di.get("links")
                if isinstance(v2, list):
                    links.extend(v2)

                line_id = None
                for lk in links:
                    if not isinstance(lk, dict):
                        continue
                    if lk.get("type") in ("line", "lines") and lk.get("id"):
                        line_id = str(lk["id"])
                        break
                    # sometimes the rel is "lines"
                    if lk.get("rel") in ("lines", "line") and lk.get("id"):
                        line_id = str(lk["id"])
                        break

                # Fallback: sometimes section embeds a "line" object
                if not line_id:
                    maybe_line = s.get("line")
                    if isinstance(maybe_line, dict) and maybe_line.get("id"):
                        line_id = str(maybe_line["id"])

                if line_id and bucket:
                    out.append((line_id, bucket))

        # dedupe keep order
        seen: set[str] = set()
        uniq: list[tuple[str, str]] = []
        for lid, mb in out:
            if lid in seen:
                continue
            seen.add(lid)
            uniq.append((lid, mb))
        return uniq

    async def _get_disruptions_for_line(self, line_id: str) -> list[dict]:
        lid = self._enc(line_id)

        def _normalize_disruption(d: dict) -> dict:
            inner = d.get("disruption")
            if isinstance(inner, dict):
                return inner
            return d

        def _extract(data: dict) -> list[dict]:
            disruptions = data.get("disruptions")
            if isinstance(disruptions, list):
                return [_normalize_disruption(x) for x in disruptions if isinstance(x, dict)]
            traffic_reports = data.get("traffic_reports")
            if isinstance(traffic_reports, list):
                out: list[dict] = []
                for tr in traffic_reports:
                    ds = tr.get("disruptions")
                    if isinstance(ds, list):
                        out.extend([_normalize_disruption(x) for x in ds if isinstance(x, dict)])
                return out
            return []

        candidate_paths: list[tuple[str, dict[str, str] | None]] = [
            (f"/lines/{lid}/traffic_reports", {"depth": "2"}),
            (f"/coverage/{self.coverage}/lines/{lid}/traffic_reports", {"depth": "2"}),
            (f"/lines/{lid}/disruptions", {"depth": "2"}),
            (f"/coverage/{self.coverage}/lines/{lid}/disruptions", {"depth": "2"}),
        ]
        for path, params in candidate_paths:
            try:
                data = await self._get(path, params or None)
                return _extract(data)
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (400, 404):
                    continue
                raise
        return []

    def _parse_navitia_datetime(self, value: object) -> datetime | None:
        """
        Best-effort parsing for Navitia/PRIM disruption datetime fields.
        Returns a timezone-aware datetime in UTC when possible.
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            # Assume unix epoch seconds (best-effort)
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            except Exception:
                return None
        if not isinstance(value, str):
            return None

        s = value.strip()
        if not s:
            return None
        # Compact format: YYYYMMDDTHHMMSS (example: 20260319T165102)
        m = re.match(r"^(\d{8})T(\d{6})$", s)
        if m:
            try:
                # Interpret as Paris time, then convert to UTC for comparisons.
                tz = ZoneInfo("Europe/Paris")
                hh = int(m.group(2)[0:2])
                mm = int(m.group(2)[2:4])
                ss = int(m.group(2)[4:6])
                yyyy = int(m.group(1)[0:4])
                mo = int(m.group(1)[4:6])
                dd = int(m.group(1)[6:8])
                dt = datetime(yyyy, mo, dd, hh, mm, ss, tzinfo=tz)
                return dt.astimezone(timezone.utc)
            except Exception:
                return None

        # Common ISO formats: "...Z" or "...+01:00"
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
        if dt.tzinfo is None:
            # Navitia/PRIM returns some datetimes without timezone offset.
            # Assume local Paris time for consistent "active" filtering and deltas.
            dt = dt.replace(tzinfo=ZoneInfo("Europe/Paris"))
        return dt.astimezone(timezone.utc)

    def _is_active_disruption(self, d: dict) -> bool:
        """
        Filters out disruptions that are clearly finished or too old.

        PRIM payloads are not consistent: sometimes start/end fields exist,
        sometimes only one timestamp is available (or timestamps are missing).

        Goal: keep only "temporary" disruptions and avoid stale alerts.
        """
        now = datetime.now(timezone.utc)
        now_local = datetime.now(ZoneInfo("Europe/Paris"))
        margin = timedelta(minutes=5)
        max_age = timedelta(minutes=90)

        # PRIM payload "application_periods" can be inconsistent with what the
        # human message says (e.g. "Reprise estimée: 15:45" but begin/end window
        # is much shorter). For traffic interruptions, prefer message-derived
        # times first.
        messages = d.get("messages")
        blob = ""
        if isinstance(messages, list):
            parts: list[str] = []
            for m in messages:
                if isinstance(m, dict) and isinstance(m.get("text"), str):
                    parts.append(m["text"])
            blob = "\n".join(parts).lower()

        def _parse_resume_hhmm() -> datetime | None:
            # Example: "Reprise estimée : 15:45"
            m = re.search(r"reprise\s*estim[^0-9]{0,30}(\d{1,2}):(\d{2})", blob)
            if not m:
                return None
            hh = int(m.group(1))
            mm = int(m.group(2))
            dt = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if dt < now_local - timedelta(hours=1):
                dt = dt + timedelta(days=1)
            return dt

        resume_dt_local = None
        if "reprise" in blob and "estim" in blob:
            resume_dt_local = _parse_resume_hhmm()

        if resume_dt_local is not None:
            # Active until shortly after the estimated resume time
            return now_local <= resume_dt_local + timedelta(minutes=60)

        # interruption range: "interrompu de 14h à 15h30"
        if "interromp" in blob:
            m2 = re.search(
                r"(\d{1,2})h(?:(\d{2}))?\s*(?:à|a)\s*(\d{1,2})h(?:(\d{2}))?",
                blob,
            )
            if m2:
                sh = int(m2.group(1))
                sm = int(m2.group(2) or "00")
                eh = int(m2.group(3))
                em = int(m2.group(4) or "00")
                end_dt = now_local.replace(hour=eh, minute=em, second=0, microsecond=0)
                if end_dt < now_local - timedelta(hours=1):
                    end_dt = end_dt + timedelta(days=1)
                # consider active until shortly after the end of interruption
                return now_local <= end_dt + timedelta(minutes=15)

        start_candidates = [
            "start_date_time",
            "begin_date_time",
            "startDateTime",
            "beginDateTime",
            "effective_start",
            "effectiveStart",
            # fallback timestamps (best-effort)
            "created_at",
            "updated_at",
        ]
        end_candidates = [
            "end_date_time",
            "effective_end",
            "endDateTime",
            "effectiveEnd",
            "publication_date_time_end",
            "publicationDateTimeEnd",
        ]

        start: datetime | None = None
        for k in start_candidates:
            if k in d:
                start = self._parse_navitia_datetime(d.get(k))
                if start is not None:
                    break

        end: datetime | None = None
        for k in end_candidates:
            if k in d:
                end = self._parse_navitia_datetime(d.get(k))
                if end is not None:
                    break

        # Prefer application_periods when present (most reliable in our payload).
        # application_periods: [{"begin": "...", "end": "..."}]
        periods = d.get("application_periods")
        if isinstance(periods, list) and periods:
            for p in periods:
                if not isinstance(p, dict):
                    continue
                begin = self._parse_navitia_datetime(p.get("begin"))
                end2 = self._parse_navitia_datetime(p.get("end"))
                # If parsing fails for this period, ignore it.
                if begin is None and end2 is None:
                    continue
                # Active if now is within window (with margin).
                if begin is not None and end2 is not None:
                    if begin - margin <= now <= end2 + margin:
                        return True
                    continue
                # Only one side parsed: be conservative.
                if begin is not None and end2 is None:
                    if now >= begin - margin and now <= begin + max_age:
                        return True
                    continue
                if begin is None and end2 is not None:
                    if now <= end2 + margin:
                        return True
                    continue
            # No period matched -> not active.
            return False

        # If we cannot determine a usable window, don't show it.
        if start is None and end is None:
            return False

        if start is not None and start > now + margin:
            return False
        if end is not None and end < now - margin:
            return False
        # If we only have a start timestamp, drop very old entries.
        if end is None and start is not None and start < now - max_age:
            return False
        return True

    async def get_status(self, *, depart: str, direction: str | None) -> TransitStatus:
        try:
            depart_sa = await self._resolve_stop_area(depart)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                return TransitStatus(
                    ok=True,
                    headline="Transports: accès PRIM refusé (401/403).",
                    details="Vérifiez `IDFM_PRIM_API_KEY` (token PRIM) et que l’API Navitia est bien autorisée sur votre compte PRIM.",
                )
            if e.response.status_code == 404:
                return TransitStatus(
                    ok=True,
                    headline="Transports: endpoint PRIM introuvable (404).",
                    details="Souvent: API non souscrite/activée sur PRIM, ou URL marketplace différente. Vérifiez que vous êtes abonné à l’API “IDFM Navitia (v2)” sur PRIM.",
                )
            return TransitStatus(
                ok=True,
                headline=f"Transports: erreur PRIM ({e.response.status_code}).",
                details="Réessayez plus tard.",
            )
        except Exception:
            return TransitStatus(
                ok=True,
                headline="Transports: erreur inattendue.",
                details="Réessayez plus tard.",
            )
        if not depart_sa:
            return TransitStatus(
                ok=True,
                headline="Transports: station introuvable.",
                details=f"Je n'ai pas trouvé '{depart}' (coverage {self.coverage}).",
            )
        depart_id, depart_name = depart_sa.id, depart_sa.label

        dest: IdFmPrimNavitiaProvider.StopArea | None = None
        if direction and direction.strip():
            # Interpret "direction" as destination stop_area (e.g. La Défense)
            try:
                dest = await self._resolve_stop_area(direction.strip())
            except Exception:
                dest = None

        try:
            disruptions = await self._get_disruptions_for_stop_area(depart_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                return TransitStatus(
                    ok=True,
                    headline="Transports: accès PRIM refusé (401/403).",
                    details="Vérifiez `IDFM_PRIM_API_KEY` (token PRIM) et que l’API Navitia est bien autorisée sur votre compte PRIM.",
                )
            if e.response.status_code == 404:
                return TransitStatus(
                    ok=True,
                    headline="Transports: endpoint PRIM introuvable (404).",
                    details="Souvent: l’API d’info-trafic (disruptions/line_reports) n’est pas activée sur votre compte PRIM.",
                )
            if e.response.status_code == 400:
                return TransitStatus(
                    ok=True,
                    headline="Transports: requête PRIM invalide (400).",
                    details="Je vais devoir ajuster l’endpoint/paramètres côté PRIM. (Astuce: les IDs doivent être encodés.)",
                )
            return TransitStatus(
                ok=True,
                headline=f"Transports: erreur PRIM ({e.response.status_code}).",
                details="Réessayez plus tard.",
            )

        # Temporal filtering: keep only disruptions that are likely "still active".
        # This reduces stale alerts (works/incidents already resolved).
        if disruptions:
            disruptions = [d for d in disruptions if self._is_active_disruption(d)]

        if not disruptions:
            if dest:
                return TransitStatus(
                    ok=True,
                    headline=f"Transports (entre {depart_name} et {dest.label}): aucune perturbation connue.",
                )
            return TransitStatus(ok=True, headline=f"Transports ({depart_name}): aucune perturbation connue.")

        def _title_message(d: dict) -> tuple[str, str]:
            title = (d.get("title") or "").strip()
            message = (d.get("message") or "").strip()

            if not title:
                # Some Navitia payloads use "cause"/"severity" without a title
                sev = d.get("severity")
                if isinstance(sev, str) and sev:
                    title = sev

            if not message:
                msgs = d.get("messages")
                if isinstance(msgs, list):
                    texts: list[str] = []
                    for m in msgs:
                        if isinstance(m, dict) and m.get("text"):
                            texts.append(str(m["text"]).strip())
                    message = "\n".join([t for t in texts if t])

            if not title:
                title = "Perturbation"
            return title, message

        wanted = (direction or "").strip().lower()
        kept: list[tuple[str, str]] = []
        for d in disruptions:
            title, message = _title_message(d)
            blob = (title + "\n" + message).lower()
            if wanted and wanted not in blob:
                continue
            kept.append((title, message))

        use = kept or [_title_message(d) for d in disruptions]

        # Deduplicate identical entries (common with traffic_reports)
        seen_tm: set[tuple[str, str]] = set()
        deduped: list[tuple[str, str]] = []
        for t, m in use:
            key = (t.strip(), m.strip())
            if key in seen_tm:
                continue
            seen_tm.add(key)
            deduped.append((t, m))
        use = deduped

        lines = []
        for i, (t, m) in enumerate(use[:5], start=1):
            lines.append(f"{i}. {t}".strip())
            if m:
                lines.append(m)
        details = "\n".join(lines).strip() or None

        if dest:
            headline = f"Transports (entre {depart_name} et {dest.label}): perturbations détectées."
        else:
            headline = f"Transports ({depart_name}): perturbations détectées."
        return TransitStatus(
            ok=False,
            headline=headline,
            details=details,
        )

    async def get_trip_status(
        self,
        *,
        depart_sa_id: str,
        depart_sa_label: str,
        arrivee_sa_id: str | None,
        arrivee_sa_label: str | None,
        allowed_modes: str | None,
    ) -> TransitStatus:
        """
        Variant used by the bot when stop_area IDs are already known.
        If arrival is known, we compute a journey A->B, extract the lines actually used (with changes),
        then query traffic_reports for those lines only, and filter by selected modes.
        """
        allowed: set[str] = set()
        if allowed_modes and allowed_modes.strip():
            allowed = {x.strip() for x in allowed_modes.split(",") if x.strip()}

        disruptions: list[dict] = []
        used_lines: list[tuple[str, str]] = []

        try:
            if arrivee_sa_id:
                j = await self._get_journeys(from_id=depart_sa_id, to_id=arrivee_sa_id)
                used_lines = self._extract_lines_from_journeys(j)
                if allowed:
                    used_lines = [(lid, mb) for (lid, mb) in used_lines if mb in allowed]

                for lid, _mb in used_lines:
                    disruptions.extend(await self._get_disruptions_for_line(lid))
            else:
                disruptions = await self._get_disruptions_for_stop_area(depart_sa_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                return TransitStatus(
                    ok=True,
                    headline="Transports: accès PRIM refusé (401/403).",
                    details="Vérifiez `IDFM_PRIM_API_KEY` (token PRIM) et que l’API Navitia est bien autorisée sur votre compte PRIM.",
                )
            return TransitStatus(ok=True, headline=f"Transports: erreur PRIM ({e.response.status_code}).", details="Réessayez plus tard.")
        except Exception:
            return TransitStatus(ok=True, headline="Transports: erreur inattendue.", details="Réessayez plus tard.")

        # Temporal filtering: keep only disruptions likely still active.
        if disruptions:
            disruptions = [d for d in disruptions if self._is_active_disruption(d)]

        if not disruptions:
            if arrivee_sa_label:
                extra = ""
                if arrivee_sa_id and used_lines and allowed:
                    extra = f"\nLignes (selon itinéraire): {', '.join([lid.split(':')[-1] for lid,_ in used_lines])}"
                return TransitStatus(ok=True, headline=f"Transports (entre {depart_sa_label} et {arrivee_sa_label}): aucune perturbation connue.{extra}")
            return TransitStatus(ok=True, headline=f"Transports ({depart_sa_label}): aucune perturbation connue.")

        def _strip_html(s: str) -> str:
            s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"</p\s*>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"<[^>]+>", "", s)
            s = re.sub(r"\n{3,}", "\n\n", s).strip()
            # de-duplicate identical lines (PRIM sometimes repeats blocks)
            raw_lines = [ln.strip() for ln in s.splitlines()]
            out_lines: list[str] = []
            seen: set[str] = set()
            for ln in raw_lines:
                if not ln:
                    out_lines.append("")
                    continue
                if ln in seen:
                    continue
                seen.add(ln)
                out_lines.append(ln)
            s = "\n".join(out_lines)
            return re.sub(r"\n{3,}", "\n\n", s).strip()

        def _infer_mode_from_title(title: str) -> str | None:
            t = title.strip().lower()
            if t.startswith("bus "):
                return "Bus"
            if t.startswith("tram") or t.startswith("tramway"):
                return "Tram"
            if t.startswith("métro") or t.startswith("metro"):
                return "Metro"
            if t.startswith("rer "):
                return "RER"
            if "transilien" in t or t.startswith("train"):
                return "Train"
            return None

        def _title_message(d: dict) -> tuple[str, str, str]:
            title = (d.get("title") or "").strip()
            message = (d.get("message") or "").strip()
            if not message:
                msgs = d.get("messages")
                if isinstance(msgs, list):
                    texts: list[str] = []
                    for m in msgs:
                        if isinstance(m, dict) and m.get("text"):
                            texts.append(str(m["text"]).strip())
                    message = "\n".join([t for t in texts if t])
            if not title:
                title = "Perturbation"
            cause = str(d.get("cause") or "").strip()
            return title, _strip_html(message), cause

        # Prefer delays/incidents; exclude works/closures when possible.
        def _is_probably_delay(t: str, m: str, cause: str) -> bool:
            blob = (t + "\n" + m).lower()
            # Ignore accessibility-only disruptions (doesn't prevent traffic circulation)
            if any(w in blob for w in ["ascenseur", "escalier", "escaliers", "escalier mécanique", "accessibilité", "accessibilite", "pmr"]):
                return False

            interruption_words = [
                "interromp",
                "interruption",
                "interrompu",
                "reprise",
                "dernier",
                "derniers",
                "remplacement",
                "itinéraires alternatifs",
                "trafic interrom",
                "trafic est interrom",
                "traffic interrupted",
                "interruption urgente",
            ]
            if any(w in blob for w in interruption_words):
                return True

            if cause.lower() == "works":
                return False
            if any(w in blob for w in ["travaux", "works", "fermé", "fermée", "closed", "station ferm", "arrêt(s) non desservi", "non desservi"]):
                return False
            return any(w in blob for w in ["retard", "retards", "delay", "delays", "ralenti", "perturbation", "traffic", "trafic"])

        items = [_title_message(d) for d in disruptions]
        if allowed:
            filtered: list[tuple[str, str, str]] = []
            for t, m, c in items:
                mode = _infer_mode_from_title(t)
                # If we can infer and it's not allowed, drop it.
                if mode and mode not in allowed:
                    continue
                filtered.append((t, m, c))
            items = filtered
        delayish = [(t, m, c) for (t, m, c) in items if _is_probably_delay(t, m, c)]
        use = delayish or items

        seen: set[tuple[str, str]] = set()
        deduped: list[tuple[str, str, str]] = []
        for t, m, c in use:
            key = (t.strip(), m.strip())
            if key in seen:
                continue
            seen.add(key)
            deduped.append((t, m, c))
        use = deduped

        lines: list[str] = []
        for i, (t, m, _c) in enumerate(use[:5], start=1):
            lines.append(f"{i}. {t}".strip())
            if m:
                lines.append(m)
        details = "\n".join(lines).strip() or None

        if arrivee_sa_label:
            headline = f"Transports (entre {depart_sa_label} et {arrivee_sa_label}): perturbations détectées."
        else:
            headline = f"Transports ({depart_sa_label}): perturbations détectées."
        return TransitStatus(ok=False, headline=headline, details=details)


    async def get_disruptions_for_line(
        self,
        *,
        line_id: str,
        direction_label: str | None,
        allowed_modes: str | None,
        direction_hints: list[str] | None = None,
    ) -> TransitStatus:
        """
        Returns only temporary disruptions for a specific line (optionally filtered by direction),
        to avoid leaking station incidents across other lines.
        """
        allowed: set[str] = set()
        if allowed_modes and allowed_modes.strip():
            allowed = {x.strip() for x in allowed_modes.split(",") if x.strip()}

        disruptions = await self._get_disruptions_for_line(line_id)

        # Temporal filtering: keep only disruptions likely still active.
        if disruptions:
            disruptions = [d for d in disruptions if self._is_active_disruption(d)]

        if not disruptions:
            return TransitStatus(ok=True, headline="Transports: aucune perturbation temporaire connue.")

        def _strip_html(s: str) -> str:
            s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"</p\s*>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"<[^>]+>", "", s)
            s = html.unescape(s)
            s = re.sub(r"\n{3,}", "\n\n", s).strip()
            raw_lines = [ln.strip() for ln in s.splitlines()]
            out_lines: list[str] = []
            seen: set[str] = set()
            for ln in raw_lines:
                if not ln:
                    out_lines.append("")
                    continue
                if ln in seen:
                    continue
                seen.add(ln)
                out_lines.append(ln)
            s = "\n".join(out_lines)
            return re.sub(r"\n{3,}", "\n\n", s).strip()

        def _infer_mode_from_title(title: str) -> str | None:
            t = title.strip().lower()
            if t.startswith("bus "):
                return "Bus"
            if t.startswith("tram") or t.startswith("tramway"):
                return "Tram"
            if t.startswith("métro") or t.startswith("metro"):
                return "Metro"
            if t.startswith("rer "):
                return "RER"
            if "transilien" in t or t.startswith("train"):
                return "Train"
            return None

        def _title_message(d: dict) -> tuple[str, str, str]:
            title = (d.get("title") or "").strip()
            message = (d.get("message") or "").strip()
            if not message:
                msgs = d.get("messages")
                if isinstance(msgs, list):
                    texts: list[str] = []
                    for m in msgs:
                        if isinstance(m, dict) and m.get("text"):
                            texts.append(str(m["text"]).strip())
                    message = "\n".join([t for t in texts if t])
            if not title:
                title = "Perturbation"
            cause = str(d.get("cause") or "").strip()
            return title, _strip_html(message), cause

        # Prefer delays/incidents; exclude works/closures when possible.
        def _is_probably_delay(t: str, m: str, cause: str) -> bool:
            blob = (t + "\n" + m).lower()
            # Ignore accessibility-only disruptions (doesn't prevent traffic circulation)
            if any(w in blob for w in ["ascenseur", "escalier", "escaliers", "escalier mécanique", "accessibilité", "accessibilite", "pmr"]):
                return False

            interruption_words = [
                "interromp",
                "interruption",
                "interrompu",
                "reprise",
                "dernier",
                "derniers",
                "remplacement",
                "itinéraires alternatifs",
                "trafic interrom",
                "trafic est interrom",
                "traffic interrupted",
                "interruption urgente",
                "interrompu de",
                "interrompue de",
            ]
            if any(w in blob for w in interruption_words):
                return True

            if cause.lower() == "works":
                return False
            if any(
                w in blob
                for w in [
                    "travaux",
                    "works",
                    "fermé",
                    "fermée",
                    "closed",
                    "station ferm",
                    "arrêt(s) non desservi",
                    "non desservi",
                ]
            ):
                return False
            return any(w in blob for w in ["retard", "retards", "delay", "delays", "ralenti", "perturbation", "traffic", "trafic"])

        def _dir_matches(blob_txt: str, label: str) -> bool:
            blob_norm = blob_txt.lower().replace("-", " ")
            core = label.split("(", 1)[0].strip().lower().replace("-", " ")
            parts = [p.strip() for p in re.split(r"[-/]", core) if p.strip()]
            tokens: list[str] = []
            for p in parts:
                for t in p.split():
                    t = t.strip().lower()
                    if t in {"saint", "st"}:
                        continue
                    if len(t) >= 3:
                        tokens.append(t)
            return any(tok in blob_norm for tok in tokens)

        hints = [str(h).strip() for h in (direction_hints or []) if str(h).strip()]
        legacy_l = (direction_label or "").strip()

        def _hint_matches_disruption(blob_txt: str, hint: str) -> bool:
            hn = hint.strip().lower()
            if not hn:
                return False
            b = blob_txt.lower()
            if hn in b:
                return True
            return _dir_matches(blob_txt, hint)

        items = [_title_message(d) for d in disruptions]
        filtered_dir: list[tuple[str, str, str]] = []
        for t, m, c in items:
            blob = (t + "\n" + m).lower()
            # When the disruption is explicitly "on the whole line" or "in both directions",
            # it should be shown regardless of the user's selected direction.
            whole_line = any(
                phrase in blob
                for phrase in [
                    "sur toute la ligne",
                    "toute la ligne",
                    "dans les deux sens",
                    "dans les 2 sens",
                    "dans les deux direction",
                    "dans les 2 direction",
                    "dans les 2 directions",
                    "en deux sens",
                    "en 2 sens",
                    "in both directions",
                    "entièrement sur toute la ligne",
                    "entièrement sur toute la ligne",
                ]
            )
            if hints:
                if not whole_line and not any(_hint_matches_disruption(blob, h) for h in hints):
                    continue
            elif legacy_l:
                if not whole_line and not _hint_matches_disruption(blob, legacy_l):
                    continue
            filtered_dir.append((t, m, c))
        # Ne pas retomber sur toutes les perturbations si le filtre direction n'a rien gardé :
        # sinon on affiche des messages non pertinents (ex. atelier / autre sens).
        if hints or legacy_l:
            items = filtered_dir
        else:
            items = filtered_dir or items

        # Apply mode filter
        if allowed:
            filtered_modes: list[tuple[str, str, str]] = []
            for t, m, c in items:
                mode = _infer_mode_from_title(t)
                if mode and mode not in allowed:
                    continue
                filtered_modes.append((t, m, c))
            items = filtered_modes

        delayish = [(t, m, c) for (t, m, c) in items if _is_probably_delay(t, m, c)]
        # Be strict: if nothing looks temporary, don't show any.
        use = delayish
        if not use:
            return TransitStatus(ok=True, headline="Transports: aucune perturbation temporaire connue.")

        seen: set[tuple[str, str]] = set()
        deduped: list[tuple[str, str, str]] = []
        for t, m, c in use:
            key = (t.strip(), m.strip())
            if key in seen:
                continue
            seen.add(key)
            deduped.append((t, m, c))
        use = deduped

        def _compact_disruption_message(title: str, message: str) -> str:
            """
            Keep only the actionable user-facing part (RATP-like):
            - one traffic line (prefer 'trafic perturbé/interrompu ...')
            - one motif line when present.
            """
            blob = f"{title}\n{message}".strip()
            raw_lines = [ln.strip() for ln in blob.splitlines() if ln.strip()]
            # remove boilerplate or routing helper text
            filtered = [
                ln
                for ln in raw_lines
                if "application idfm" not in ln.lower()
                and "calculateur d'itinéraire" not in ln.lower()
                and "merci de consulter" not in ln.lower()
                and ln.lower() != "perturbation"
            ]
            if not filtered:
                return ""

            traffic_line = ""
            motif_line = ""

            # 1) best traffic sentence
            for ln in filtered:
                low = ln.lower()
                if "trafic perturb" in low or "trafic interrom" in low:
                    traffic_line = ln
                    break
            if not traffic_line:
                for ln in filtered:
                    low = ln.lower()
                    if "interrompu entre" in low or "perturbé entre" in low:
                        traffic_line = ln
                        break
            if not traffic_line:
                traffic_line = filtered[0]

            # 2) motif line if available
            for ln in filtered:
                if "motif" in ln.lower():
                    motif_line = ln
                    break

            if motif_line and motif_line != traffic_line:
                return f"{traffic_line}\n\n{motif_line}".strip()
            return traffic_line.strip()

        def _candidate_score(compact_msg: str, title: str, message: str, cause: str) -> int:
            blob = f"{title}\n{message}\n{compact_msg}".lower()
            score = 0
            if hints:
                if any(_hint_matches_disruption(blob, h) for h in hints):
                    score += 6
            elif legacy_l and _hint_matches_disruption(blob, legacy_l):
                score += 6
            if "trafic perturb" in blob:
                score += 4
            if "jusqu'à fin de service" in blob or "jusqu a fin de service" in blob:
                score += 2
            if "motif :" in blob:
                score += 1
            if "trafic interrom" in blob:
                score -= 1
            if any(w in blob for w in ["travaux", "bus de remplacement", "merci de consulter"]):
                score -= 4
            if cause.strip().lower() == "works":
                score -= 4
            return score

        # Prefer the disruption that best matches the selected itinerary direction.
        compact_candidates: list[tuple[int, str]] = []
        for t, m, c in use:
            cm = _compact_disruption_message(t, m)
            if not cm:
                continue
            compact_candidates.append((_candidate_score(cm, t, m, c), cm.strip()))

        # de-dup compact outputs and keep only the highest-score concise message
        if compact_candidates:
            dedup_scores: dict[str, int] = {}
            for sc, msg in compact_candidates:
                if msg not in dedup_scores or sc > dedup_scores[msg]:
                    dedup_scores[msg] = sc
            best = sorted(dedup_scores.items(), key=lambda x: x[1], reverse=True)[0][0]
            details = best
        else:
            details = None

        headline = f"Transports (ligne {line_id}): perturbations temporaires détectées."
        return TransitStatus(ok=False, headline=headline, details=details)


def make_provider(
    *,
    idfm_prim_api_key: str | None,
    allow_planning_fallback: bool = True,
    realtime_departures_retries: int = 2,
) -> TransitProvider:
    if idfm_prim_api_key:
        return IdFmPrimNavitiaProvider(
            idfm_prim_api_key,
            allow_planning_fallback=allow_planning_fallback,
            realtime_departures_retries=realtime_departures_retries,
        )
    return NotConfiguredProvider()

