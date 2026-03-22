from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class UserPrefs:
    chat_id: int
    depart: str | None
    direction: str | None
    depart_sa_id: str | None
    depart_sa_label: str | None
    arrivee_sa_id: str | None
    arrivee_sa_label: str | None
    allowed_modes: str | None
    meteo_label: str | None
    meteo_lat: float | None
    meteo_lon: float | None
    # JSON-encoded list of segments for /depart + /changement_1..3
    segments_json: str | None
    # Notification time chosen via /heure_notif, format "HH:MM" in bot timezone
    notif_time: str | None
    # Last sent notification time key (format "YYYY-MM-DD HH:MM")
    last_notif_sent_key: str | None
    # Inclure l’extrait d’actu du jour dans la notification
    recevoir_news_du_jour: bool
    # Selected news category: "tech" | "sport" | "science"
    news_category: str | None
    # Comma-separated finance keys: sp500,cac40,btc,gold
    finance_selection: str | None


class Db:
    def __init__(self, path: str):
        self.path = str(Path(path))
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    depart TEXT,
                    direction TEXT,
                    depart_sa_id TEXT,
                    depart_sa_label TEXT,
                    arrivee_sa_id TEXT,
                    arrivee_sa_label TEXT,
                    allowed_modes TEXT,
                    meteo_label TEXT,
                    meteo_lat REAL,
                    meteo_lon REAL,
                    segments_json TEXT,
                    notif_time TEXT,
                    last_notif_sent_key TEXT,
                    recevoir_news_du_jour INTEGER,
                    news_category TEXT,
                    finance_selection TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
                """.strip()
            )

            # Lightweight migrations for existing DBs
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
            if "recevoir_news_du_jour" not in cols:
                if "recevoir_bonne_nouvelle" in cols:
                    conn.execute(
                        "ALTER TABLE users RENAME COLUMN recevoir_bonne_nouvelle TO recevoir_news_du_jour"
                    )
                else:
                    conn.execute("ALTER TABLE users ADD COLUMN recevoir_news_du_jour INTEGER")
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()}

            def add_col(name: str, ddl: str) -> None:
                if name not in cols:
                    conn.execute(f"ALTER TABLE users ADD COLUMN {ddl}")

            add_col("depart_sa_id", "depart_sa_id TEXT")
            add_col("depart_sa_label", "depart_sa_label TEXT")
            add_col("arrivee_sa_id", "arrivee_sa_id TEXT")
            add_col("arrivee_sa_label", "arrivee_sa_label TEXT")
            add_col("allowed_modes", "allowed_modes TEXT")
            add_col("segments_json", "segments_json TEXT")
            add_col("notif_time", "notif_time TEXT")
            add_col("last_notif_sent_key", "last_notif_sent_key TEXT")
            add_col("news_category", "news_category TEXT")
            add_col("finance_selection", "finance_selection TEXT")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS news_cache (
                    day TEXT PRIMARY KEY,
                    headline TEXT,
                    url TEXT,
                    fetched_at TEXT,
                    state TEXT DEFAULT 'ready'
                )
                """.strip()
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS finance_cache (
                    day TEXT PRIMARY KEY,
                    payload TEXT,
                    fetched_at TEXT,
                    state TEXT DEFAULT 'ready'
                )
                """.strip()
            )

    def upsert_user(self, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id) VALUES (?)
                ON CONFLICT(chat_id) DO UPDATE SET updated_at=datetime('now')
                """.strip(),
                (chat_id,),
            )

    def set_depart(self, chat_id: int, depart: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, depart) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    depart=excluded.depart,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, depart),
            )

    def set_direction(self, chat_id: int, direction: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, direction) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    direction=excluded.direction,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, direction),
            )

    def set_depart_stop_area(self, chat_id: int, *, sa_id: str, sa_label: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, depart_sa_id, depart_sa_label) VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    depart_sa_id=excluded.depart_sa_id,
                    depart_sa_label=excluded.depart_sa_label,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, sa_id, sa_label),
            )

    def set_arrivee_stop_area(self, chat_id: int, *, sa_id: str, sa_label: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, arrivee_sa_id, arrivee_sa_label) VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    arrivee_sa_id=excluded.arrivee_sa_id,
                    arrivee_sa_label=excluded.arrivee_sa_label,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, sa_id, sa_label),
            )

    def set_allowed_modes(self, chat_id: int, allowed_modes: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, allowed_modes) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    allowed_modes=excluded.allowed_modes,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, allowed_modes),
            )

    def set_meteo(self, chat_id: int, label: str, lat: float, lon: float) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, meteo_label, meteo_lat, meteo_lon)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    meteo_label=excluded.meteo_label,
                    meteo_lat=excluded.meteo_lat,
                    meteo_lon=excluded.meteo_lon,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, label, lat, lon),
            )

    def get_user(self, chat_id: int) -> UserPrefs | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if row is None:
            return None
        return UserPrefs(
            chat_id=int(row["chat_id"]),
            depart=row["depart"],
            direction=row["direction"],
            depart_sa_id=row["depart_sa_id"],
            depart_sa_label=row["depart_sa_label"],
            arrivee_sa_id=row["arrivee_sa_id"],
            arrivee_sa_label=row["arrivee_sa_label"],
            allowed_modes=row["allowed_modes"],
            meteo_label=row["meteo_label"],
            meteo_lat=row["meteo_lat"],
            meteo_lon=row["meteo_lon"],
            segments_json=row["segments_json"],
            notif_time=row["notif_time"],
            last_notif_sent_key=row["last_notif_sent_key"],
            recevoir_news_du_jour=_row_bool_news_du_jour(row),
            news_category=row["news_category"] if "news_category" in row.keys() else None,
            finance_selection=row["finance_selection"] if "finance_selection" in row.keys() else None,
        )

    def iter_users(self) -> Iterable[UserPrefs]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM users").fetchall()
        for row in rows:
            yield UserPrefs(
                chat_id=int(row["chat_id"]),
                depart=row["depart"],
                direction=row["direction"],
                depart_sa_id=row["depart_sa_id"],
                depart_sa_label=row["depart_sa_label"],
                arrivee_sa_id=row["arrivee_sa_id"],
                arrivee_sa_label=row["arrivee_sa_label"],
                allowed_modes=row["allowed_modes"],
                meteo_label=row["meteo_label"],
                meteo_lat=row["meteo_lat"],
                meteo_lon=row["meteo_lon"],
                segments_json=row["segments_json"],
                notif_time=row["notif_time"],
                last_notif_sent_key=row["last_notif_sent_key"],
                recevoir_news_du_jour=_row_bool_news_du_jour(row),
                news_category=row["news_category"] if "news_category" in row.keys() else None,
                finance_selection=row["finance_selection"] if "finance_selection" in row.keys() else None,
            )

    # High-level helpers for new features

    def set_segments_json(self, chat_id: int, segments_json: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, segments_json) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    segments_json=excluded.segments_json,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, segments_json),
            )

    def set_notif_time(self, chat_id: int, notif_time: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, notif_time) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    notif_time=excluded.notif_time,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, notif_time),
            )

    def set_last_notif_sent_key(self, chat_id: int, sent_key: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, last_notif_sent_key) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    last_notif_sent_key=excluded.last_notif_sent_key,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, sent_key),
            )

    def should_send_notif(self, *, user: UserPrefs, sent_key: str) -> bool:
        return user.last_notif_sent_key != sent_key

    def purge_users(self) -> None:
        """
        Deletes all user rows and clears shared caches (news, finance).
        Reserved for admin /purge_db YES.
        """
        with self._connect() as conn:
            conn.execute("DELETE FROM users")

        # Development/testing purge of news cache too.
        with self._connect() as conn:
            conn.execute("DELETE FROM news_cache")
            conn.execute("DELETE FROM finance_cache")

    def reset_user_profile(self, chat_id: int) -> None:
        """Remet à zéro toutes les préférences d'un utilisateur (ligne users conservée)."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(
                    chat_id, depart, direction, depart_sa_id, depart_sa_label,
                    arrivee_sa_id, arrivee_sa_label, allowed_modes,
                    meteo_label, meteo_lat, meteo_lon, segments_json,
                    notif_time, last_notif_sent_key, recevoir_bonne_nouvelle,
                    news_category, finance_selection
                ) VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL)
                ON CONFLICT(chat_id) DO UPDATE SET
                    depart=NULL,
                    direction=NULL,
                    depart_sa_id=NULL,
                    depart_sa_label=NULL,
                    arrivee_sa_id=NULL,
                    arrivee_sa_label=NULL,
                    allowed_modes=NULL,
                    meteo_label=NULL,
                    meteo_lat=NULL,
                    meteo_lon=NULL,
                    segments_json=NULL,
                    notif_time=NULL,
                    last_notif_sent_key=NULL,
                    recevoir_bonne_nouvelle=0,
                    news_category=NULL,
                    finance_selection=NULL,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id,),
            )

    def set_finance_selection(self, chat_id: int, finance_selection: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, finance_selection) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    finance_selection=excluded.finance_selection,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, finance_selection),
            )

    def set_recevoir_bonne_nouvelle(self, chat_id: int, enabled: bool) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users(chat_id, recevoir_bonne_nouvelle) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    recevoir_bonne_nouvelle=excluded.recevoir_bonne_nouvelle,
                    updated_at=datetime('now')
                """.strip(),
                (chat_id, 1 if enabled else 0),
            )

    def get_news_cache_ready(self, *, day: str) -> tuple[str | None, str | None] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT headline, url FROM news_cache WHERE day=? AND state='ready'",
                (day,),
            ).fetchone()
        if row is None:
            return None
        headline = row["headline"]
        url = row["url"]
        if headline is None:
            return None
        return str(headline), (str(url) if url is not None else None)

    def mark_news_pending(self, *, day: str) -> bool:
        """
        Marks the news cache as 'pending' for the given day.
        Returns True if this caller should perform the API fetch.
        """
        with self._connect() as conn:
            row = conn.execute("SELECT state, headline FROM news_cache WHERE day=?", (day,)).fetchone()
            if row is not None:
                state = str(row["state"] or "")
                headline = row["headline"]
                if state == "ready" and headline is not None:
                    return False
                if state == "pending":
                    return False
                conn.execute(
                    "UPDATE news_cache SET headline=NULL, url=NULL, fetched_at=NULL, state='pending' WHERE day=?",
                    (day,),
                )
                return True

            conn.execute(
                "INSERT INTO news_cache(day, headline, url, fetched_at, state) VALUES (?, NULL, NULL, NULL, 'pending')",
                (day,),
            )
            return True

    def set_news_cache_ready(
        self,
        *,
        day: str,
        headline: str,
        url: str | None,
    ) -> None:
        from datetime import datetime, timezone

        fetched_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO news_cache(day, headline, url, fetched_at, state)
                VALUES (?, ?, ?, ?, 'ready')
                ON CONFLICT(day) DO UPDATE SET
                    headline=excluded.headline,
                    url=excluded.url,
                    fetched_at=excluded.fetched_at,
                    state='ready'
                """.strip(),
                (day, headline, url, fetched_at),
            )

    def get_finance_cache_ready(self, *, day: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM finance_cache WHERE day=? AND state='ready' AND payload IS NOT NULL",
                (day,),
            ).fetchone()
        if row is None:
            return None
        raw = row["payload"]
        if raw is None:
            return None
        try:
            data = json.loads(str(raw))
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        return data

    def mark_finance_pending(self, *, day: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT state, payload FROM finance_cache WHERE day=?", (day,)).fetchone()
            if row is not None:
                state = str(row["state"] or "")
                payload = row["payload"]
                if state == "ready" and payload is not None:
                    return False
                if state == "pending":
                    return False
                conn.execute(
                    "UPDATE finance_cache SET payload=NULL, fetched_at=NULL, state='pending' WHERE day=?",
                    (day,),
                )
                return True

            conn.execute(
                "INSERT INTO finance_cache(day, payload, fetched_at, state) VALUES (?, NULL, NULL, 'pending')",
                (day,),
            )
            return True

    def set_finance_cache_ready(self, *, day: str, payload: dict[str, Any]) -> None:
        from datetime import datetime, timezone

        fetched_at = datetime.now(timezone.utc).isoformat()
        blob = json.dumps(payload, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO finance_cache(day, payload, fetched_at, state)
                VALUES (?, ?, ?, 'ready')
                ON CONFLICT(day) DO UPDATE SET
                    payload=excluded.payload,
                    fetched_at=excluded.fetched_at,
                    state='ready'
                """.strip(),
                (day, blob, fetched_at),
            )

