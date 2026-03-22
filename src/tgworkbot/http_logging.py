"""Réduit la verbosité des clients HTTP (évite notamment les URLs Telegram avec token en INFO)."""

from __future__ import annotations

import logging


def quiet_http_client_loggers() -> None:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
