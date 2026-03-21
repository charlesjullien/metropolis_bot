from __future__ import annotations

import html

# Lien non officiel mais utile (données PRIM / historique) — voir https://ratpstatus.fr/
RATPSTATUS_FOOTER_PLAIN = (
    "Pour des infos très visuelles sur les perturbations, allez voir sur "
    "https://ratpstatus.fr/ dans les onglets Métros, RER ou Tram."
)

RATPSTATUS_FOOTER_HTML = (
    "Pour des infos très visuelles sur les perturbations, allez voir sur "
    '<a href="https://ratpstatus.fr/">https://ratpstatus.fr/</a> '
    "dans les onglets Métros, RER ou Tram."
)


def escape_telegram_html(text: str) -> str:
    """Évite les doubles entités (&amp; affiché tel quel) : normalise puis échappe pour ParseMode.HTML."""
    return html.escape(html.unescape(text or ""))
