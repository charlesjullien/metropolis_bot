"""
WSGI pour PythonAnywhere : déclenche l'envoi des notifications via HTTP.

Onglet **Web** sur PythonAnywhere :
  1. **Source code** : dossier racine du dépôt (là où se trouvent ``run.py``, ``src/``, ``.env``).
  2. **WSGI configuration file** : ouvre le fichier généré et **remplace tout** son contenu par
     l’import ci-dessous (ou mets le chemin absolu vers ce fichier comme cible si PA le permet).

     Le modèle Django/Flask par défaut ne connaît pas ``/check_for_notifications`` : sans remplacement,
     tu obtiens un **404 HTML** (pas le JSON du bot).

     Exemple minimal (adapte le chemin ``/home/TON_USER/...``) ::

         import sys
         sys.path.insert(0, "/home/solaris777/tgworkbot/src")
         from tgworkbot.api_check_notifications import application

     Ou garde ce fichier dans le repo et pointe le WSGI vers son chemin absolu.

  3. **Reload** l’application après chaque changement.

URL à appeler (cron, navigateur) ::
  https://<ton_user>.pythonanywhere.com/check_for_notifications

Si tu vois un JSON ``{"error":"not found","path_received":...}``, c’est bien notre app : l’URL ou le
chemin ne correspond pas. Si c’est une page HTML « Not found », le WSGI n’utilise pas encore
``api_check_notifications.application``.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tgworkbot.api_check_notifications import application  # noqa: E402

__all__ = ["application"]
