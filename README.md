# tgWorkBot (Telegram)

Bot Telegram qui :

- envoie une notification à **l’heure choisie** (setup ou `/heure_notif`, minutes par pas de 5), uniquement sur les **jours sélectionnés** via `/jours_notifs`, avec **la météo du jour** (pluie : plages horaires + pluviométrie) et **les perturbations transports** (zone/ligne),
- permet à chaque utilisateur de configurer :
  - `/depart <station>` : station de départ (texte libre, ex: `Bastille`)
  - `/direction <destination>` : direction (texte libre, ex: `La Défense`)
  - `/lieumeteo <ville|lat,lon>` : lieu météo (ex: `Paris` ou `48.8566,2.3522`)

## Prérequis

- Python 3.11+ recommandé
- Un token Telegram (via **@BotFather**)

## Installation

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

## Configuration

Éditez `.env` :

- `TELEGRAM_BOT_TOKEN` (**obligatoire**)
- `BOT_TIMEZONE` (défaut `Europe/Paris`) — fuseau pour comparer l’heure courante à `notif_time` en base

### Transports (Île‑de‑France / PRIM - optionnel)

Si vous voulez une vraie vérification perturbations en IDF, ajoutez :

- `IDFM_PRIM_API_KEY` : votre jeton PRIM

Le bot utilisera l’API Navitia PRIM (base `https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia`).

Sans clé, le bot répondra avec un message “provider non configuré”.

## Lancer le bot

```bash
python run.py
```

## Commandes

- `/start` : aide
- `/depart <station>`
- `/direction <destination>`
- `/lieumeteo <ville|lat,lon>`
- `/meteo` : météo du jour (manuel)
- `/perturbations` : perturbations (manuel)
- `/jours_notifs` : choisir les jours de notification (lun-dim)
- `/status` : affiche la config enregistrée

