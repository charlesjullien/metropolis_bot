#!/usr/bin/env bash
# Teste l’accès HTTP depuis PythonAnywhere (whitelist proxy) vers l’API Wikimedia « Ce jour-là ».
# Usage :  bash scripts/pa_whitelist_historical_event_urls.sh

set -u
MAX_SEC="${MAX_SEC:-25}"

urls=(
  "https://fr.wikipedia.org/api/rest_v1/feed/onthisday/selected/03/24"
  "https://fr.wikipedia.org/api/rest_v1/feed/onthisday/events/03/24"
  "https://api.wikimedia.org/feed/v1/wikipedia/fr/onthisday/selected/03/24"
)

echo "HTTP → URL (timeout ${MAX_SEC}s)"
echo "----------------------------------------------"
for u in "${urls[@]}"; do
  code=$(curl -sS -o /dev/null -w "%{http_code}" --max-time "$MAX_SEC" -H "User-Agent: MetropolisBot/1.0" -L "$u" 2>/dev/null || true)
  [[ -z "$code" ]] && code="000"
  printf '%s → %s\n' "$code" "$u"
done
echo "----------------------------------------------"
echo "Attendu : 200 sur ces URLs (avec User-Agent descriptif en prod)."
