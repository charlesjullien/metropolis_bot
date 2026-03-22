#!/usr/bin/env bash
# Teste l’accès HTTP depuis PythonAnywhere (whitelist proxy).
# Usage :  bash scripts/pa_whitelist_daily_news_urls.sh

set -u
MAX_SEC="${MAX_SEC:-25}"

urls=(
  "https://metropolis-swagger.vercel.app/getGoodNewsOfTheDay"
  "https://newsapi.org/v2/top-headlines?country=fr&pageSize=1"
)

echo "HTTP → URL (timeout ${MAX_SEC}s)"
echo "----------------------------------------------"
for u in "${urls[@]}"; do
  code=$(curl -sS -o /dev/null -w "%{http_code}" --max-time "$MAX_SEC" -L "$u" 2>/dev/null || true)
  [[ -z "$code" ]] && code="000"
  printf '%s → %s\n' "$code" "$u"
done
echo "----------------------------------------------"
echo "Métropolis : 401 sans Bearer (normal). NewsAPI : 401/426 sans apiKey (normal)."
