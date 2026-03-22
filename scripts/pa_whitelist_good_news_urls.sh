#!/usr/bin/env bash
# Teste l’accès HTTP depuis PythonAnywhere (whitelist proxy).
# Usage :  bash scripts/pa_whitelist_good_news_urls.sh

set -u
MAX_SEC="${MAX_SEC:-25}"

urls=(
  "https://metropolis-swagger.vercel.app/getGoodNewsOfTheDay"
  "https://fr.wikipedia.org/w/api.php?action=featuredfeed&feed=featured&feedformat=rss&language=fr"
  "https://commons.wikimedia.org/w/api.php?action=featuredfeed&feed=potd&feedformat=rss&language=fr"
  "https://apod.nasa.gov/apod.rss"
  "https://www.gutenberg.org/cache/epub/feeds/today.rss"
  "https://blog.khanacademy.org/feed/"
)

echo "HTTP → URL (timeout ${MAX_SEC}s)"
echo "----------------------------------------------"
for u in "${urls[@]}"; do
  code=$(curl -sS -o /dev/null -w "%{http_code}" --max-time "$MAX_SEC" -L "$u" 2>/dev/null || true)
  [[ -z "$code" ]] && code="000"
  printf '%s → %s\n' "$code" "$u"
done
echo "----------------------------------------------"
echo "L’API Métropolis renvoie 401 sans Bearer : c’est normal. 200 = joignable."
