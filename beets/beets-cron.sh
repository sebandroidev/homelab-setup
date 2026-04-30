#!/bin/bash
# Beets full library sync: import, art, embed, lyrics
set -uo pipefail
LOG=/DATA/AppData/beets/cron.log
exec >> "$LOG" 2>&1
echo "=== [$(date)] beets-cron start ==="
docker exec beets beet import /music -q
docker exec beets beet import /evymusics -q
docker exec beets beet fetchart
echo y | docker exec -i beets beet embedart
python3 /DATA/AppData/beets/all-lyrics.py
echo "=== [$(date)] beets-cron done ==="
