#!/bin/bash
set -e
REPO=/DATA/homelab-setup
cd "$REPO"
git pull origin main

echo "[deploy] Syncing nas-controller..."
cp nas-controller/app.py          /DATA/AppData/nas-controller/app.py
cp nas-controller/Dockerfile      /DATA/AppData/nas-controller/Dockerfile
cp nas-controller/docker-compose.yaml /DATA/AppData/nas-controller/docker-compose.yaml

echo "[deploy] Syncing beets scripts..."
for f in all-lyrics.py beet-organize.py flac-lyrics.py flac-lrc-simple.py flac-lrc-v3.py beets-cron.sh; do
  [ -f "beets/$f" ] && cp "beets/$f" "/DATA/AppData/beets/$f"
done
cp beets/config/config.yaml /DATA/AppData/beets/config/config.yaml

echo "[deploy] Restarting containers..."
docker restart nas-controller

echo "[deploy] Done."
