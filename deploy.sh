#!/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
set -e
REPO=/DATA/homelab-setup
cd "$REPO"
GIT_SSH_COMMAND="ssh -i /DATA/.ssh/github_ed25519 -o StrictHostKeyChecking=no" git pull origin main

echo "[deploy] Syncing nas-controller..."
cp nas-controller/app.py          /DATA/AppData/nas-controller/app.py
cp nas-controller/Dockerfile      /DATA/AppData/nas-controller/Dockerfile
cp nas-controller/docker-compose.yaml /DATA/AppData/nas-controller/docker-compose.yaml

echo "[deploy] Syncing beets scripts..."
for f in all-lyrics.py beet-organize.py beets-cron.sh; do
  [ -f "beets/$f" ] && cp "beets/$f" "/DATA/AppData/beets/$f"
done
cp beets/beet-organize.py /DATA/AppData/beets/config/beet-organize.py
cp beets/config/config.yaml /DATA/AppData/beets/config/config.yaml

echo "[deploy] Syncing navidrome config..."
cp navidrome/navidrome.toml /DATA/AppData/navidrome/data/navidrome.toml

echo "[deploy] Restarting containers..."
docker restart nas-controller

echo "[deploy] Done."
