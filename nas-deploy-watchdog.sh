#!/bin/bash
# Poll git every 5 min; only redeploy if remote has new commits
REPO=/DATA/homelab-setup
LOCK=/tmp/nas-deploy.lock

[ -f "$LOCK" ] && exit 0
touch "$LOCK"
trap "rm -f $LOCK" EXIT

cd "$REPO"
GIT_SSH_COMMAND="ssh -i /DATA/.ssh/github_ed25519 -o StrictHostKeyChecking=no" \
  git fetch origin main --quiet 2>/dev/null || exit 0

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" != "$REMOTE" ]; then
  logger -t nas-deploy "New commit detected ($REMOTE), running deploy"
  bash /DATA/homelab-setup/deploy.sh >> /tmp/nas-deploy.log 2>&1
fi
