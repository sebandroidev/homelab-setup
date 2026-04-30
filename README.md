# homelab-setup

ZimaCube NAS configuration and custom services.

## Services

| Service | Description |
|---|---|
| `nas-controller` | Flask app — cron jobs, file watcher, Telegram bot, Spotify import, slskd downloads |
| `beets` | Music library manager — import, art, lyrics, LRC sidecars |
| `explo` | Music discovery — ListenBrainz + slskd/YouTube downloader |
| `scripts` | System scripts — Caddy/Dokploy integration |

## Auto-deploy

Push to `main` → GitHub webhook → `nas-controller` pulls repo and restarts containers.

### Webhook setup (one-time)

1. Go to GitHub repo → Settings → Webhooks → Add webhook
2. Payload URL: `https://nas.bastien-nas.duckdns.org/webhook/deploy`
3. Content type: `application/json`
4. Secret: value of `DEPLOY_SECRET` from `nas-controller/.env`
5. Events: Just the push event

## Secrets

Never commit `.env` files. Copy `.env.example` → `.env` for each service and fill in values.
