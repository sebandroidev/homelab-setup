# Watcharr — self-hosted watchlist

Movie + TV watchlist with TMDB integration. Used by the nas-controller bot's
🔍 Find Infos / 📺 Watchlist feature.

## Deployment

```bash
mkdir -p /opt/apps/watcharr/data
chown 1000:1000 /opt/apps/watcharr/data

# Generate JWT secret once, save to .env on NAS (never commit)
echo "WATCHARR_JWT_SECRET=$(openssl rand -hex 32)" >> /opt/apps/watcharr/.env

cd /opt/apps/watcharr
docker compose --env-file .env up -d
```

## First-run setup

1. Open https://watchlist.bastienlab.com
2. Sign up the admin account (first user is owner)
3. In the UI: Settings → Tokens → Create token (read+write)
4. Copy the token → add to `/opt/apps/nas-controller/.env`:
   ```
   WATCHARR_URL=http://host.docker.internal:3201
   WATCHARR_TOKEN=<token>
   ```
5. (Optional) Settings → TMDB → paste your TMDB API key so Watcharr can do
   its own searches; the nas-controller bot also has its own TMDB key.

## Cloudflare tunnel

Ingress rule on tunnel `d2703da1-d43e-4137-b98b-3776748b1d2d`:
`watchlist.bastienlab.com → http://127.0.0.1:3201`
