#!/bin/bash
until [ -f /run/casaos/Caddyfile ]; do sleep 1; done
sleep 2

if ! grep -q "dokploy.nas" /run/casaos/Caddyfile; then
cat >> /run/casaos/Caddyfile << 'CADDYEOF'

dokploy.nas:443 {
    tls /etc/dokploy/traefik/certs/dokploy-nas-cert.pem /etc/dokploy/traefik/certs/dokploy-nas-key.pem
    reverse_proxy http://127.0.0.1:80 {
        header_up Host dokploy.nas
    }
}
CADDYEOF
fi

kill -HUP $(pgrep zimaos-gateway | head -1)
