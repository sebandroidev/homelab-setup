"""Tiny webhook server — receives GitHub push events and runs deploy.sh.
Runs as a separate container so nas-controller restarts don't self-interrupt."""
import hashlib
import hmac
import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

SECRET = os.getenv("DEPLOY_SECRET", "").encode()
DEPLOY_SCRIPT = "/DATA/homelab-setup/deploy.sh"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[deploy-agent] {fmt % args}", flush=True)

    def do_POST(self):
        if self.path != "/webhook/deploy":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        if SECRET:
            sig = self.headers.get("X-Hub-Signature-256", "")
            expected = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(sig, expected):
                self.send_response(403)
                self.end_headers()
                self.wfile.write(b'{"error":"invalid signature"}')
                return

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"deploying"}')

        def _run():
            print("[deploy-agent] Running deploy.sh", flush=True)
            subprocess.run(["bash", DEPLOY_SCRIPT], check=False)
            print("[deploy-agent] deploy.sh finished", flush=True)

        threading.Thread(target=_run, daemon=True).start()


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8889))
    print(f"[deploy-agent] Listening on :{port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
