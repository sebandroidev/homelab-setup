#!/usr/bin/env python3
"""NAS Controller — cron jobs, file watcher, Spotify import, Telegram bot."""

import csv, hashlib, hmac, io, json, logging, os, re, subprocess, threading, time, urllib.parse, urllib.request, uuid, urllib.error
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, Response, request, redirect

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_IDS   = set(filter(None, os.getenv("TELEGRAM_CHAT_IDS", "").split(",")))
HISTORY_FILE  = Path("/data/history.json")
SEEN_FILE     = Path("/data/seen_files.json")
SPOTIFY_FILE  = Path("/data/spotify.json")

NAVIDROME_URL = os.getenv("NAVIDROME_URL", "http://host.docker.internal:4533")
NAV_USER      = os.getenv("NAVIDROME_USER", "sebastien")
NAV_PASS      = os.getenv("NAVIDROME_PASS", "sebastien")
DOWNLOADS_DIR = os.getenv("DOWNLOADS_DIR", "/media/sdb/Musics/Soulseek")

SLSKD_URL     = os.getenv("SLSKD_URL", "http://host.docker.internal:5030")
SLSKD_API_KEY = os.getenv("SLSKD_API_KEY", "cff949683de044ba885fa83b1d01b5d07eca5fd47f00afd4")
LASTFM_KEY    = os.getenv("LASTFM_API_KEY", "")
DISCOVER_FILE = Path("/data/discover.json")

NAS_SSH_HOST  = os.getenv("NAS_SSH_HOST", "host.docker.internal")
NAS_SSH_USER  = os.getenv("NAS_SSH_USER", "sebastien")
NAS_SSH_PASS  = os.getenv("NAS_SSH_PASS", "sebastien")

WATCH_DIRS     = ["/media/sdb/Musics", "/media/sdb/Evyy Musics"]
AUDIO_EXTS     = {".flac", ".m4a", ".mp3", ".aac", ".ogg", ".opus"}
WATCH_INTERVAL = 60
DEBOUNCE_SECS  = 90
BEETS_DIR_MAP  = {
    "/media/sdb/Musics":      "/music",
    "/media/sdb/Evyy Musics": "/evymusics",
}

JOBS = {
    "beets": {
        "name": "Beets — import + art", "icon": "🎵",
        "cmd": (
            "docker exec beets beet import /music -q ; "
            "docker exec beets beet import /evymusics -q ; "
            "docker exec beets beet fetchart ; "
            "docker exec beets beet embedart -y"
        ),
    },
    "lyrics": {
        "name": "Lyrics — .lrc sidecars", "icon": "📝",
        "cmd":  "python3 /DATA/AppData/beets/all-lyrics.py",
    },
    "lrclib": {
        "name": "lrclib-docker", "icon": "🎤",
        "cmd": (
            "docker exec lrclib-docker python3 /app/run.py --folder /app/music --file-limit 0 ; "
            "docker exec lrclib-docker python3 /app/run.py --folder /app/evymusics --file-limit 0"
        ),
    },
    "explo": {
        "name": "Explo — discovery", "icon": "🔍",
        "cmd":  'docker exec explo sh -c "apk add --upgrade yt-dlp -q && cd /opt/explo && ./explo"',
    },
    "backup": {
        "name": "NAS Backup", "icon": "💾",
        "cmd":  "/media/sdb/Backups/nas-backup.sh",
    },
}


# ── Exposed Services ─────────────────────────────────────────────────────────
EXPOSED_SERVICES = [
    {"name": "Navidrome",   "icon": "🎵", "url": "https://navidrome.bastien-nas.duckdns.org",  "check": "http://host.docker.internal:4533"},
    {"name": "Immich",      "icon": "📸", "url": "https://immich.bastien-nas.duckdns.org",     "check": "http://host.docker.internal:2283"},
    {"name": "Gitea",       "icon": "🐙", "url": "https://gitea.bastien-nas.duckdns.org",      "check": "http://host.docker.internal:8070"},
    {"name": "Slskd",       "icon": "🔍", "url": "https://slskd.bastien-nas.duckdns.org",      "check": "http://host.docker.internal:5030"},
    {"name": "Uptime Kuma", "icon": "📡", "url": "https://uptime.bastien-nas.duckdns.org",     "check": "http://host.docker.internal:8071"},
    {"name": "Grafana",     "icon": "📊", "url": "https://grafana.bastien-nas.duckdns.org",    "check": "http://host.docker.internal:3030"},
    {"name": "Orly API",    "icon": "🚀", "url": "https://api.bastien-nas.duckdns.org",        "check": "http://host.docker.internal:4000"},
    {"name": "NAS",         "icon": "🖥",  "url": "https://nas.bastien-nas.duckdns.org",        "check": "http://host.docker.internal:8888"},
    {"name": "ZimaOS",      "icon": "🏠", "url": "https://home.bastien-nas.duckdns.org",       "check": "http://host.docker.internal:8080"},
    {"name": "Dokploy",     "icon": "🐳", "url": "https://dokploy.bastien-nas.duckdns.org",    "check": "http://host.docker.internal:3000"},
    {"name": "AdGuard",     "icon": "🛡",  "url": "https://adguard.bastien-nas.duckdns.org",    "check": "http://host.docker.internal:3080"},
]


# ── Telegram keyboard ────────────────────────────────────────────────────────
TG_KEYBOARD = json.dumps({
    "keyboard": [
        [{"text": "⬇️ Download Tracks"}, {"text": "📊 NAS Stats"}],
        [{"text": "🗂 Library"}, {"text": "🔌 NAS Control"}],
        [{"text": "🌐 Services"}],
    ],
    "resize_keyboard": True,
    "persistent": True,
})

_STATS_REFRESH_KB = json.dumps({
    "inline_keyboard": [[{"text": "🔄 Refresh", "callback_data": "stats:refresh"}]]
})

# ── Shared state ──────────────────────────────────────────────────────────────
_locks:  dict[str, bool]      = {k: False for k in JOBS}
_live:   dict[str, list[str]] = {k: []    for k in JOBS}
_history: dict                = {}
_state_lock                   = threading.Lock()

_seen_files: dict = {}
_watch = {
    "enabled": True, "last_scan": None,
    "pending": {}, "recent": [],
}
_watch_lock = threading.Lock()

# Pipeline serialization: one pipeline runs at a time; last one does global tasks
_pipeline_lock    = threading.Lock()
_pipeline_waiting = 0
_pipeline_wlock   = threading.Lock()

# Batch mode: suppress per-dir TG noise when many dirs queued at once
_BATCH_THRESHOLD  = 5
_pipeline_results: list = []   # accumulates (short, count, ok, elapsed) in batch mode
_pipeline_rlock   = threading.Lock()

# Telegram notification rate-limiting + mute
_mute_until          = 0.0    # epoch time until which notifications are silenced
_last_notify_time    = 0.0
_NOTIFY_MIN_INTERVAL = 15.0   # seconds — min gap between non-forced notifications
_notify_state_lock   = threading.Lock()

# Free-search state: chat_ids waiting for a search query message
_search_pending: set = set()

# Spotify / download
_spotify: dict = {
    "tracks": [], "playlists": [], "last_import": None, "nav_loaded": None,
}
_nav_index: set = set()
_nav_isrc:  set = set()
_downloads: dict = {}
_sp_lock = threading.Lock()
_dl_lock = threading.Lock()

# Discover
_discover: dict = {"artists": [], "last_refresh": None, "refreshing": False}
_disc_lock = threading.Lock()

# Download sessions (Telegram interactive downloads)
_dl_sessions: dict = {}
_sess_lock = threading.Lock()

# Telegram result message IDs — ephemeral, not persisted to Redis
_result_msgs: dict = {}  # chat_id -> (msg_id, msg_type)
_pending_pipeline: dict = {}  # "artist - title" -> dl_id, for watcher→download linkage

# Background slskd searches: search_id -> {"done": bool, "folders": list}
_bg_searches: dict = {}
_bg_lock = threading.Lock()

# Background lyrics scan lock — prevents concurrent full-library scans
_lyrics_bg_lock = threading.Lock()

# Manual library maintenance state
_maint_running = False
_maint_state_lock = threading.Lock()

# DNS watchdog state — tracks actual Telegram HTTP reachability, not DNS resolution
_dns_fail_since: float | None = None
_last_tg_success: float = time.time()  # updated on each successful getUpdates
_DNS_RESTART_AFTER = 600  # restart only if HTTP unreachable AND no poll success for 10 min

# ── NAS Stats ─────────────────────────────────────────────────────────────────
def _collect_nas_stats() -> dict:
    import glob as _glob
    stats: dict = {}

    try:
        uptime_secs = float(open("/proc/uptime").read().split()[0])
        h, rem = divmod(int(uptime_secs), 3600)
        m = rem // 60
        d, h = divmod(h, 24)
        stats["uptime"] = (f"{d}d {h}h {m}m" if d else f"{h}h {m}m")
    except Exception:
        stats["uptime"] = "?"

    try:
        p = open("/proc/loadavg").read().split()
        stats["load"] = f"{p[0]} · {p[1]} · {p[2]}"
    except Exception:
        stats["load"] = "?"

    try:
        mem: dict = {}
        for line in open("/proc/meminfo"):
            k, _, v = line.partition(":")
            mem[k.strip()] = int(v.strip().split()[0])
        total      = mem.get("MemTotal", 0)
        avail      = mem.get("MemAvailable", 0)
        used       = total - avail
        swap_total = mem.get("SwapTotal", 0)
        swap_used  = swap_total - mem.get("SwapFree", 0)
        def _gib(kb): return f"{kb/1024/1024:.1f} GiB"
        def _pct(u, t): return f"{u*100//t}%" if t else "?"
        stats.update(mem_used=_gib(used), mem_total=_gib(total), mem_pct=_pct(used, total),
                     swap_used=_gib(swap_used), swap_total=_gib(swap_total), swap_pct=_pct(swap_used, swap_total))
    except Exception:
        for k in ("mem_used", "mem_total", "mem_pct", "swap_used", "swap_total", "swap_pct"):
            stats[k] = "?"

    try:
        r = subprocess.run(["df", "--output=used,avail,pcent", "/media/sdb"],
                           capture_output=True, text=True, timeout=5)
        _, data_line = r.stdout.strip().splitlines()
        used_kb, avail_kb, pct = data_line.split()
        def _gb(kb): return f"{int(kb)/1024/1024:.0f} GB"
        stats.update(disk_used=_gb(used_kb), disk_total=_gb(int(used_kb)+int(avail_kb)), disk_pct=pct.strip())
    except Exception:
        stats.update(disk_used="?", disk_total="?", disk_pct="?")

    try:
        temps = []
        for tf in sorted(_glob.glob("/sys/class/thermal/thermal_zone*/temp")):
            t = int(open(tf).read().strip()) // 1000
            if t > 0:
                temps.append(t)
        if temps:
            stats["temps"] = "  ".join(f"{t}°C" for t in temps)
    except Exception:
        pass

    try:
        r = subprocess.run(
            ["/usr/bin/docker", "stats", "--no-stream",
             "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
            capture_output=True, text=True, timeout=12)
        rows = []
        for line in r.stdout.strip().splitlines():
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            name, cpu_str, mem_str = parts[0], parts[1], parts[2]
            try:
                cpu_val = float(cpu_str.rstrip("%")) if cpu_str not in ("--", "") else 0.0
            except ValueError:
                cpu_val = 0.0
            mem_used = mem_str.split("/")[0].strip() if "/" in mem_str else mem_str.strip()
            def _to_mib(s):
                s = s.strip()
                try:
                    if s.endswith("GiB"): return float(s[:-3]) * 1024
                    if s.endswith("MiB"): return float(s[:-3])
                    if s.endswith("KiB"): return float(s[:-3]) / 1024
                except ValueError:
                    pass
                return 0.0
            if cpu_val >= 0.1 or _to_mib(mem_used) >= 10:
                rows.append((cpu_val, name, cpu_str, mem_used))
        rows.sort(reverse=True)
        stats["containers"] = [(n, c, m) for _, n, c, m in rows[:7]]
    except Exception as e:
        stats["containers_err"] = str(e)

    return stats

def _nas_stats_text(stats: dict) -> str:
    import datetime as _dt
    now = _dt.datetime.now().strftime("%H:%M")
    lines = [f"📊 *NAS Status* — {now} · up {stats.get('uptime', '?')}", ""]
    lines += [
        "💻 *System*",
        f"  Load:   {stats.get('load', '?')}  _(1 / 5 / 15 min)_",
        f"  Mem:    {stats.get('mem_used', '?')} / {stats.get('mem_total', '?')}  ({stats.get('mem_pct', '?')})",
        f"  Swap:   {stats.get('swap_used', '?')} / {stats.get('swap_total', '?')}  ({stats.get('swap_pct', '?')})",
    ]
    if stats.get("temps"):
        lines.append(f"  Temp:   {stats['temps']}")
    lines += [
        "",
        "💾 *Storage*",
        f"  Music:  {stats.get('disk_used', '?')} / {stats.get('disk_total', '?')}  ({stats.get('disk_pct', '?')})",
        "",
        "🐳 *Containers* _(top by CPU)_",
    ]
    if stats.get("containers"):
        for name, cpu, mem in stats["containers"]:
            lines.append(f"  `{name[:20]:<20}` {cpu:>7}  {mem}")
    else:
        lines.append("  ⚠️ stats unavailable" + (f": {stats['containers_err']}" if stats.get("containers_err") else ""))
    return "\n".join(lines)

def _send_nas_stats(chat_id: int, msg_id: int | None = None):
    stats = _collect_nas_stats()
    text  = _nas_stats_text(stats)
    if msg_id:
        _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                 text=text, parse_mode="Markdown", reply_markup=_STATS_REFRESH_KB)
    else:
        tg_send(chat_id, text, reply_markup=_STATS_REFRESH_KB)

# ── Persistence ───────────────────────────────────────────────────────────────
def _load_history():
    global _history
    try: _history = json.loads(HISTORY_FILE.read_text())
    except: _history = {}

def _save_history():
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(json.dumps(_history, indent=2))

def _load_seen():
    global _seen_files
    try: _seen_files = json.loads(SEEN_FILE.read_text())
    except: _seen_files = {}

def _save_seen():
    SEEN_FILE.write_text(json.dumps(_seen_files))

def _load_spotify():
    global _spotify
    try: _spotify.update(json.loads(SPOTIFY_FILE.read_text()))
    except: pass

def _save_spotify():
    SPOTIFY_FILE.write_text(json.dumps({k: v for k, v in _spotify.items()
                                        if k != "tracks"}, indent=2))  # tracks saved separately

# ── Discover helpers ──────────────────────────────────────────────────────────
def _load_discover():
    global _discover
    try:
        d = json.loads(DISCOVER_FILE.read_text())
        _discover["artists"]      = d.get("artists", [])
        _discover["last_refresh"] = d.get("last_refresh")
    except: pass

def _get_nav_artists() -> set:
    artists = set()
    try:
        data = _nav_request("getArtists")
        for idx in data.get("artists", {}).get("index", []):
            for a in idx.get("artist", []):
                artists.add(_norm(a["name"]))
    except Exception as e:
        log.error("getArtists: %s", e)
    return artists

def _lastfm(method: str, **params) -> dict:
    if not LASTFM_KEY:
        return {}
    base = {"method": method, "api_key": LASTFM_KEY, "format": "json"}
    url  = "https://ws.audioscrobbler.com/2.0/?" + urllib.parse.urlencode({**base, **params})
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning("last.fm %s: %s", method, e)
        return {}

def _refresh_discover():
    with _disc_lock:
        if _discover["refreshing"]:
            return
        _discover["refreshing"] = True
    log.info("discover: starting refresh")
    try:
        nav_artists = _get_nav_artists()
        if not nav_artists:
            log.warning("discover: no artists from Navidrome")
            return

        found = {}  # norm_name -> {artist, top_album, similar_to, score}
        for nav_artist in sorted(nav_artists)[:40]:
            res = _lastfm("artist.getSimilar", artist=nav_artist, limit=20)
            for sim in res.get("similarartists", {}).get("artist", []):
                name  = sim.get("name", "").strip()
                norm  = _norm(name)
                if not name or norm in nav_artists:
                    continue
                score = float(sim.get("match", 0))
                if norm not in found or score > found[norm]["score"]:
                    found[norm] = {"artist": name, "top_album": "",
                                   "similar_to": [], "score": round(score, 3)}
                if nav_artist not in found[norm]["similar_to"]:
                    found[norm]["similar_to"].append(nav_artist)
            time.sleep(0.15)

        # Sort by score, take top 120, fetch top album for each
        ranked = sorted(found.values(), key=lambda x: -x["score"])[:120]
        results = []
        for info in ranked:
            res2 = _lastfm("artist.getTopAlbums", artist=info["artist"], limit=1)
            albums = res2.get("topalbums", {}).get("album", [])
            info["top_album"] = (albums[0].get("name", "") if albums else "")
            info["similar_to"] = info["similar_to"][:3]
            results.append(info)
            time.sleep(0.12)

        with _disc_lock:
            _discover["artists"]      = results
            _discover["last_refresh"] = datetime.now().isoformat(timespec="seconds")
            _discover["refreshing"]   = False
        DISCOVER_FILE.write_text(json.dumps(
            {"artists": results, "last_refresh": _discover["last_refresh"]}, indent=2))
        log.info("discover: found %d new artists", len(results))
    except Exception as e:
        log.error("discover refresh: %s", e)
        with _disc_lock:
            _discover["refreshing"] = False

# ── Job runner ────────────────────────────────────────────────────────────────
def run_job(job_id: str, on_start=None, on_done=None):
    with _state_lock:
        if _locks[job_id]:
            if on_done: on_done(f"⚠️ {JOBS[job_id]['name']} is already running")
            return
        _locks[job_id] = True
        _live[job_id]  = []

    job = JOBS[job_id]
    t0  = time.time()
    log.info("Job start: %s", job_id)
    if on_start: on_start(f"▶️ Starting *{job['name']}*…")

    try:
        proc = subprocess.Popen(job["cmd"], shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            with _state_lock:
                _live[job_id].append(line)
                if len(_live[job_id]) > 2000: _live[job_id] = _live[job_id][-2000:]
        proc.wait(timeout=7200)
        elapsed = int(time.time() - t0)
        ok      = proc.returncode == 0
        output  = "".join(_live[job_id])
        with _state_lock:
            _history[job_id] = {
                "last_run": datetime.now().isoformat(timespec="seconds"),
                "success": ok, "elapsed": elapsed, "output": output[-4000:],
            }
            _save_history()
        icon = "✅" if ok else "❌"
        msg  = f"{icon} *{job['name']}* done in {elapsed}s\n```\n{output[-800:]}\n```"
    except subprocess.TimeoutExpired:
        proc.kill(); msg = f"⏱️ *{job['name']}* timed out"
    except Exception as e:
        msg = f"❌ *{job['name']}* error: {e}"
    finally:
        with _state_lock: _locks[job_id] = False

    # Beets moves files (beet-organize) so re-index all watch dirs to prevent
    # the watcher from re-detecting the reorganised files as new music.
    if job_id == "beets":
        log.info("Post-beets: refreshing watch-dir index…")
        _refresh_all_watch_dirs()

    log.info("Job done: %s", job_id)
    if on_done: on_done(msg)

# ── File watcher ──────────────────────────────────────────────────────────────
def _beets_path(host_dir):
    for h, b in BEETS_DIR_MAP.items():
        if host_dir.startswith(h): return b + host_dir[len(h):]
    return host_dir

def _short_dir(host_dir):
    for root in WATCH_DIRS:
        if host_dir.startswith(root): return host_dir[len(root):].lstrip("/") or root
    return host_dir

def _scan_new_files():
    new_by_dir = {}
    for watch_dir in WATCH_DIRS:
        if not os.path.isdir(watch_dir): continue
        for root, dirs, files in os.walk(watch_dir):
            dirs[:] = [d for d in dirs if not d.startswith('.')]  # skip .incomplete and hidden dirs
            dirs.sort()
            for fname in sorted(files):
                if os.path.splitext(fname)[1].lower() not in AUDIO_EXTS: continue
                fpath = os.path.join(root, fname)
                try: mtime = os.path.getmtime(fpath)
                except OSError: continue
                if fpath not in _seen_files:
                    # Truly new file — record and trigger
                    _seen_files[fpath] = mtime
                    new_by_dir.setdefault(root, []).append(fpath)
                elif _seen_files[fpath] != mtime:
                    # File was modified (e.g. embedart changed mtime) — just update, don't re-trigger
                    _seen_files[fpath] = mtime
    if new_by_dir: _save_seen()
    return new_by_dir

def _refresh_all_watch_dirs():
    """Re-record mtimes for every audio file across all watch dirs."""
    for watch_dir in WATCH_DIRS:
        if not os.path.isdir(watch_dir):
            continue
        for root, _, files in os.walk(watch_dir):
            for fname in files:
                if os.path.splitext(fname)[1].lower() not in AUDIO_EXTS:
                    continue
                fpath = os.path.join(root, fname)
                try:
                    _seen_files[fpath] = os.path.getmtime(fpath)
                except OSError:
                    pass
    _save_seen()

def _run_lyrics_background():
    """Full-library lyrics scan running as a background daemon after the priority phase."""
    if not _lyrics_bg_lock.acquire(blocking=False):
        log.info("Background lyrics scan already running — skipping")
        return
    try:
        log.info("Background lyrics scan starting…")
        r = subprocess.run(
            "python3 /DATA/AppData/beets/all-lyrics.py",
            shell=True, capture_output=True, text=True, timeout=7200)
        summary = next(
            (l.strip() for l in (r.stdout + r.stderr).splitlines() if "Done:" in l), "")
        log.info("Background lyrics scan done: %s", summary)
    except Exception as e:
        log.warning("Background lyrics scan error: %s", e)
    finally:
        _lyrics_bg_lock.release()

def _run_manual_maintenance(chat_id: int, msg_id: int):
    """Manual library sync: art → organize → lyrics (streamed) → navidrome rescan."""
    global _maint_running
    t0 = time.time()

    def _elapsed():
        s = int(time.time() - t0)
        return f"{s // 60}m {s % 60:02d}s"

    def _edit(text):
        _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                 text=text, parse_mode="Markdown",
                 reply_markup=json.dumps({"inline_keyboard": []}))

    # --- Stage 1+2: Art + Organize (serialized under pipeline lock) ---
    if not _pipeline_lock.acquire(blocking=False):
        _edit("⚠️ A download pipeline is currently running. Try again when it finishes.")
        with _maint_state_lock:
            _maint_running = False
        return

    art_ok = org_ok = False
    art_org_error = None
    try:
        _edit("🗂 *Library Sync*\n🎨 Fetching art & covers…")
        r_art = subprocess.run(
            'docker exec beets beet fetchart ; docker exec beets beet embedart -y',
            shell=True, capture_output=True, text=True, timeout=1800)
        for line in (r_art.stdout + r_art.stderr).splitlines():
            if line.strip():
                log.info("[maint:art] %s", line.strip())
        _refresh_all_watch_dirs()
        art_ok = r_art.returncode == 0

        _edit(f"🗂 *Library Sync*\n🎨 Art {'✓' if art_ok else '⚠️'}\n📦 Organizing…\n⏱ {_elapsed()}")
        r_org = subprocess.run(
            'docker exec beets python3 /config/beet-organize.py',
            shell=True, capture_output=True, text=True, timeout=1800)
        for line in (r_org.stdout + r_org.stderr).splitlines():
            if line.strip():
                log.info("[maint:org] %s", line.strip())
        _refresh_all_watch_dirs()
        org_ok = r_org.returncode == 0
    except Exception as e:
        log.warning("Manual maintenance art/org error: %s", e)
        art_org_error = e
    finally:
        _pipeline_lock.release()

    if art_org_error:
        _edit(f"⚠️ Art/organize error: {art_org_error}")
        with _maint_state_lock:
            _maint_running = False
        return

    art_line = f"🎨 Art {'✓' if art_ok else '⚠️'}  📦 Organized {'✓' if org_ok else '⚠️'}"

    # --- Stage 3: Lyrics (streaming) ---
    if not _lyrics_bg_lock.acquire(blocking=False):
        _edit(f"🗂 *Library Sync*\n{art_line}\n⚠️ Lyrics scan already running — skipped\n⏱ {_elapsed()}")
        with _maint_state_lock:
            _maint_running = False
        return

    try:
        _edit(f"🗂 *Library Sync*\n{art_line}\n🎵 Starting lyrics fetch…\n⏱ {_elapsed()}")
        n_done = 0
        current_track = ""
        lyrics_summary = ""
        last_edit = 0.0

        proc = subprocess.Popen(
            "python3 /DATA/AppData/beets/all-lyrics.py",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1)

        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            log.info("[maint:lyrics] %s", line)
            if "=== Done:" in line:
                lyrics_summary = line.strip()
            elif " -> synced OK" in line:
                current_track = line.split(" -> ")[0].strip()
                n_done += 1
            elif " -> " in line:
                current_track = line.split(" -> ")[0].strip()

            if time.time() - last_edit >= 5:
                track_line = f"\n▶ `{_esc(current_track)}`" if current_track else ""
                _edit(
                    f"🗂 *Library Sync*\n{art_line}\n"
                    f"🎵 Fetching lyrics…{track_line}\n"
                    f"📊 {n_done} synced · ⏱ {_elapsed()}"
                )
                last_edit = time.time()

        proc.wait()

        # Parse summary into clean line
        m = re.search(r"written=(\d+).*?skipped=(\d+).*?missing=(\d+).*?errors=(\d+)", lyrics_summary)
        if m:
            w, sk, ms, er = m.groups()
            summary_line = f"written={w} · skipped={sk} · missing={ms} · errors={er}"
        else:
            summary_line = lyrics_summary or f"{n_done} tracks processed"

        # Navidrome rescan
        try:
            _nav_request("startScan")
            log.info("[maint] Navidrome rescan triggered")
        except Exception as _e:
            log.warning("[maint] Navidrome rescan failed: %s", _e)

        _edit(
            f"🗂 *Library Sync* ✅\n{art_line}\n"
            f"🎵 {summary_line}\n"
            f"⏱ Total: {_elapsed()}"
        )
        log.info("[maint] Done in %s", _elapsed())

    except Exception as e:
        log.warning("Manual maintenance lyrics error: %s", e)
        try:
            _edit(f"⚠️ Lyrics error: {e}\n⏱ {_elapsed()}")
        except Exception:
            pass
    finally:
        _lyrics_bg_lock.release()
        with _maint_state_lock:
            _maint_running = False


def _ssh_host(cmd: str, timeout: int = 20):
    """Run a command on the NAS host via SSH."""
    return subprocess.run(
        f"sshpass -p '{NAS_SSH_PASS}' ssh -o StrictHostKeyChecking=no "
        f"-o ConnectTimeout=10 {NAS_SSH_USER}@{NAS_SSH_HOST} "
        f"\"echo '{NAS_SSH_PASS}' | sudo -S {cmd}\"",
        shell=True, capture_output=True, text=True, timeout=timeout)

# ── NAS power action definitions ──────────────────────────────────────────────
_NAS_ACTIONS = {
    "reboot": {
        "label": "🔄 Restart NAS",
        "confirm_text": "⚠️ *Restart NAS?*\nAll services will be unavailable for ~1–2 min.",
        "cmd": "reboot",
        "ack": "🔄 Restarting NAS… back in ~2 min.",
    },
    "restart_bot": {
        "label": "🤖 Restart Bot",
        "confirm_text": "⚠️ *Restart the bot container?*\nBot will be offline for ~10 sec.",
        "cmd": "docker restart nas-controller",
        "ack": "🤖 Bot restarting… tap any button in ~15 sec.",
    },
    "restart_docker": {
        "label": "🐳 Restart Docker",
        "confirm_text": "⚠️ *Restart Docker daemon?*\nAll containers will stop and restart (~1 min).",
        "cmd": "systemctl restart docker",
        "ack": "🐳 Docker restarting… all containers back in ~1 min.",
    },
}

def _nas_control_menu_kb():
    rows = [[{"text": a["label"], "callback_data": f"nas:{key}"}]
            for key, a in _NAS_ACTIONS.items()]
    rows.append([{"text": "❌ Close", "callback_data": "nas:cancel"}])
    return _inline(rows)

def _handle_nas_action(chat_id: int, msg_id: int, action_key: str):
    action = _NAS_ACTIONS[action_key]
    log.info("[nas] Executing: %s", action_key)
    _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
             text=action["ack"], parse_mode="Markdown",
             reply_markup=json.dumps({"inline_keyboard": []}))
    try:
        r = _ssh_host(action["cmd"])
        if r.returncode != 0 and r.stderr.strip():
            log.warning("[nas] %s stderr: %s", action_key, r.stderr.strip())
    except Exception as e:
        log.warning("[nas] %s failed: %s", action_key, e)
        try:
            _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                     text=f"⚠️ Command failed: {e}", parse_mode="Markdown",
                     reply_markup=json.dumps({"inline_keyboard": []}))
        except Exception:
            pass


def _run_watch_pipeline(dir_path, files):
    global _pipeline_waiting

    with _pipeline_lock:  # serialize: only one pipeline runs at a time
        with _pipeline_wlock:
            original_waiting = _pipeline_waiting
            _pipeline_waiting -= 1
            is_last = (_pipeline_waiting == 0)

        is_batch = (original_waiting >= _BATCH_THRESHOLD)

        short     = _short_dir(dir_path)
        count     = len(files)
        beets_dir = _beets_path(dir_path)
        t0        = time.time()
        log.info("Watcher pipeline: %s (%d files), last=%s, batch=%s", short, count, is_last, is_batch)

        # Link to active download if artist/title appears in the detected dir path
        watcher_dl_id = None
        with _dl_lock:
            short_lower = short.lower()
            for _key, _did in list(_pending_pipeline.items()):
                if not _key:
                    continue
                # Match full key or individual artist/title tokens (album folder may not contain track title)
                _tokens = [_key] + [t.strip() for t in _key.split(" - ") if len(t.strip()) > 2]
                if any(tok in short_lower for tok in _tokens):
                    watcher_dl_id = _did
                    del _pending_pipeline[_key]
                    break

        def _wpipe_log(msg):
            if watcher_dl_id:
                _dl_log(watcher_dl_id, msg)
            log.info("[pipeline] %s", msg)

        def _wpipe_notify(msg, force=False):
            if not watcher_dl_id:
                _notify_tg(msg, force=force)

        def _wpipe_stage(stage):
            if watcher_dl_id:
                with _dl_lock:
                    _downloads[watcher_dl_id]["progress"] = {"pct": 100, "stage": stage}

        # In batch mode, suppress noisy per-dir start messages
        if not is_batch:
            if watcher_dl_id:
                _wpipe_log(f"Beets import starting: {short}")
            else:
                _notify_tg(f"🔔 *New music detected!*\n📁 `{short}` ({count} track{'s' if count!=1 else ''})\n▶️ Running beets import…")
        _wpipe_stage("importing")

        def _log_proc(label: str, r):
            for line in (r.stdout + r.stderr).splitlines():
                if line.strip():
                    log.info("[beets:%s] %s", label, line.strip())
            if r.returncode != 0:
                log.warning("[beets:%s] exited %d", label, r.returncode)

        r1 = subprocess.run(
            f'docker exec beets beet import "{beets_dir}" -q',
            shell=True, capture_output=True, text=True, timeout=1800)
        _log_proc("import", r1)

        lyrics_summary = ""
        if is_last:
            # Snapshot existing paths before global ops — used to detect organize destinations
            seen_before_global_ops = set(_seen_files.keys())
            # fetchart + embedart run on whole library — do once after all imports
            _wpipe_stage("art")
            r_art = subprocess.run(
                'docker exec beets beet fetchart ; docker exec beets beet embedart -y',
                shell=True, capture_output=True, text=True, timeout=1800)
            _log_proc("art", r_art)
            # Pre-register embedart-modified files so the watcher doesn't re-detect them
            _refresh_all_watch_dirs()
            # Organize: move Soulseek downloads into library structure
            r_org = subprocess.run(
                'docker exec beets python3 /config/beet-organize.py',
                shell=True, capture_output=True, text=True, timeout=1800)
            _log_proc("organize", r_org)
            # Pre-register organized files (new paths) before next watcher scan
            _refresh_all_watch_dirs()
            # Fresh dirs = directories of paths that didn't exist before global ops.
            # embedart only modifies existing files; organize MOVES files to new paths.
            # So new paths in _seen_files are exactly the organize destinations.
            fresh_dirs = set()
            for _fp in _seen_files:
                if _fp not in seen_before_global_ops:
                    fresh_dirs.add(os.path.dirname(_fp))
            if not fresh_dirs:
                fresh_dirs.add(dir_path)  # fallback: original detected dir
            # Lyrics also run once at the end, scoped to fresh dirs only
            _wpipe_stage("lyrics")
            if not is_batch:
                _wpipe_log("Lyrics fetch starting…")
                _wpipe_notify("▶️ Running lyrics fetch…")
            # Phase 1 — priority: fetch lyrics only for newly imported/organized tracks
            if fresh_dirs:
                dirs_arg = " ".join(f'"{d}"' for d in sorted(fresh_dirs))
                lyrics_cmd = f"python3 /DATA/AppData/beets/all-lyrics.py --dirs {dirs_arg}"
            else:
                lyrics_cmd = "python3 /DATA/AppData/beets/all-lyrics.py"
            r2 = subprocess.run(lyrics_cmd,
                shell=True, capture_output=True, text=True, timeout=1800)
            _log_proc("lyrics", r2)
            # Fallback: if script logged to file instead of stdout, read the summary line
            lyrics_summary = next(
                (l.strip() for l in (r2.stdout + r2.stderr).splitlines() if "Done:" in l), "")
            if not lyrics_summary:
                try:
                    with open("/data/logs/flac-lyrics.log") as _lf:
                        for _line in _lf:
                            if "Done:" in _line:
                                lyrics_summary = _line.strip()
                except Exception:
                    pass
            # Navidrome rescan so new tracks appear immediately
            try:
                _nav_request("startScan")
                log.info("Navidrome rescan triggered")
            except Exception as _e:
                log.warning("Navidrome rescan failed: %s", _e)
            # Phase 2 — background: scan full library for any other missing lyrics (non-blocking)
            threading.Thread(target=_run_lyrics_background, daemon=True).start()
        else:
            # More pipelines queued — skip global tasks, just note it
            lyrics_summary = "art+lyrics after remaining imports"

        elapsed = int(time.time() - t0)
        ok   = r1.returncode == 0
        icon = "✅" if ok else "⚠️"

        if is_batch:
            with _pipeline_rlock:
                _pipeline_results.append((short, count, ok, elapsed))
            if is_last:
                # Send one consolidated summary for the whole batch
                with _pipeline_rlock:
                    results = list(_pipeline_results)
                    _pipeline_results.clear()
                total_tracks = sum(c for _, c, _, _ in results)
                ok_dirs  = sum(1 for _, _, o, _ in results if o)
                fail_dirs = len(results) - ok_dirs
                status_icon = "✅" if fail_dirs == 0 else "⚠️"
                lines = [
                    f"{status_icon} *Batch import done* — {len(results)} dirs, {total_tracks} tracks",
                    f"• {ok_dirs} succeeded{f', {fail_dirs} failed' if fail_dirs else ''}",
                ]
                if lyrics_summary:
                    lines.append(f"• {lyrics_summary}")
                _notify_tg("\n".join(lines), force=True)
        else:
            if watcher_dl_id:
                summary = f"✅ {count} track{'s' if count!=1 else ''} in {elapsed}s"
                if lyrics_summary:
                    summary += f" • {lyrics_summary}"
                with _dl_lock:
                    _downloads[watcher_dl_id].update({
                        "status": "done" if ok else "error",
                        "finished": datetime.now().isoformat(timespec="seconds"),
                        "progress": {"pct": 100, "stage": "done", "summary": summary},
                    })
                _dl_log(watcher_dl_id, summary)
            else:
                _notify_tg(f"{icon} *{short}* done in {elapsed}s\n• {count} track{'s' if count!=1 else ''}\n• {lyrics_summary}", force=True)

        with _watch_lock:
            _watch["recent"].insert(0, {"dir": short, "count": count, "elapsed": elapsed,
                                        "success": ok, "ts": datetime.now().isoformat(timespec="seconds")})
            _watch["recent"] = _watch["recent"][:10]

def watcher_loop():
    global _pipeline_waiting
    log.info("Watcher: initial scan…")
    _scan_new_files()
    log.info("Watcher: ready (polling every %ds, debounce %ds)", WATCH_INTERVAL, DEBOUNCE_SECS)
    while True:
        time.sleep(WATCH_INTERVAL)
        with _watch_lock:
            if not _watch["enabled"]: continue
        now = time.time()
        new_by_dir = _scan_new_files()
        with _watch_lock:
            _watch["last_scan"] = datetime.now().isoformat(timespec="seconds")
            for d, files in new_by_dir.items():
                e = _watch["pending"].setdefault(d, {"files": [], "last_seen": 0})
                e["files"].extend(files); e["last_seen"] = now
            to_trigger = [(d, e["files"]) for d, e in list(_watch["pending"].items())
                          if now - e["last_seen"] >= DEBOUNCE_SECS]
            for d, _ in to_trigger: del _watch["pending"][d]
        for d, files in to_trigger:
            with _pipeline_wlock:
                _pipeline_waiting += 1
            threading.Thread(target=_run_watch_pipeline, args=(d, files), daemon=True).start()

# ── Spotify helpers ───────────────────────────────────────────────────────────
def _norm(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r'\s*[\(\[](feat|ft|featuring)[.\s][^\)\]]*[\)\]]', '', s, flags=re.IGNORECASE)
    s = re.split(r'\s+(feat\.?|ft\.?|featuring)\s+', s, flags=re.IGNORECASE)[0]
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _nav_request(endpoint, **params):
    base = {"u": NAV_USER, "p": NAV_PASS, "v": "1.16.1", "c": "nas-controller", "f": "json"}
    url = f"{NAVIDROME_URL}/rest/{endpoint}?" + urllib.parse.urlencode({**base, **params})
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())["subsonic-response"]

def load_nav_index() -> int:
    global _nav_index, _nav_isrc
    index, isrcs = set(), set()
    offset, batch = 0, 500
    while True:
        try:
            data  = _nav_request("search3", query="", songCount=batch, songOffset=offset,
                                  artistCount=0, albumCount=0)
            songs = data.get("searchResult3", {}).get("song", [])
            for s in songs:
                artist = _norm(s.get("artist", ""))
                album_artist = _norm(s.get("albumArtist", "") or s.get("displayAlbumArtist", ""))
                title  = _norm(s.get("title", ""))
                index.add((artist, title))
                if album_artist: index.add((album_artist, title))
                for isrc in (s.get("isrc") or []):
                    if isrc: isrcs.add(isrc.upper().strip())
            if len(songs) < batch: break
            offset += batch
        except Exception as e:
            log.error("Navidrome load: %s", e); break
    _nav_index = index
    _nav_isrc  = isrcs
    count = len(index) // 2  # approx unique songs
    with _sp_lock: _spotify["nav_loaded"] = datetime.now().isoformat(timespec="seconds")
    log.info("Navidrome index: ~%d songs, %d ISRCs", count, len(isrcs))
    return count

def _in_library(track: dict) -> bool:
    isrc = (track.get("isrc") or "").upper().strip()
    if isrc and isrc in _nav_isrc: return True
    return (_norm(track["artist"]), _norm(track["name"])) in _nav_index

def parse_spotify_csv(content: str) -> list:
    rows = []
    reader = csv.DictReader(io.StringIO(content))
    for i, row in enumerate(reader):
        rows.append({
            "idx":      i,
            "name":     row.get("Track name", "").strip(),
            "artist":   row.get("Artist name", "").strip(),
            "album":    row.get("Album", "").strip(),
            "playlist": row.get("Playlist name", "").strip(),
            "isrc":     row.get("ISRC", "").strip(),
            "spotify_id": row.get("Spotify - id", "").strip(),
        })
    return [r for r in rows if r["name"] and r["artist"]]

def find_missing(playlist: str = "") -> list:
    with _sp_lock:
        tracks = _spotify["tracks"]
    if playlist:
        tracks = [t for t in tracks if t["playlist"] == playlist]
    return [t for t in tracks if not _in_library(t)]

def _slskd(method: str, path: str, body=None, timeout: int = 15):
    url  = f"{SLSKD_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req  = urllib.request.Request(url, data=data, method=method,
               headers={"X-API-Key": SLSKD_API_KEY, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read()) if r.length != 0 else {}
    except urllib.error.HTTPError as e:
        return {"_error": e.code, "_body": e.read().decode()}
    except Exception as e:
        return {"_error": str(e)}


_YTDL_PROGRESS_RE = re.compile(
    r'\[download\]\s+([\d.]+)%\s+of\s+~?([\d.]+)(\S+)\s+at\s+([\d.]+)(\S+)(?:\s+ETA\s+(\d+:\d+))?'
)

def _bar(pct: float, width: int = 18) -> str:
    filled = round(pct / 100 * width)
    return "▓" * filled + "░" * (width - filled)

def _fmt_size(b) -> str:
    if b is None: return "?"
    b = int(b)
    if b >= 1_048_576: return f"{b/1_048_576:.1f}MiB"
    if b >= 1024:      return f"{b/1024:.0f}KiB"
    return f"{b}B"

def _fmt_speed(bps) -> str:
    if not bps: return ""
    bps = float(bps)
    if bps >= 1_048_576: return f"{bps/1_048_576:.1f}MiB/s"
    if bps >= 1024:      return f"{bps/1024:.0f}KiB/s"
    return f"{bps:.0f}B/s"

def _fmt_eta(secs) -> str:
    if secs is None or secs < 0: return ""
    secs = int(secs)
    if secs >= 3600: return f"{secs//3600}h{(secs%3600)//60:02d}m"
    if secs >= 60:   return f"{secs//60}m{secs%60:02d}s"
    return f"{secs}s"

def _parse_ytdl_size(val, unit) -> int:
    val = float(val)
    unit = unit.strip().upper()
    if unit == "GIB": return int(val * 1_073_741_824)
    if unit == "MIB": return int(val * 1_048_576)
    if unit == "KIB": return int(val * 1024)
    return int(val)

def _parse_ytdl_speed(val, unit) -> float:
    return _parse_ytdl_size(val, unit.replace("/s", "").replace("/S", "")) / 1.0

def _parse_ytdl_eta(s) -> int:
    if not s: return 0
    parts = s.split(":")
    if len(parts) == 2: return int(parts[0])*60 + int(parts[1])
    if len(parts) == 3: return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
    return 0

def _dl_log(dl_id: str, msg: str):
    with _dl_lock:
        _downloads[dl_id]["output"].append(msg)
        if len(_downloads[dl_id]["output"]) > 500:
            _downloads[dl_id]["output"] = _downloads[dl_id]["output"][-500:]

AUDIO_DL_EXTS = {".flac", ".mp3", ".m4a", ".ogg", ".opus"}

def _active_peers_only(peers: list) -> list:
    """Keep only peers that appear online with upload capacity."""
    return [
        p for p in peers
        if p.get("hasFreeUploadSlot")
        or p.get("uploadSlots", 0) > 0
        or p.get("freeUploadSlots", 0) > 0
    ]

def _peer_folders(results: list, prefer_flac: bool = True, active_only: bool = False) -> list:
    """
    Group every peer's files by their parent folder.
    Returns list of dicts sorted by (flac_count desc, total_size desc).
    """
    if active_only:
        results = _active_peers_only(results)
    folders = {}  # (username, folder_path) -> {files, flac_count, total_size, slots}
    for peer in results:
        username = peer.get("username", "")
        slots    = peer.get("uploadSlots", peer.get("freeUploadSlots", 0))
        for f in peer.get("files", []):
            fname = f.get("filename", "")
            ext   = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
            if f".{ext}" not in AUDIO_DL_EXTS:
                continue
            folder = fname.rsplit("\\", 1)[0] if "\\" in fname else ""
            key    = (username, folder)
            if key not in folders:
                folders[key] = {"username": username, "folder": folder,
                                "files": [], "flac_count": 0, "total_size": 0, "slots": slots}
            folders[key]["files"].append(f)
            folders[key]["total_size"] += f.get("size", 0)
            if fname.lower().endswith(".flac"):
                folders[key]["flac_count"] += 1

    ranked = sorted(folders.values(),
                    key=lambda x: (-x["flac_count"], -x["total_size"]))
    return ranked


def _download_worker(dl_id: str, track: dict):
    artist = track['artist']
    name   = track['name']
    album  = track.get('album', '')

    def fail(msg):
        _dl_log(dl_id, f"ERROR: {msg}")
        with _dl_lock:
            _downloads[dl_id].update({"status": "error",
                                       "finished": datetime.now().isoformat(timespec="seconds")})

    with _dl_lock:
        _downloads[dl_id].update({"status": "searching",
                                   "started": datetime.now().isoformat(timespec="seconds")})

    # Decide search mode: album search when album name is known
    if album:
        query = f"{artist} {album}"
        album_mode = True
    else:
        query = f"{artist} {name}"
        album_mode = False

    # 1. Start search (retry once if slskd returns 0 results)
    search_id = None
    results   = []
    for attempt in range(2):
        if attempt:
            _dl_log(dl_id, "No results — retrying search in 5s…")
            time.sleep(5)
        _dl_log(dl_id, f"Searching slskd ({'album' if album_mode else 'track'}): {query}")
        res = _slskd("POST", "/api/v0/searches",
                     {"searchText": query, "responseLimit": 50, "fileLimit": 1000})
        if "_error" in res:
            return fail(f"Search failed: {res}")
        search_id = res.get("id")
        if not search_id:
            return fail(f"No search ID returned: {res}")

        # 2. Wait for search to complete (up to 45s)
        for _ in range(45):
            time.sleep(1)
            info = _slskd("GET", f"/api/v0/searches/{search_id}")
            if "Completed" in info.get("state", "") or info.get("isComplete"):
                break

        resp    = _slskd("GET", f"/api/v0/searches/{search_id}/responses")
        results = resp if isinstance(resp, list) else resp.get("responses", [])
        _dl_log(dl_id, f"Search done — {len(results)} peers responded")
        if results:
            break
        _slskd("DELETE", f"/api/v0/searches/{search_id}")

    if not results:
        return fail(f"No peers found for: {query}")

    # 3. Group files by folder and pick best source
    folders = _peer_folders(results)
    if not folders:
        return fail(f"No audio files found for: {query}")

    chosen = None

    if album_mode:
        # Find folder whose name contains album/artist and has the most FLACs
        norm_album    = _norm(album)
        norm_artist_q = _norm(artist)
        for folder in folders:
            norm_folder = _norm(folder["folder"])
            if (norm_album in norm_folder or norm_artist_q in norm_folder) and folder["flac_count"] > 0:
                chosen = folder
                break
        # Fallback: any folder with FLACs
        if not chosen:
            chosen = next((f for f in folders if f["flac_count"] > 0), None)

    if not chosen:
        # Single-track mode: find any folder containing a file matching the track name
        norm_name = _norm(name)
        for folder in folders:
            for f in folder["files"]:
                base = _norm(f["filename"].rsplit("\\", 1)[-1].rsplit(".", 1)[0])
                if norm_name in base:
                    chosen = folder
                    break
            if chosen:
                break
        # Last resort: best folder overall
        if not chosen:
            chosen = folders[0]

    username = chosen["username"]
    files_to_dl = chosen["files"] if album_mode else [
        next((f for f in chosen["files"]
              if _norm(name) in _norm(f["filename"].rsplit("\\", 1)[-1])),
             chosen["files"][0])
    ]

    # Keep only audio files (skip .xml, .jpg, .cue etc that sneak in)
    files_to_dl = [f for f in files_to_dl
                   if "." + f["filename"].rsplit(".", 1)[-1].lower() in AUDIO_DL_EXTS]
    if not files_to_dl:
        return fail(f"No audio files remain after filtering for: {query}")

    flac_count = sum(1 for f in files_to_dl if f["filename"].lower().endswith(".flac"))
    _dl_log(dl_id, f"{'Album' if album_mode else 'Track'} from {username}: "
                   f"{len(files_to_dl)} files ({flac_count} FLAC) in "
                   f"{chosen['folder'].rsplit(chr(92), 1)[-1]}")

    with _dl_lock:
        _downloads[dl_id]["status"] = "downloading"

    # 4. Queue all files in one request
    payload = [{"filename": f["filename"], "size": f.get("size", 0)} for f in files_to_dl]
    dl_res  = _slskd("POST", f"/api/v0/transfers/downloads/{username}", payload)
    if isinstance(dl_res, dict) and "_error" in dl_res:
        return fail(f"Download queue failed: {dl_res}")

    queued_filenames = {f["filename"] for f in files_to_dl}
    total            = len(queued_filenames)

    # 5. Monitor until all files complete (up to 30 min)
    # Use persistent sets so we don't lose state when slskd clears completed entries.
    ever_seen: bool   = False
    confirmed_done: set = set()
    confirmed_fail: set = set()

    for _ in range(1800):
        time.sleep(2)
        transfers = _slskd("GET", "/api/v0/transfers/downloads")

        # Skip transient API errors — keep polling rather than treating as empty
        if isinstance(transfers, dict) and "_error" in transfers:
            _dl_log(dl_id, f"Transfer poll error (retrying): {transfers}")
            continue

        states: dict = {}
        for peer_group in (transfers if isinstance(transfers, list) else []):
            if peer_group.get("username") != username:
                continue
            for d in peer_group.get("directories", []):
                for f in d.get("files", []):
                    if f.get("filename") in queued_filenames:
                        states[f["filename"]] = f

        if states:
            ever_seen = True
            for fname, finfo in states.items():
                s = finfo.get("state", "")
                if "Completed" in s and "Succeeded" in s:
                    confirmed_done.add(fname)
                elif "Completed" in s:
                    confirmed_fail.add(fname)
        elif not ever_seen:
            # Files never appeared in the queue — something went wrong at queue time
            break

        # Files that were visible before but have since been cleared by slskd
        # (slskd removes succeeded transfers from the list after a short window)
        cleared = queued_filenames - set(states.keys()) - confirmed_done - confirmed_fail

        n_done  = len(confirmed_done) + len(cleared)  # cleared == implicitly succeeded
        n_error = len(confirmed_fail)
        avg_pct = (sum(f.get("percentComplete", 0) for f in states.values()) / len(states)
                   if states else 100)
        tot_bytes  = sum(int(f.get("size", 0)) for f in states.values())
        done_bytes = sum(int(f.get("bytesTransferred", 0)) for f in states.values())
        avg_speed  = sum(float(f.get("averageSpeed", 0)) for f in states.values())
        eta_s = int((tot_bytes - done_bytes) / avg_speed) if avg_speed > 1 else None
        with _dl_lock:
            _downloads[dl_id]["progress"] = {
                "pct": avg_pct,
                "downloaded": done_bytes,
                "total": tot_bytes or None,
                "speed_bps": avg_speed,
                "eta_secs": eta_s,
                "stage": "downloading",
                "n_done": n_done,
                "total_files": total,
            }
        _dl_log(dl_id, f"{n_done}/{total} done, {n_error} errors — {avg_pct:.0f}% avg")

        if n_done + n_error >= total or (not states and ever_seen):
            if n_error == 0:
                with _dl_lock:
                    _downloads[dl_id].update({"status": "done",
                                               "finished": datetime.now().isoformat(timespec="seconds")})
                _dl_log(dl_id, f"Done! {n_done} file(s) downloaded.")
            else:
                fail(f"{n_error}/{total} files failed")
            _slskd("DELETE", f"/api/v0/searches/{search_id}")
            return

    fail("Timed out waiting for download")
    _slskd("DELETE", f"/api/v0/searches/{search_id}")

def start_download(track: dict) -> str:
    dl_id = str(uuid.uuid4())[:8]
    with _dl_lock:
        _downloads[dl_id] = {"track": track, "status": "queued", "output": [],
                              "started": None, "finished": None}
    threading.Thread(target=_download_worker, args=(dl_id, track), daemon=True).start()
    return dl_id

# ── Flask ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)

_JOBS_JS = json.dumps({k: {"name": v["name"], "icon": v["icon"]} for k, v in JOBS.items()})

# ── Main page HTML ─────────────────────────────────────────────────────────────
MAIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NAS Controller</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    .spin{animation:spin 1s linear infinite;display:inline-block}
    @keyframes spin{to{transform:rotate(360deg)}}
    .pulse{animation:pulse 2s infinite}
    @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
    #log-box{font-family:monospace;font-size:.75rem}
  </style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen p-6">
<div class="max-w-xl mx-auto space-y-8">

  <div class="flex justify-between items-start">
    <div><h1 class="text-2xl font-bold mb-1">NAS Controller</h1>
    <p class="text-gray-400 text-sm">Trigger cron jobs on demand</p></div>
    <div class="flex gap-2">
      <a href="/discover" class="px-3 py-1.5 rounded-lg bg-purple-800 hover:bg-purple-700 text-sm font-medium text-white">🔭 Discover</a>
      <a href="/spotify" class="px-3 py-1.5 rounded-lg bg-green-800 hover:bg-green-700 text-sm font-medium text-white">🎵 Spotify Import</a>
    </div>
  </div>

  <div id="jobs" class="space-y-3"></div>

  <div>
    <div class="flex items-center justify-between mb-3">
      <div><h2 class="text-lg font-semibold">File Watcher</h2>
      <p class="text-gray-400 text-xs">Auto-runs pipeline when new music is detected</p></div>
      <button id="watch-toggle" onclick="toggleWatcher()" class="px-3 py-1.5 rounded-lg text-sm font-medium"></button>
    </div>
    <div id="watcher-panel" class="space-y-2"></div>
  </div>
</div>

<!-- Log modal -->
<div id="modal" class="hidden fixed inset-0 bg-black/80 flex items-center justify-center z-50 p-4">
  <div class="bg-gray-900 rounded-xl w-full max-w-2xl flex flex-col" style="max-height:80vh">
    <div class="flex justify-between items-center p-5 pb-3 shrink-0">
      <div><h2 id="modal-title" class="font-semibold text-lg"></h2>
      <span id="modal-badge" class="text-xs text-gray-400"></span></div>
      <button onclick="closeModal()" class="text-gray-400 hover:text-white text-xl leading-none">✕</button>
    </div>
    <div id="log-box" class="bg-gray-950 rounded-lg mx-5 mb-5 p-4 text-green-400 overflow-auto flex-1 whitespace-pre-wrap"></div>
  </div>
</div>

<script>
const JOBS = """ + _JOBS_JS + r""";
let statusCache={}, watchCache={}, activeStream=null, activeJob=null, autoScroll=true;

async function poll(){
  try{
    const [sr,wr]=await Promise.all([fetch('/status'),fetch('/watcher/status')]);
    const sd=await sr.json(), wd=await wr.json();
    statusCache=sd.jobs; watchCache=wd;
    renderJobs(sd.jobs); renderWatcher(wd);
    if(activeJob&&statusCache[activeJob]) updateModalBadge(activeJob);
  }catch(_){}
}

function badge(id,s){
  if(s.running) return`<span class="text-yellow-400 text-sm flex items-center gap-1"><span class="spin">⟳</span> Running…</span>`;
  if(!s.last_run) return`<span class="text-gray-500 text-sm">Never run</span>`;
  const col=s.success?'text-green-400':'text-red-400',icon=s.success?'✓':'✗';
  return`<span class="${col} text-sm">${icon} ${s.last_run.replace('T',' ')} (${s.elapsed}s)</span>`;
}

function renderJobs(jobs){
  document.getElementById('jobs').innerHTML=Object.entries(JOBS).map(([id,job])=>{
    const s=jobs[id]||{},running=s.running,hasLog=running||!!s.output;
    return`<div class="bg-gray-900 rounded-xl p-4 flex items-center gap-4">
      <div class="flex-1 min-w-0"><div class="font-medium">${job.icon} ${job.name}</div>
      <div class="mt-0.5">${badge(id,s)}</div></div>
      <div class="flex gap-2 shrink-0">
        ${hasLog?`<button onclick="openModal('${id}')" class="px-3 py-1.5 rounded-lg bg-gray-800 hover:bg-gray-700 text-sm">${running?'📡 Live':'Logs'}</button>`:''}
        <button id="btn-${id}" onclick="triggerJob('${id}',this)" ${running?'disabled':''}
          class="px-4 py-1.5 rounded-lg text-sm font-medium ${running?'bg-gray-700 text-gray-500 cursor-not-allowed':'bg-indigo-600 hover:bg-indigo-500 text-white'}">
          ${running?'Running':'Run'}</button>
      </div></div>`;
  }).join('');
}

async function triggerJob(id,btn){
  btn.disabled=true;btn.textContent='Starting…';
  await fetch(`/run/${id}`,{method:'POST'});
  setTimeout(()=>{poll();openModal(id);},400);
}

function renderWatcher(w){
  const toggle=document.getElementById('watch-toggle');
  toggle.textContent=w.enabled?'⏸ Pause':'▶ Resume';
  toggle.className=`px-3 py-1.5 rounded-lg text-sm font-medium ${w.enabled?'bg-yellow-700 hover:bg-yellow-600':'bg-green-700 hover:bg-green-600'} text-white`;
  const dot=w.enabled?'<span class="pulse text-green-400">●</span>':'<span class="text-gray-500">●</span>';
  const scan=w.last_scan?`Last scan: ${w.last_scan.replace('T',' ')}`:'Not yet scanned';
  const pendHtml=Object.entries(w.pending||{}).map(([dir,info])=>{
    const wait=Math.max(0,""" + str(DEBOUNCE_SECS) + r"""-Math.round(Date.now()/1000-info.last_seen));
    return`<div class="bg-gray-800 rounded-lg px-3 py-2 text-sm flex justify-between">
      <span>📁 ${dir} <span class="text-gray-400">(${info.files.length} files)</span></span>
      <span class="text-yellow-400">~${wait}s…</span></div>`;
  }).join('');
  const recHtml=(w.recent||[]).map(r=>`<div class="text-sm text-gray-300 flex justify-between">
    <span>${r.success?'✅':'⚠️'} ${r.dir} <span class="text-gray-500">(${r.count} tracks)</span></span>
    <span class="text-gray-500">${r.ts.replace('T',' ')}</span></div>`).join('');
  document.getElementById('watcher-panel').innerHTML=`
    <div class="bg-gray-900 rounded-xl p-4 space-y-3">
      <div class="flex items-center gap-2 text-sm">${dot} <span>${w.enabled?'Watching':'Paused'}</span>
        <span class="text-gray-500 ml-2">${scan}</span></div>
      ${pendHtml?`<div class="space-y-1"><div class="text-xs text-gray-400 uppercase mb-1">Pending</div>${pendHtml}</div>`:''}
      ${recHtml?`<div class="space-y-1"><div class="text-xs text-gray-400 uppercase mb-1">Recent</div>${recHtml}</div>`:''}
      ${!pendHtml&&!recHtml?'<div class="text-sm text-gray-500">No activity yet</div>':''}
    </div>`;
}

async function toggleWatcher(){await fetch('/watcher/toggle',{method:'POST'});poll();}

function openModal(jobId){
  activeJob=jobId;autoScroll=true;
  document.getElementById('modal-title').textContent=JOBS[jobId].icon+' '+JOBS[jobId].name;
  updateModalBadge(jobId);
  const box=document.getElementById('log-box');box.innerHTML='';
  document.getElementById('modal').classList.remove('hidden');
  if(activeStream){activeStream.close();activeStream=null;}
  const es=new EventSource(`/logs/${jobId}/stream`);activeStream=es;
  es.onmessage=e=>{
    if(e.data==='"__DONE__"'){es.close();activeStream=null;updateModalBadge(jobId);return;}
    const d=document.createElement('div');d.textContent=JSON.parse(e.data);box.appendChild(d);
    if(autoScroll)box.scrollTop=box.scrollHeight;
  };
  es.onerror=()=>{es.close();activeStream=null;};
  box.onscroll=()=>{autoScroll=box.scrollTop+box.clientHeight>=box.scrollHeight-10;};
}

function updateModalBadge(jobId){
  const s=statusCache[jobId]||{},b=document.getElementById('modal-badge');if(!b)return;
  if(s.running){b.textContent='🔴 LIVE';b.className='text-xs text-red-400 animate-pulse';}
  else if(s.last_run){b.textContent=`${s.success?'✅':'❌'} done in ${s.elapsed}s`;b.className='text-xs text-gray-400';}
}

function closeModal(){
  if(activeStream){activeStream.close();activeStream=null;}
  activeJob=null;document.getElementById('modal').classList.add('hidden');
}
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal();});
poll();setInterval(poll,3000);
</script></body></html>"""

# ── Spotify page HTML ──────────────────────────────────────────────────────────
SPOTIFY_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Spotify Import - NAS Controller</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body{font-family:system-ui,sans-serif}
    .spin{animation:spin 1s linear infinite;display:inline-block}
    @keyframes spin{to{transform:rotate(360deg)}}
    #dl-log{font-family:monospace;font-size:.7rem}
  </style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen p-6">
<div class="max-w-4xl mx-auto space-y-6">

  <div class="flex items-center justify-between">
    <div><h1 class="text-2xl font-bold">🎵 Spotify Import</h1>
    <p class="text-gray-400 text-sm mt-1">Find tracks missing from your Navidrome library and download them</p></div>
    <a href="/" class="text-gray-400 hover:text-white text-sm">← Back</a>
  </div>

  <!-- Controls -->
  <div class="bg-gray-900 rounded-xl p-5 space-y-4">
    <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">

      <!-- CSV Upload -->
      <div>
        <label class="block text-sm font-medium text-gray-300 mb-2">1. Upload Spotify CSV</label>
        <div class="flex gap-2">
          <input type="file" id="csv-input" accept=".csv"
            class="flex-1 text-sm text-gray-300 bg-gray-800 rounded-lg px-3 py-2 file:mr-3 file:py-1 file:px-2 file:rounded file:border-0 file:text-sm file:bg-indigo-600 file:text-white cursor-pointer">
          <button onclick="uploadCSV()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-lg text-sm font-medium shrink-0">Upload</button>
        </div>
        <p id="csv-status" class="text-xs text-gray-400 mt-1.5"></p>
      </div>

      <!-- Navidrome refresh -->
      <div>
        <label class="block text-sm font-medium text-gray-300 mb-2">2. Load Navidrome Library</label>
        <button onclick="loadNavidrome()" id="nav-btn"
          class="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg text-sm font-medium">
          ↻ Load Library
        </button>
        <p id="nav-status" class="text-xs text-gray-400 mt-1.5"></p>
      </div>
    </div>

    <!-- Playlist filter + search -->
    <div class="flex flex-wrap gap-3 items-end pt-2 border-t border-gray-800">
      <div>
        <label class="block text-xs text-gray-400 mb-1">Filter playlist</label>
        <select id="playlist-filter" class="bg-gray-800 rounded-lg px-3 py-2 text-sm min-w-48">
          <option value="">All playlists</option>
        </select>
      </div>
      <button onclick="findMissing()" id="find-btn"
        class="px-4 py-2 bg-green-700 hover:bg-green-600 rounded-lg text-sm font-medium">
        3. Find Missing Tracks
      </button>
      <span id="missing-count" class="text-sm text-gray-400"></span>
    </div>
  </div>

  <!-- Results table -->
  <div id="results-section" class="hidden">
    <div class="flex items-center justify-between mb-3">
      <h2 class="font-semibold" id="results-title"></h2>
      <div class="flex gap-2 items-center">
        <input id="search-box" oninput="filterTable()" placeholder="Search artist / track…"
          class="bg-gray-800 rounded-lg px-3 py-1.5 text-sm w-48 placeholder-gray-500">
      </div>
    </div>

    <div class="bg-gray-900 rounded-xl overflow-hidden">
      <table class="w-full text-sm">
        <thead class="bg-gray-800 text-gray-400 text-xs uppercase">
          <tr>
            <th class="px-4 py-3 text-left w-10">#</th>
            <th class="px-4 py-3 text-left">Artist</th>
            <th class="px-4 py-3 text-left">Track</th>
            <th class="px-4 py-3 text-left hidden sm:table-cell">Album</th>
            <th class="px-4 py-3 text-left hidden md:table-cell">Playlist</th>
            <th class="px-4 py-3 text-right w-28">Action</th>
          </tr>
        </thead>
        <tbody id="results-body" class="divide-y divide-gray-800"></tbody>
      </table>
    </div>

    <!-- Pagination -->
    <div id="pagination" class="flex gap-2 items-center justify-center mt-4 flex-wrap"></div>
  </div>

  <!-- Active downloads -->
  <div id="downloads-section" class="hidden">
    <h2 class="font-semibold mb-3">Downloads</h2>
    <div id="downloads-list" class="space-y-2"></div>
  </div>
</div>

<!-- Download log modal -->
<div id="dl-modal" class="hidden fixed inset-0 bg-black/80 flex items-center justify-center z-50 p-4">
  <div class="bg-gray-900 rounded-xl w-full max-w-2xl flex flex-col" style="max-height:70vh">
    <div class="flex justify-between items-center p-5 pb-3 shrink-0">
      <h2 id="dl-modal-title" class="font-semibold"></h2>
      <button onclick="document.getElementById('dl-modal').classList.add('hidden')"
        class="text-gray-400 hover:text-white text-xl">✕</button>
    </div>
    <div id="dl-log" class="bg-gray-950 rounded-lg mx-5 mb-5 p-4 text-green-400 overflow-auto flex-1 whitespace-pre-wrap"></div>
  </div>
</div>

<script>
let allMissing=[], filteredMissing=[], currentPage=0;
const PAGE_SIZE=50;
let downloads={};
let dlStreams={};

// ── Init ─────────────────────────────────────────────────────────────────────
async function init(){
  await fetchSpotifyStatus();
  setInterval(fetchDownloads,3000);
}

async function fetchSpotifyStatus(){
  try{
    const r=await fetch('/spotify/status');
    const d=await r.json();
    if(d.last_import){
      document.getElementById('csv-status').textContent=
        `✓ ${d.track_count} tracks loaded · ${d.playlists.length} playlists · ${d.last_import.replace('T',' ')}`;
      populatePlaylists(d.playlists);
    }
    if(d.nav_loaded){
      document.getElementById('nav-status').textContent=
        `✓ Library indexed (~${d.nav_songs} songs) · ${d.nav_loaded.replace('T',' ')}`;
    }
  }catch(_){}
}

function populatePlaylists(playlists){
  const sel=document.getElementById('playlist-filter');
  const cur=sel.value;
  sel.innerHTML='<option value="">All playlists</option>'+
    playlists.map(p=>`<option value="${p}"${p===cur?' selected':''}>${p}</option>`).join('');
}

// ── CSV Upload ────────────────────────────────────────────────────────────────
async function uploadCSV(){
  const file=document.getElementById('csv-input').files[0];
  if(!file){alert('Select a CSV file first');return;}
  const fd=new FormData();fd.append('file',file);
  document.getElementById('csv-status').textContent='Uploading…';
  try{
    const r=await fetch('/spotify/upload',{method:'POST',body:fd});
    const d=await r.json();
    if(d.error){document.getElementById('csv-status').textContent='❌ '+d.error;return;}
    document.getElementById('csv-status').textContent=
      `✓ ${d.track_count} tracks · ${d.playlist_count} playlists`;
    populatePlaylists(d.playlists);
  }catch(e){document.getElementById('csv-status').textContent='❌ '+e;}
}

// ── Navidrome ─────────────────────────────────────────────────────────────────
async function loadNavidrome(){
  const btn=document.getElementById('nav-btn');
  btn.disabled=true;btn.textContent='Loading…';
  document.getElementById('nav-status').textContent='Fetching library from Navidrome…';
  try{
    const r=await fetch('/spotify/load-navidrome',{method:'POST'});
    const d=await r.json();
    document.getElementById('nav-status').textContent=
      d.error?'❌ '+d.error:`✓ ~${d.songs} songs indexed`;
  }catch(e){document.getElementById('nav-status').textContent='❌ '+e;}
  btn.disabled=false;btn.textContent='↻ Load Library';
}

// ── Find Missing ──────────────────────────────────────────────────────────────
async function findMissing(){
  const playlist=document.getElementById('playlist-filter').value;
  const btn=document.getElementById('find-btn');
  btn.disabled=true;btn.textContent='Searching…';
  document.getElementById('missing-count').textContent='';
  try{
    const url='/spotify/missing'+(playlist?'?playlist='+encodeURIComponent(playlist):'');
    const r=await fetch(url);
    const d=await r.json();
    if(d.error){alert(d.error);return;}
    allMissing=d.tracks;
    filteredMissing=allMissing;
    currentPage=0;
    document.getElementById('missing-count').textContent=
      `${d.tracks.length} missing out of ${d.total} tracks`;
    document.getElementById('results-title').textContent=
      `Missing tracks${playlist?' — '+playlist:''} (${d.tracks.length})`;
    document.getElementById('results-section').classList.remove('hidden');
    renderTable();
  }catch(e){alert('Error: '+e);}
  btn.disabled=false;btn.textContent='3. Find Missing Tracks';
}

// ── Table ─────────────────────────────────────────────────────────────────────
function filterTable(){
  const q=document.getElementById('search-box').value.toLowerCase();
  filteredMissing=q?allMissing.filter(t=>
    t.artist.toLowerCase().includes(q)||t.name.toLowerCase().includes(q)):allMissing;
  currentPage=0;renderTable();
}

function renderTable(){
  const start=currentPage*PAGE_SIZE;
  const page=filteredMissing.slice(start,start+PAGE_SIZE);
  document.getElementById('results-body').innerHTML=page.map((t,i)=>{
    const num=start+i+1;
    const dlId=dlForTrack(t);
    const dl=dlId?downloads[dlId]:null;
    let action='';
    if(dl){
      if(dl.status==='done') action='<span class="text-green-400 text-xs">✓ Downloaded</span>';
      else if(dl.status==='error') action=`<button onclick="showDlLog('${dlId}')" class="text-red-400 text-xs">❌ Error</button>`;
      else action=`<button onclick="showDlLog('${dlId}')" class="text-yellow-400 text-xs"><span class="spin">⟳</span> ${dl.status}</button>`;
    } else {
      action=`<button onclick="download(${JSON.stringify(t).replace(/"/g,'&quot;')})"
        class="px-3 py-1 bg-indigo-600 hover:bg-indigo-500 rounded text-xs font-medium">↓ Download</button>`;
    }
    return`<tr class="hover:bg-gray-800/50">
      <td class="px-4 py-2.5 text-gray-500">${num}</td>
      <td class="px-4 py-2.5 font-medium">${esc(t.artist)}</td>
      <td class="px-4 py-2.5">${esc(t.name)}</td>
      <td class="px-4 py-2.5 text-gray-400 hidden sm:table-cell">${esc(t.album)}</td>
      <td class="px-4 py-2.5 text-gray-400 hidden md:table-cell">${esc(t.playlist)}</td>
      <td class="px-4 py-2.5 text-right">${action}</td></tr>`;
  }).join('');
  renderPagination();
}

function renderPagination(){
  const total=Math.ceil(filteredMissing.length/PAGE_SIZE);
  if(total<=1){document.getElementById('pagination').innerHTML='';return;}
  let html='';
  if(currentPage>0) html+=`<button onclick="goPage(${currentPage-1})" class="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 rounded text-sm">← Prev</button>`;
  html+=`<span class="text-sm text-gray-400">Page ${currentPage+1} / ${total}</span>`;
  if(currentPage<total-1) html+=`<button onclick="goPage(${currentPage+1})" class="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 rounded text-sm">Next →</button>`;
  document.getElementById('pagination').innerHTML=html;
}

function goPage(p){currentPage=p;renderTable();window.scrollTo(0,0);}

// ── Downloads ─────────────────────────────────────────────────────────────────
function dlForTrack(t){
  return Object.keys(downloads).find(id=>{
    const d=downloads[id];
    return d.track.artist===t.artist&&d.track.name===t.name;
  })||null;
}

async function download(track){
  try{
    const r=await fetch('/spotify/download',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({track})});
    const d=await r.json();
    downloads[d.dl_id]={track,status:'queued',output:[]};
    document.getElementById('downloads-section').classList.remove('hidden');
    renderTable();fetchDownloads();
    streamDlLog(d.dl_id,track);
  }catch(e){alert('Download failed: '+e);}
}

async function fetchDownloads(){
  try{
    const r=await fetch('/spotify/downloads');
    const d=await r.json();
    downloads=d.downloads;
    renderDownloads();
    if(Object.keys(filteredMissing).length) renderTable();
  }catch(_){}
}

function renderDownloads(){
  const entries=Object.entries(downloads);
  if(!entries.length) return;
  document.getElementById('downloads-section').classList.remove('hidden');
  document.getElementById('downloads-list').innerHTML=entries.reverse().map(([id,d])=>{
    const icon=d.status==='done'?'✅':d.status==='error'?'❌':d.status==='downloading'?'<span class="spin">⟳</span>':'⏳';
    return`<div class="bg-gray-900 rounded-lg px-4 py-3 flex items-center justify-between gap-3">
      <div class="flex-1 min-w-0">
        <div class="text-sm font-medium">${icon} ${esc(d.track.artist)} — ${esc(d.track.name)}</div>
        <div class="text-xs text-gray-400 mt-0.5">${d.status}${d.finished?' · '+d.finished.replace('T',' '):''}</div>
      </div>
      <button onclick="showDlLog('${id}')" class="text-xs text-gray-400 hover:text-white shrink-0">Logs</button>
    </div>`;
  }).join('');
}

function streamDlLog(dlId,track){
  if(dlStreams[dlId]) return;
  const es=new EventSource(`/spotify/download/${dlId}/stream`);
  dlStreams[dlId]=es;
  es.onmessage=e=>{
    if(e.data==='"__DONE__"'){es.close();delete dlStreams[dlId];return;}
    if(downloads[dlId]) downloads[dlId].output=(downloads[dlId].output||[]).concat(JSON.parse(e.data));
  };
  es.onerror=()=>{es.close();delete dlStreams[dlId];};
}

function showDlLog(dlId){
  const d=downloads[dlId];if(!d) return;
  document.getElementById('dl-modal-title').textContent=d.track.artist+' — '+d.track.name;
  document.getElementById('dl-log').textContent=(d.output||[]).join('')||'(no output yet)';
  document.getElementById('dl-modal').classList.remove('hidden');
}

function esc(s){return(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
document.addEventListener('keydown',e=>{if(e.key==='Escape'){
  document.getElementById('dl-modal').classList.add('hidden');}});
init();
</script></body></html>"""

# ── Discover page HTML ─────────────────────────────────────────────────────────
DISCOVER_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Discover - NAS Controller</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    .spin{animation:spin 1s linear infinite;display:inline-block}
    @keyframes spin{to{transform:rotate(360deg)}}
  </style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen p-6">
<div class="max-w-5xl mx-auto space-y-6">

  <div class="flex items-center justify-between">
    <div>
      <h1 class="text-2xl font-bold">🔭 Discover</h1>
      <p class="text-gray-400 text-sm mt-1">Artists similar to your library — not yet in Navidrome</p>
    </div>
    <a href="/" class="text-gray-400 hover:text-white text-sm">← Back</a>
  </div>

  <div id="setup-warn" class="hidden bg-yellow-900/40 border border-yellow-700 rounded-xl p-4 text-sm text-yellow-200">
    ⚠️ <strong>Last.fm API key not configured.</strong>
    Add <code class="bg-yellow-900 px-1 rounded">LASTFM_API_KEY</code> to your docker-compose env
    and get a free key at <a href="https://www.last.fm/api/account/create" class="underline">last.fm/api</a>.
  </div>

  <div class="bg-gray-900 rounded-xl p-5 flex items-center justify-between gap-4">
    <div>
      <div id="status-text" class="text-sm text-gray-300">Loading…</div>
      <div id="status-sub" class="text-xs text-gray-500 mt-0.5"></div>
    </div>
    <div class="flex gap-2">
      <input id="search-box" oninput="filterArtists()" placeholder="Search artist…"
        class="bg-gray-800 rounded-lg px-3 py-1.5 text-sm w-44 placeholder-gray-500">
      <button onclick="refresh()" id="refresh-btn"
        class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-lg text-sm font-medium shrink-0">
        ↻ Refresh
      </button>
    </div>
  </div>

  <div id="grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4"></div>
  <div id="pagination" class="flex gap-2 items-center justify-center mt-2 flex-wrap"></div>

  <div id="downloads-section" class="hidden mt-4">
    <h2 class="font-semibold mb-3">Downloads</h2>
    <div id="downloads-list" class="space-y-2"></div>
  </div>
</div>

<script>
let allArtists=[], filteredArtists=[], downloads={}, currentPage=0;
const PAGE_SIZE=30;

async function init(){
  await loadStatus();
  await fetchDownloads();
  setInterval(async()=>{ await loadStatus(); await fetchDownloads(); },8000);
}

async function loadStatus(){
  try{
    const r=await fetch('/discover/status');
    const d=await r.json();
    if(!d.key_configured){
      document.getElementById('setup-warn').classList.remove('hidden');
      document.getElementById('status-text').textContent='Last.fm API key missing';
      return;
    }
    if(d.refreshing){
      document.getElementById('status-text').innerHTML='<span class="spin">⟳</span> Refreshing from Last.fm…';
      document.getElementById('refresh-btn').disabled=true;
    } else {
      document.getElementById('status-text').textContent=
        d.artists.length>0?`${d.artists.length} artists discovered`:'No results yet — click Refresh';
      document.getElementById('refresh-btn').disabled=false;
    }
    if(d.last_refresh)
      document.getElementById('status-sub').textContent='Last updated: '+d.last_refresh.replace('T',' ');
    allArtists=d.artists||[];
    filterArtists();
  }catch(_){}
}

function filterArtists(){
  const q=document.getElementById('search-box').value.toLowerCase();
  filteredArtists=q?allArtists.filter(a=>a.artist.toLowerCase().includes(q)||
    (a.top_album||'').toLowerCase().includes(q)):allArtists;
  currentPage=0;renderGrid();
}

function renderGrid(){
  const start=currentPage*PAGE_SIZE;
  const page=filteredArtists.slice(start,start+PAGE_SIZE);
  document.getElementById('grid').innerHTML=page.map(a=>{
    const dlId=dlForArtist(a);
    const dl=dlId?downloads[dlId]:null;
    let action='';
    const album=a.top_album||'';
    if(dl){
      if(dl.status==='done') action='<span class="text-green-400 text-xs">✓ Downloaded</span>';
      else if(dl.status==='error') action='<span class="text-red-400 text-xs">❌ Error</span>';
      else action=`<span class="text-yellow-400 text-xs"><span class="spin">⟳</span> ${dl.status}</span>`;
    } else if(album){
      const payload=JSON.stringify({artist:a.artist,album}).replace(/"/g,'&quot;');
      action=`<button onclick="dlAlbum(${payload})"
        class="px-3 py-1.5 bg-indigo-600 hover:bg-indigo-500 rounded-lg text-xs font-medium">↓ Download</button>`;
    } else {
      action='<span class="text-gray-500 text-xs">No album data</span>';
    }
    const because=a.similar_to&&a.similar_to.length?
      `<div class="text-xs text-gray-500 mt-1">Because you like: ${a.similar_to.join(', ')}</div>`:'';
    const match=a.score?`<span class="text-gray-500 text-xs">${Math.round(a.score*100)}% match</span>`:'';
    return`<div class="bg-gray-900 rounded-xl p-4 flex flex-col gap-3">
      <div class="flex-1">
        <div class="font-semibold text-base">${esc(a.artist)}</div>
        ${album?`<div class="text-sm text-gray-300 mt-0.5">💿 ${esc(album)}</div>`:''}
        ${because}
      </div>
      <div class="flex items-center justify-between gap-2">
        ${match}
        ${action}
      </div>
    </div>`;
  }).join('');
  renderPagination();
  renderDownloads();
}

function renderPagination(){
  const total=Math.ceil(filteredArtists.length/PAGE_SIZE);
  if(total<=1){document.getElementById('pagination').innerHTML='';return;}
  let h='';
  if(currentPage>0) h+=`<button onclick="goPage(${currentPage-1})" class="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 rounded text-sm">← Prev</button>`;
  h+=`<span class="text-sm text-gray-400">Page ${currentPage+1} / ${total}</span>`;
  if(currentPage<total-1) h+=`<button onclick="goPage(${currentPage+1})" class="px-3 py-1.5 bg-gray-800 hover:bg-gray-700 rounded text-sm">Next →</button>`;
  document.getElementById('pagination').innerHTML=h;
}

function goPage(p){currentPage=p;renderGrid();window.scrollTo(0,0);}

function dlForArtist(a){
  return Object.keys(downloads).find(id=>{
    const d=downloads[id];
    return d.track&&d.track.artist===a.artist&&d.track.album===a.top_album;
  })||null;
}

async function dlAlbum(payload){
  const track={artist:payload.artist,name:'',album:payload.album};
  try{
    const r=await fetch('/spotify/download',{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({track})});
    const d=await r.json();
    downloads[d.dl_id]={track,status:'queued',output:[]};
    document.getElementById('downloads-section').classList.remove('hidden');
    renderGrid();
  }catch(e){alert('Download failed: '+e);}
}

async function fetchDownloads(){
  try{
    const r=await fetch('/spotify/downloads');
    const d=await r.json();
    downloads=d.downloads;
    renderGrid();
  }catch(_){}
}

function renderDownloads(){
  const entries=Object.entries(downloads).filter(([,d])=>d.track&&d.track.album&&!d.track.name);
  if(!entries.length) return;
  document.getElementById('downloads-section').classList.remove('hidden');
  document.getElementById('downloads-list').innerHTML=entries.reverse().map(([id,d])=>{
    const icon=d.status==='done'?'✅':d.status==='error'?'❌':'<span class="spin">⟳</span>';
    return`<div class="bg-gray-900 rounded-lg px-4 py-3 flex items-center gap-3">
      <div class="flex-1 min-w-0">
        <div class="text-sm font-medium">${icon} ${esc(d.track.artist)} — ${esc(d.track.album)}</div>
        <div class="text-xs text-gray-400 mt-0.5">${d.status}${d.finished?' · '+d.finished.replace('T',' '):''}</div>
      </div>
    </div>`;
  }).join('');
}

async function refresh(){
  document.getElementById('refresh-btn').disabled=true;
  document.getElementById('status-text').innerHTML='<span class="spin">⟳</span> Starting refresh…';
  await fetch('/discover/refresh',{method:'POST'});
  await loadStatus();
}

function esc(s){return(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
init();
</script></body></html>"""

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route("/")
def index(): return MAIN_HTML

@app.route("/spotify")
def spotify_page():
    if not _nav_index:
        threading.Thread(target=load_nav_index, daemon=True).start()
    return SPOTIFY_HTML

@app.route("/status")
def status():
    with _state_lock:
        return jsonify({"jobs": {
            k: {"name": v["name"], "running": _locks[k], **_history.get(k, {})}
            for k, v in JOBS.items()
        }})

@app.route("/run/<job_id>", methods=["POST"])
def trigger(job_id):
    if job_id not in JOBS: return jsonify({"error": "unknown job"}), 404
    threading.Thread(target=run_job, args=(job_id,), daemon=True).start()
    return jsonify({"status": "started", "job": job_id})

@app.route("/logs/<job_id>/stream")
def stream_logs(job_id):
    if job_id not in JOBS: return "unknown job", 404
    def generate():
        with _state_lock: existing = list(_live.get(job_id, []))
        for line in existing: yield f"data: {json.dumps(line)}\n\n"
        sent = len(existing)
        while True:
            with _state_lock:
                running = _locks.get(job_id, False)
                current = _live.get(job_id, [])
            for line in current[sent:]: yield f"data: {json.dumps(line)}\n\n"
            sent = len(current)
            if not running: break
            time.sleep(0.3)
        with _state_lock: current = _live.get(job_id, [])
        for line in current[sent:]: yield f"data: {json.dumps(line)}\n\n"
        yield 'data: "__DONE__"\n\n'
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/watcher/status")
def watcher_status():
    with _watch_lock:
        return jsonify({
            "enabled": _watch["enabled"], "last_scan": _watch["last_scan"],
            "pending": {_short_dir(d): {"files": e["files"], "last_seen": e["last_seen"]}
                        for d, e in _watch["pending"].items()},
            "recent": _watch["recent"],
        })

@app.route("/watcher/toggle", methods=["POST"])
def watcher_toggle():
    with _watch_lock:
        _watch["enabled"] = not _watch["enabled"]
        return jsonify({"enabled": _watch["enabled"]})

# ── Spotify routes ─────────────────────────────────────────────────────────────
@app.route("/spotify/status")
def spotify_status():
    with _sp_lock:
        return jsonify({
            "last_import":  _spotify["last_import"],
            "track_count":  len(_spotify["tracks"]),
            "playlists":    _spotify["playlists"],
            "nav_loaded":   _spotify["nav_loaded"],
            "nav_songs":    len(_nav_index) // 2,
        })

@app.route("/spotify/upload", methods=["POST"])
def spotify_upload():
    f = request.files.get("file")
    if not f: return jsonify({"error": "no file"}), 400
    try:
        content = f.read().decode("utf-8-sig")
        tracks  = parse_spotify_csv(content)
        playlists = sorted(set(t["playlist"] for t in tracks))
        with _sp_lock:
            _spotify["tracks"]      = tracks
            _spotify["playlists"]   = playlists
            _spotify["last_import"] = datetime.now().isoformat(timespec="seconds")
        return jsonify({"track_count": len(tracks), "playlist_count": len(playlists),
                        "playlists": playlists})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/spotify/load-navidrome", methods=["POST"])
def spotify_load_navidrome():
    try:
        count = load_nav_index()
        return jsonify({"songs": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/spotify/missing")
def spotify_missing():
    with _sp_lock:
        if not _spotify["tracks"]:
            return jsonify({"error": "Upload a Spotify CSV first"}), 400
    if not _nav_index:
        return jsonify({"error": "Load Navidrome library first"}), 400
    playlist = request.args.get("playlist", "")
    with _sp_lock:
        total  = len([t for t in _spotify["tracks"] if not playlist or t["playlist"] == playlist])
    missing = find_missing(playlist)
    return jsonify({"tracks": missing, "total": total, "missing": len(missing)})

@app.route("/spotify/download", methods=["POST"])
def spotify_download():
    track = request.json.get("track")
    if not track: return jsonify({"error": "no track"}), 400
    dl_id = start_download(track)
    return jsonify({"dl_id": dl_id})

@app.route("/spotify/download/<dl_id>/stream")
def spotify_download_stream(dl_id):
    def generate():
        sent = 0
        while True:
            with _dl_lock:
                if dl_id not in _downloads: break
                d       = _downloads[dl_id]
                current = list(d["output"])
                done    = d["status"] in ("done", "error")
            for line in current[sent:]: yield f"data: {json.dumps(line)}\n\n"
            sent = len(current)
            if done: break
            time.sleep(0.4)
        yield 'data: "__DONE__"\n\n'
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/spotify/downloads")
def spotify_downloads():
    with _dl_lock:
        return jsonify({"downloads": {
            k: {**v, "output": v["output"][-50:]}  # last 50 lines only
            for k, v in _downloads.items()
        }})

# ── Discover routes ────────────────────────────────────────────────────────────
@app.route("/discover")
def discover_page():
    return DISCOVER_HTML

@app.route("/discover/status")
def discover_status():
    with _disc_lock:
        return jsonify({
            "key_configured": bool(LASTFM_KEY),
            "artists":        _discover["artists"],
            "last_refresh":   _discover["last_refresh"],
            "refreshing":     _discover["refreshing"],
        })

@app.route("/discover/refresh", methods=["POST"])
def discover_refresh():
    threading.Thread(target=_refresh_discover, daemon=True).start()
    return jsonify({"status": "started"})

@app.route("/webhook/deploy", methods=["POST"])
def webhook_deploy():
    sig    = request.headers.get("X-Hub-Signature-256", "")
    secret = os.getenv("DEPLOY_SECRET", "").encode()
    if secret:
        expected = "sha256=" + hmac.new(secret, request.data, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return jsonify({"error": "invalid signature"}), 403
    def _deploy():
        time.sleep(1)
        subprocess.run("/DATA/homelab-setup/deploy.sh", shell=True)
    threading.Thread(target=_deploy, daemon=True).start()
    return jsonify({"status": "deploying"}), 200

# ── Telegram ──────────────────────────────────────────────────────────────────
def _tg_call(method, _timeout=10, **kwargs):
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = urllib.parse.urlencode(kwargs).encode()
    # getUpdates uses long-polling — one attempt only, caller loop handles retries
    max_attempts = 1 if method == "getUpdates" else 4
    delays = [5, 15, 30]
    for attempt in range(max_attempts):
        try:
            with urllib.request.urlopen(url, data, timeout=_timeout) as r:
                return json.loads(r.read())
        except Exception as e:
            is_dns = "name resolution" in str(e).lower() or "errno -3" in str(e).lower()
            is_timeout = "timed out" in str(e).lower()
            if attempt < max_attempts - 1 and (is_dns or is_timeout):
                delay = delays[min(attempt, len(delays) - 1)]
                log.warning("Telegram %s: %s — retrying in %ds (attempt %d/%d)",
                            method, e, delay, attempt + 1, max_attempts)
                time.sleep(delay)
            else:
                lvl = log.debug if is_timeout else log.warning
                lvl("Telegram %s: %s", method, e)
                break  # non-retryable error
    return {}

def _notify_tg(text, force=False):
    global _last_notify_time
    if not BOT_TOKEN or not ALLOWED_IDS: return
    if not force:
        now = time.time()
        with _notify_state_lock:
            if now < _mute_until: return
            if now - _last_notify_time < _NOTIFY_MIN_INTERVAL: return
            _last_notify_time = now
    for cid in ALLOWED_IDS:
        _tg_call("sendMessage", chat_id=cid, text=text, parse_mode="Markdown")

def _esc(s: str) -> str:
    """Escape Telegram legacy Markdown special chars in untrusted text."""
    for ch in ('_', '*', '`', '['):
        s = s.replace(ch, '\\' + ch)
    return s

def tg_send(chat_id, text, reply_markup=None):
    kwargs = dict(chat_id=chat_id, text=text, parse_mode="Markdown")
    if reply_markup is not None:
        kwargs["reply_markup"] = reply_markup
    return _tg_call("sendMessage", **kwargs)

def tg_edit(chat_id, msg_id, text, reply_markup=None):
    kwargs = dict(chat_id=chat_id, message_id=msg_id, text=text, parse_mode="Markdown")
    if reply_markup:
        kwargs["reply_markup"] = reply_markup
    _tg_call("editMessageText", **kwargs)

def _tail(lines, n=30, max_chars=3000):
    t = "".join(lines[-n:]); return t[-max_chars:]

def run_job_with_tg_stream(job_id, chat_id):
    res    = tg_send(chat_id, f"▶️ Starting *{JOBS[job_id]['name']}*…")
    msg_id = res.get("result", {}).get("message_id")
    def updater():
        while True:
            with _state_lock:
                running = _locks.get(job_id, False)
                lines   = list(_live.get(job_id, []))
            if msg_id and lines:
                tg_edit(chat_id, msg_id,
                        f"🔄 *{JOBS[job_id]['name']}* running…\n```\n{_tail(lines)}\n```")
            if not running: break
            time.sleep(5)
    t = threading.Thread(target=updater, daemon=True); t.start()
    run_job(job_id)
    t.join(timeout=2)
    with _state_lock: h = _history.get(job_id, {})
    icon = "✅" if h.get("success") else "❌"
    final = f"{icon} *{JOBS[job_id]['name']}* done in {h.get('elapsed',0)}s\n```\n{h.get('output','')[-800:]}\n```"
    if msg_id: tg_edit(chat_id, msg_id, final)
    else: tg_send(chat_id, final)


BACKUP_STEPS = [
    (1, "AppData configs"),
    (2, "PostgreSQL databases"),
    (3, "Docker volumes"),
    (4, "Dokploy config"),
    (5, "Secrets"),
    (6, "Container inventory"),
]

def _render_backup_msg(lines, done, running):
    total = len(BACKUP_STEPS)
    completed = done if (not running or done == 0) else done - 1
    filled = round((completed / total) * 12) if total else 0
    bar = "█" * filled + "░" * (12 - filled)
    pct = round((completed / total) * 100) if total else 0

    parts = ["💾 *NAS Backup*" + (" — running…" if running else " — done")]
    parts.append(f"`[{bar}]` {pct}%  ({done}/{total})")
    parts.append("")
    for num, label in BACKUP_STEPS:
        if num < done or (num == done and not running):
            parts.append(f"✅ {label}")
        elif num == done and running:
            parts.append(f"⏳ {label}…")
        else:
            parts.append(f"⬜ {label}")

    # Last few log lines for detail
    recent = [l.strip() for l in lines[-6:] if l.strip()]
    if recent:
        parts.append("")
        parts.append("```")
        parts.extend(recent)
        parts.append("```")
    return "\n".join(parts)

def _parse_backup_step(lines):
    """Return the highest step number seen in output (1-6)."""
    done = 0
    for line in lines:
        for num, _ in BACKUP_STEPS:
            if f"{num}/{len(BACKUP_STEPS)}" in line and num > done:
                done = num
    return done

def run_backup_with_tg_progress(chat_id):
    job_id = "backup"
    res    = tg_send(chat_id, _render_backup_msg([], 0, True))
    msg_id = res.get("result", {}).get("message_id")

    def updater():
        while True:
            with _state_lock:
                running = _locks.get(job_id, False)
                lines   = list(_live.get(job_id, []))
            done = _parse_backup_step(lines)
            if msg_id:
                tg_edit(chat_id, msg_id, _render_backup_msg(lines, done, running))
            if not running:
                break
            time.sleep(4)

    t = threading.Thread(target=updater, daemon=True); t.start()
    run_job(job_id)
    t.join(timeout=2)

    with _state_lock: h = _history.get(job_id, {})
    ok = h.get("success", False)
    lines = h.get("output", "").splitlines()
    size_line = next((l for l in reversed(lines) if "size:" in l.lower() or "complete" in l.lower()), "")
    final = _render_backup_msg(lines, len(BACKUP_STEPS) if ok else _parse_backup_step(lines), False)
    if ok:
        final += f"\n\n✅ *Complete* — {size_line.split(']')[-1].strip() if size_line else ''}"
    else:
        final += f"\n\n❌ *Failed* — check logs"
    if msg_id: tg_edit(chat_id, msg_id, final)
    else:       tg_send(chat_id, final)


def _run_search_download(chat_id: int, query: str):
    """Parse a free-text query and kick off a slskd download, with live status."""
    # "Artist - Track" → track mode;  "Artist - Album" resolved by _download_worker
    if " - " in query:
        artist, rest = query.split(" - ", 1)
        track = {"artist": artist.strip(), "name": rest.strip(), "album": ""}
    else:
        # Treat as album/free search: pass whole string as artist, worker uses it as query
        track = {"artist": query.strip(), "name": "", "album": ""}
    tg_send(chat_id, f"🔍 Searching slskd for `{query}`…")
    def _worker(t=track, cid=chat_id, q=query):
        dl_id = start_download(t)
        while True:
            with _dl_lock:
                s = _downloads.get(dl_id, {}).get("status", "queued")
            if s in ("done", "error"): break
            time.sleep(3)
        icon = "✅" if s == "done" else "❌"
        tg_send(cid, f"{icon} `{q}` — {'downloaded!' if s == 'done' else 'download failed'}")
    threading.Thread(target=_worker, daemon=True).start()


# ── Download Tracks feature ───────────────────────────────────────────────────

def _inline(rows):
    """Build an inline keyboard JSON string from a list of button rows."""
    return json.dumps({"inline_keyboard": rows})

def _strip_featured(artist: str) -> str:
    """Remove 'ft ...', 'feat ...', 'x ...' collab suffixes from artist string."""
    return re.split(r'\s+(?:ft\.?|feat\.?|x\b)', artist, maxsplit=1, flags=re.IGNORECASE)[0].strip()

def _title_matches(query: str, result: str) -> bool:
    """True if result title is close enough to the queried title."""
    norm = lambda s: re.sub(r"[^\w\s]", " ", s.lower())
    q, r = norm(query), norm(result)
    if q.strip() in r or r.strip() in q:
        return True
    q_words = {w for w in q.split() if len(w) > 2}
    r_words = {w for w in r.split() if len(w) > 2}
    if not q_words or not r_words:
        return False
    return len(q_words & r_words) / max(len(q_words), len(r_words)) >= 0.5

def _artist_artwork(artist: str) -> str:
    """Get any iTunes artwork for the artist (ignores track title)."""
    clean = _strip_featured(artist)
    for q in filter(None, [clean, artist]):
        try:
            url = f"https://itunes.apple.com/search?term={urllib.parse.quote(q)}&entity=song&limit=1"
            with urllib.request.urlopen(url, timeout=8) as r:
                item = (json.loads(r.read()).get("results") or [None])[0]
            if item:
                art = item.get("artworkUrl100", "").replace("100x100bb", "600x600bb")
                if art:
                    return art
        except Exception:
            pass
    return ""

_MB_UA = "nas-controller/1.0 (homelab-bot)"

def _mb_request(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": _MB_UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def _caa_artwork(release_mbid: str) -> str:
    """Follow CoverArtArchive /front redirect to get the final HTTPS image URL."""
    try:
        req = urllib.request.Request(
            f"https://coverartarchive.org/release/{release_mbid}/front",
            headers={"User-Agent": _MB_UA},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.url  # final HTTPS CDN URL after redirect
    except Exception:
        return ""

def _artist_matches(query_artist: str, result_artist: str) -> bool:
    """Check that result artist shares at least one meaningful word with the query."""
    if not query_artist:
        return True
    q_words = {w for w in query_artist.lower().split() if len(w) > 2}
    r_lower  = result_artist.lower()
    return any(w in r_lower for w in q_words)

def _mb_credits(item: dict, fallback: str) -> str:
    credits = item.get("artist-credit", [])
    return "".join(
        (c.get("artist", {}).get("name", "") + c.get("joinphrase", ""))
        for c in credits if isinstance(c, dict)
    ).strip() or fallback

def _mb_search(artist: str, title: str, kind: str) -> dict | None:
    """Query MusicBrainz and return normalized metadata dict or None."""
    try:
        if kind == "album":
            q   = f'release-group:"{title}"' + (f' AND artist:"{artist}"' if artist else "")
            url = f"https://musicbrainz.org/ws/2/release-group?query={urllib.parse.quote(q)}&fmt=json&limit=5"
            data  = _mb_request(url)
            items = data.get("release-groups", [])
            if not items:
                return None
            # Pick first result whose artist matches query
            for item in items:
                artist_name = _mb_credits(item, artist)
                if artist and not _artist_matches(artist, artist_name):
                    continue
                # Fetch a release MBID for cover art
                releases = item.get("releases", [])
                mbid     = releases[0].get("id", "") if releases else ""
                year     = (item.get("first-release-date") or "")[:4]
                tags     = item.get("tags", [])
                genre    = tags[0]["name"].title() if tags else ""
                return {
                    "title":       item.get("title", title),
                    "artist":      artist_name,
                    "album":       item.get("title", ""),
                    "year":        year,
                    "genre":       genre,
                    "artwork_url": "",
                    "mbid":        mbid,
                }
            return None
        else:
            q   = f'recording:"{title}"' + (f' AND artist:"{artist}"' if artist else "")
            url = f"https://musicbrainz.org/ws/2/recording?query={urllib.parse.quote(q)}&fmt=json&limit=5"
            data  = _mb_request(url)
            items = data.get("recordings", [])
            if not items:
                return None
            for item in items:
                artist_name = _mb_credits(item, artist)
                if artist and not _artist_matches(artist, artist_name):
                    continue
                releases = item.get("releases", [])
                # Prefer proper Album releases (not compilations/live) with a date
                def _rel_score(r):
                    rg = r.get("release-group", {})
                    pt = rg.get("primary-type", "")
                    st = rg.get("secondary-types") or []
                    if pt == "Album" and "Compilation" not in st and "Live" not in st:
                        return 0
                    if pt == "Single":
                        return 1
                    if pt == "Album":
                        return 2
                    return 3
                dated = sorted([r for r in releases if r.get("date")], key=_rel_score)
                release = dated[0] if dated else (releases[0] if releases else {})
                mbid    = release.get("id", "")
                year    = (release.get("date") or "")[:4]
                # Fallback year from release-group
                if not year and release.get("release-group"):
                    year = (release["release-group"].get("first-release-date") or "")[:4]
                tags  = item.get("tags", [])
                genre = tags[0]["name"].title() if tags else ""
                return {
                    "title":       item.get("title", title),
                    "artist":      artist_name,
                    "album":       release.get("title", ""),
                    "year":        year,
                    "genre":       genre,
                    "artwork_url": "",
                    "mbid":        mbid,
                }
            return None
    except Exception as e:
        log.warning("MusicBrainz search failed (%r): %s", title, e)
        return None

def _itunes_artwork(artist: str, title: str, kind: str) -> str:
    """iTunes artwork-only fallback."""
    entity = "album" if kind == "album" else "song"
    for raw_q in [f"{_strip_featured(artist)} {title}".strip(), title]:
        if not raw_q:
            continue
        try:
            q    = urllib.parse.quote(raw_q)
            url  = f"https://itunes.apple.com/search?term={q}&entity={entity}&limit=1"
            with urllib.request.urlopen(url, timeout=8) as r:
                data = json.loads(r.read())
            item = (data.get("results") or [None])[0]
            if item:
                return item.get("artworkUrl100", "").replace("100x100bb", "600x600bb")
        except Exception:
            pass
    return ""

def _itunes_search_strict(artist: str, title: str, kind: str) -> dict | None:
    """Query iTunes and return metadata ONLY if the returned title matches the query."""
    entity = "album" if kind == "album" else "song"
    clean  = _strip_featured(artist)
    for raw_q in [f"{clean} {title}".strip(), f"{artist} {title}".strip()]:
        if not raw_q:
            continue
        try:
            url = f"https://itunes.apple.com/search?term={urllib.parse.quote(raw_q)}&entity={entity}&limit=5"
            log.info("iTunes strict query: %r", raw_q)
            with urllib.request.urlopen(url, timeout=8) as r:
                items = json.loads(r.read()).get("results", [])
            for item in items:
                returned = item.get("trackName") or item.get("collectionName", "")
                if not _title_matches(title, returned):
                    continue
                artwork = item.get("artworkUrl100", "").replace("100x100bb", "600x600bb")
                log.info("iTunes strict hit: %r matches %r", returned, title)
                return {
                    "title":       returned,
                    "artist":      item.get("artistName", artist),
                    "album":       item.get("collectionName", ""),
                    "year":        str(item.get("releaseDate", ""))[:4],
                    "artwork_url": artwork,
                    "genre":       item.get("primaryGenreName", ""),
                }
        except Exception as e:
            log.warning("iTunes strict fetch failed for %r: %s", raw_q, e)
    return None

def _fetch_meta(artist: str, title: str, kind: str) -> dict:
    """MusicBrainz-first metadata. Never returns metadata for a different track."""
    clean = _strip_featured(artist)

    # Deduplicated MB candidate queries
    seen, candidates = set(), []
    for a, t in [(clean, title), (artist, title)]:
        key = (a.lower(), t.lower())
        if t and key not in seen:
            seen.add(key); candidates.append((a, t))
    if not artist and title and ("", title.lower()) not in seen:
        candidates.append(("", title))

    mb = None
    for a, t in candidates:
        log.info("MusicBrainz query: artist=%r title=%r kind=%s", a, t, kind)
        result = _mb_search(a, t, kind)
        if not result:
            continue
        if not _title_matches(title, result["title"]):
            log.info("MB title mismatch: searched %r, got %r — skipping", title, result["title"])
            continue
        mb = result
        log.info("MusicBrainz hit: %r by %r (mbid=%s)", mb["title"], mb["artist"], mb.get("mbid"))
        break

    if mb:
        artwork = ""
        if mb.get("mbid"):
            artwork = _caa_artwork(mb["mbid"])
            log.info("CoverArtArchive: %s", artwork or "no artwork")
        if not artwork:
            artwork = _itunes_artwork(artist, title, kind)
            log.info("iTunes artwork fallback: %s", artwork or "nothing")
        mb["artwork_url"] = artwork
        return mb

    # MusicBrainz found nothing or title mismatched — try iTunes strictly
    itunes = _itunes_search_strict(artist, title, kind)
    if itunes:
        return itunes

    # Nothing found — return artist artwork only, never wrong track metadata
    log.info("No exact match found for %r — %r, using artist artwork only", artist, title)
    artwork = _artist_artwork(artist)
    return {"title": "", "artist": artist or "", "album": "", "year": "", "genre": "",
            "artwork_url": artwork}

def _slskd_bg_continue(search_id: str, tick_start: int):
    """Background thread: wait for search completion, store full active-peer results."""
    try:
        for tick in range(tick_start, 50):
            time.sleep(1)
            info = _slskd("GET", f"/api/v0/searches/{search_id}")
            if "Completed" in info.get("state", "") or info.get("isComplete"):
                break
        resp    = _slskd("GET", f"/api/v0/searches/{search_id}/responses")
        raw     = resp if isinstance(resp, list) else resp.get("responses", [])
        folders = _peer_folders(raw, active_only=True)
    except Exception as e:
        log.warning("bg search %s error: %s", search_id, e)
        folders = []
    finally:
        try:
            _slskd("DELETE", f"/api/v0/searches/{search_id}")
        except Exception:
            pass
    with _bg_lock:
        if search_id in _bg_searches:
            _bg_searches[search_id].update({"done": True, "folders": folders})
    log.info("bg search %s done — %d active folders total", search_id, len(folders))

def _slskd_search_progressive(query: str):
    """
    Start a slskd search and return (search_id, first_5_folders) quickly.
    Breaks after collecting ≥5 active peers (min 4s), then continues in background.
    Returns (None, []) on failure.
    """
    res = _slskd("POST", "/api/v0/searches",
                 {"searchText": query, "responseLimit": 30, "fileLimit": 500})
    if "_error" in res:
        log.warning("slskd search POST failed: %s", res)
        return None, []
    search_id = res.get("id")
    if not search_id:
        log.warning("slskd search POST returned no id: %s", res)
        return None, []

    log.info("slskd search started: id=%s query=%r", search_id[:8], query)
    snap_raw = []
    for tick in range(30):
        time.sleep(1)
        info      = _slskd("GET", f"/api/v0/searches/{search_id}", timeout=4)
        completed = "Completed" in info.get("state", "") or info.get("isComplete")
        resp      = _slskd("GET", f"/api/v0/searches/{search_id}/responses", timeout=4)
        snap_raw  = resp if isinstance(resp, list) else resp.get("responses", [])
        active    = _active_peers_only(snap_raw)
        enough    = len(active) >= 5 and tick >= 4
        if tick % 5 == 0:
            log.info("slskd poll tick=%d active_peers=%d completed=%s", tick, len(active), completed)
        if completed or enough:
            if completed:
                folders = _peer_folders(snap_raw, active_only=True)
                with _bg_lock:
                    _bg_searches[search_id] = {"done": True, "folders": folders}
                try:
                    _slskd("DELETE", f"/api/v0/searches/{search_id}")
                except Exception:
                    pass
                log.info("slskd search done (completed): %d folders", len(folders))
                return search_id, folders[:5]
            break  # enough active peers — hand off to background

    # Snapshot first page, let background collect the rest
    first = _peer_folders(snap_raw, active_only=True)[:5]
    with _bg_lock:
        _bg_searches[search_id] = {"done": False, "folders": []}
    threading.Thread(
        target=_slskd_bg_continue, args=(search_id, tick + 1), daemon=True
    ).start()
    return search_id, first

_YT_JUNK = {"paroles", "parole vidéo", "parole video", "lyric video", "lyric vid",
             "lyrics video", "lyrics", "karaoke", "instrumental",
             "slowed", "reverb", "nightcore", "sped up", "speed up"}

def _ytdl_search(query: str) -> list:
    """Search YouTube via yt-dlp, return up to 5 candidates, junk filtered first."""
    import subprocess as sp
    cmd = [
        "yt-dlp", "--dump-json", "--flat-playlist", "--no-warnings",
        f"ytsearch10:{query}"
    ]
    try:
        out = sp.check_output(cmd, timeout=30, stderr=sp.DEVNULL)
        items = [json.loads(line) for line in out.decode().splitlines() if line.strip()]
    except Exception:
        return []

    def _is_junk(item):
        text = (item.get("title","") + " " + (item.get("channel") or item.get("uploader",""))).lower()
        return any(kw in text for kw in _YT_JUNK)

    def _too_long(item):
        dur = item.get("duration") or 0
        return isinstance(dur, (int, float)) and dur > 600  # >10 min = mix/compilation

    clean   = [i for i in items if not _is_junk(i) and not _too_long(i)]
    junk    = [i for i in items if _is_junk(i) and not _too_long(i)]
    ordered = clean + junk  # show clean results first, junk at end if nothing else

    results = []
    for item in ordered[:5]:
        vid_id = item.get("id", "")
        results.append({
            "video_id":  vid_id,
            "title":     item.get("title", ""),
            "channel":   item.get("channel") or item.get("uploader", ""),
            "duration":  item.get("duration_string") or str(item.get("duration", "")),
            "url":       item.get("url") or f"https://www.youtube.com/watch?v={vid_id}",
            "thumbnail": item.get("thumbnail") or (f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg" if vid_id else ""),
            "is_junk":   _is_junk(item),
        })
    return results

def _host_to_beets_path(host_path: str) -> str:
    """Convert host filesystem path to path as seen inside the beets container."""
    for host_prefix, container_prefix in [
        ("/media/sdb/Musics", "/music"),
        ("/media/sdb/Evyy Musics", "/evymusics"),
    ]:
        if host_path.startswith(host_prefix):
            return container_prefix + host_path[len(host_prefix):]
    return host_path

def _inject_tags(flac_path: str, meta: dict):
    """Overwrite FLAC tags with clean iTunes metadata using the beets container."""
    artist = meta.get("artist", "")
    album  = meta.get("album", "")
    title  = meta.get("title", "")
    year   = meta.get("year", "")
    genre  = meta.get("genre", "")
    if not (artist and title):
        return  # need at least artist + title
    container_path = _host_to_beets_path(flac_path)
    script = (
        f"import mutagen.flac\n"
        f"f = mutagen.flac.FLAC({container_path!r})\n"
        f"f['albumartist'] = [{artist!r}]\n"
        f"f['artist']      = [{artist!r}]\n"
        f"f['album']       = [{album!r}]\n"
        f"f['title']       = [{title!r}]\n"
        f"f['date']        = [{year!r}]\n"
        f"f['genre']       = [{genre!r}]\n"
        f"f.save()\n"
        f"print('Tags injected: {artist} / {album} / {title}')\n"
    )
    import subprocess as sp
    r = sp.run(["docker", "exec", "beets", "python3", "-c", script],
               capture_output=True, text=True, timeout=30)
    return r.stdout.strip()

def _ytdl_download_worker(dl_id: str, video_id: str, artist: str, title: str,
                          meta: dict = None):
    """Download a YouTube track as FLAC into the Soulseek downloads dir."""
    out_dir = os.path.join(DOWNLOADS_DIR, f"{artist} - {title}")
    os.makedirs(out_dir, exist_ok=True)
    cmd = [
        "yt-dlp",
        "--extract-audio", "--audio-format", "flac", "--audio-quality", "0",
        "--embed-metadata", "--add-metadata",
        "--embed-thumbnail",   # embed YT thumbnail as fallback cover art
        "--output", os.path.join(out_dir, "%(title)s.%(ext)s"),
        "--no-warnings",
        "--newline",
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    with _dl_lock:
        _downloads[dl_id].update({"status": "downloading",
                                   "started": datetime.now().isoformat(timespec="seconds")})
    try:
        import subprocess as sp
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        for line in proc.stdout:
            stripped = line.rstrip()
            _dl_log(dl_id, stripped)
            m = _YTDL_PROGRESS_RE.search(stripped)
            if m:
                pct_v, sz_v, sz_u, spd_v, spd_u, eta_s = m.groups()
                pct_f  = float(pct_v)
                tot    = _parse_ytdl_size(sz_v, sz_u)
                spd    = _parse_ytdl_speed(spd_v, spd_u)
                eta    = _parse_ytdl_eta(eta_s)
                with _dl_lock:
                    _downloads[dl_id]["progress"] = {
                        "pct": pct_f,
                        "downloaded": int(pct_f / 100 * tot),
                        "total": tot,
                        "speed_bps": spd,
                        "eta_secs": eta,
                        "stage": "downloading",
                    }
        proc.wait()
        ok = proc.returncode == 0
        if ok and meta:
            flac_files = [
                os.path.join(out_dir, f) for f in os.listdir(out_dir)
                if f.lower().endswith(".flac")
            ]
            if flac_files:
                _dl_log(dl_id, "Injecting metadata tags…")
                injected = False
                for fpath in flac_files:
                    result = _inject_tags(fpath, meta)
                    if result:
                        _dl_log(dl_id, result)
                        injected = True
                    else:
                        _dl_log(dl_id, "Tag injection skipped (missing artist/title)")
                if injected:
                    # Force beet re-import with clean tags (bypass incremental,
                    # in case watcher already ran 'asis' before inject completed)
                    beets_dir = _beets_path(out_dir)
                    with _dl_lock:
                        _downloads[dl_id]["progress"] = {"pct": 100, "stage": "importing"}
                    _dl_log(dl_id, f"Re-importing via beets: {beets_dir}")
                    r_imp = sp.run(
                        f'docker exec beets beet import --noincremental -q "{beets_dir}"',
                        shell=True, capture_output=True, text=True, timeout=300)
                    for line in (r_imp.stdout + r_imp.stderr).splitlines():
                        if line.strip():
                            _dl_log(dl_id, f"[beets] {line.strip()}")
                            log.info("[beets:yt-import] %s", line.strip())
                    # Refresh cover art for newly imported item
                    with _dl_lock:
                        _downloads[dl_id]["progress"] = {"pct": 100, "stage": "art"}
                    _dl_log(dl_id, "Fetching cover art…")
                    r_art = sp.run(
                        "docker exec beets beet fetchart ; docker exec beets beet embedart -y",
                        shell=True, capture_output=True, text=True, timeout=300)
                    for line in (r_art.stdout + r_art.stderr).splitlines():
                        if line.strip():
                            _dl_log(dl_id, f"[beets] {line.strip()}")
                            log.info("[beets:yt-art] %s", line.strip())
        with _dl_lock:
            _downloads[dl_id].update({
                "status": "done" if ok else "error",
                "finished": datetime.now().isoformat(timespec="seconds"),
            })
        _dl_log(dl_id, "Done!" if ok else f"yt-dlp exited {proc.returncode}")
    except Exception as e:
        _dl_log(dl_id, f"ERROR: {e}")
        with _dl_lock:
            _downloads[dl_id].update({"status": "error",
                                       "finished": datetime.now().isoformat(timespec="seconds")})

def _sess_get(chat_id: int) -> dict:
    with _sess_lock:
        return _dl_sessions.setdefault(chat_id, {})

def _sess_set(chat_id: int, data: dict):
    with _sess_lock:
        _dl_sessions[chat_id] = data

def _result_text(session: dict, idx: int) -> str:
    results = session.get("results", [])
    total   = len(results)
    if not results or idx >= total:
        return "No results."
    r    = results[idx]
    meta = session.get("meta", {})

    lines = []
    if meta.get("title"):
        lines.append(f"*{_esc(meta['title'])}*")
    if meta.get("artist"):
        lines.append(f"🎤 {_esc(meta['artist'])}")
    if meta.get("album"):
        lines.append(f"💿 {_esc(meta['album'])}" + (f" ({meta['year']})" if meta.get('year') else ""))
    if meta.get("genre"):
        lines.append(f"🎸 {_esc(meta['genre'])}")
    lines.append("")

    src = r.get("source", "slskd")
    if src == "slskd":
        flac   = r.get("flac_count", 0)
        nfiles = len(r.get("files", []))
        size   = r.get("total_size", 0) // (1024 * 1024)
        folder = r.get("folder", "").rsplit("\\", 1)[-1]
        lines.append(f"📁 `{folder}`")
        lines.append(f"👤 {_esc(r['username'])}  •  {nfiles} file(s)  •  {size} MB" +
                     (f"  •  {flac} FLAC" if flac else ""))
        lines.append("🟢 Soulseek")
    else:
        junk_warn = "  ⚠️ _lyrics/cover_" if r.get("is_junk") else ""
        lines.append(f"🎬 {_esc(r.get('title',''))}{junk_warn}")
        lines.append(f"📺 {_esc(r.get('channel',''))}" +
                     (f"  •  {r.get('duration','')}" if r.get('duration') else ""))
        lines.append("🔴 YouTube")

    footer = f"\n_{idx + 1}/{total}_"
    if idx == total - 1 and session.get("bg_search_id"):
        with _bg_lock:
            bg = _bg_searches.get(session["bg_search_id"], {})
        if not bg.get("done"):
            footer += " · ⏳ more loading…"
    lines.append(footer)
    return "\n".join(lines)

def _result_keyboard(idx: int, total: int, source: str = "slskd",
                     bg_search_id: str = None) -> str:
    nav_row = []
    if idx > 0:
        nav_row.append({"text": "← Prev", "callback_data": "dl:prev"})
    nav_row.append({"text": f"{idx+1}/{total}", "callback_data": "dl:noop"})
    # Show Next on the last result when background search is active/done with more
    show_next = idx < total - 1
    if not show_next and bg_search_id:
        with _bg_lock:
            bg = _bg_searches.get(bg_search_id, {})
        # Show Next if bg is still running OR if bg is done with more results than shown
        if not bg.get("done") or len(bg.get("folders", [])) > total:
            show_next = True
    if show_next:
        nav_row.append({"text": "Next →", "callback_data": "dl:next"})
    action_row = [
        {"text": "⬇️ Download", "callback_data": "dl:download"},
        {"text": "❌ Cancel",   "callback_data": "dl:cancel"},
    ]
    rows = [nav_row, action_row] if nav_row else [action_row]
    if source == "slskd":
        rows.append([{"text": "🔴 Search on YouTube", "callback_data": "dl:retry_yt"}])
    return json.dumps({"inline_keyboard": rows})

def _show_result(chat_id: int, session: dict):
    idx      = session.get("idx", 0)
    results  = session.get("results", [])
    text     = _result_text(session, idx)
    total    = len(results)
    source        = results[idx].get("source", "slskd") if results else "slskd"
    bg_search_id  = session.get("bg_search_id")
    kb            = _result_keyboard(idx, total, source, bg_search_id=bg_search_id)
    meta     = session.get("meta", {})
    msg_id, msg_type = _result_msgs.get(chat_id, (None, "text"))
    # Artwork: meta first, then YouTube thumbnail as fallback for yt results
    artwork  = meta.get("artwork_url", "")
    if not artwork and source == "yt" and results:
        artwork = results[idx].get("thumbnail", "")

    def _save_msg(new_id: int, mtype: str):
        _result_msgs[chat_id] = (new_id, mtype)

    if msg_id:
        # Edit the existing message using the correct method for its type
        if msg_type == "photo":
            r = _tg_call("editMessageCaption", chat_id=chat_id, message_id=msg_id,
                         caption=text, parse_mode="Markdown", reply_markup=kb)
        else:
            r = _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                         text=text, parse_mode="Markdown", reply_markup=kb)
        if r.get("ok"):
            return
        # stale message_id — clear and fall through to send fresh
        _result_msgs.pop(chat_id, None)
        msg_id = None

    # No existing message — send fresh
    if artwork:
        log.info("Sending artwork: %s", artwork)
        r      = _tg_call("sendPhoto", chat_id=chat_id, photo=artwork,
                          caption=text, parse_mode="Markdown", reply_markup=kb)
        new_id = r.get("result", {}).get("message_id")
        if new_id:
            _save_msg(new_id, "photo")
            return
        log.warning("sendPhoto failed, falling back to sendMessage")

    r      = tg_send(chat_id, text, reply_markup=kb)
    new_id = r.get("result", {}).get("message_id")
    if new_id:
        _save_msg(new_id, "text")

def _progress_text(dl_id: str) -> str:
    with _dl_lock:
        d = _downloads.get(dl_id, {})
    status   = d.get("status", "queued")
    progress = d.get("progress")
    track    = d.get("track", {})
    lines    = d.get("output", [])
    icons = {"queued": "⏳", "searching": "🔍", "downloading": "⬇️",
             "importing": "📥", "lyrics": "🎵", "art": "🎨",
             "done": "✅", "error": "❌", "paused": "⏸", "stalled": "🕐"}
    icon   = icons.get(status, "⏳")
    artist = track.get("artist", "")
    title  = track.get("title") or track.get("name", "")
    label  = f"*{artist}* — {title}" if (artist or title) else "*Download*"
    text   = f"{icon} {label}\n"

    if progress and status not in ("done", "error", "searching", "queued"):
        pct        = progress.get("pct", 0)
        stage      = progress.get("stage", "downloading")
        speed_bps  = progress.get("speed_bps", 0)
        downloaded = progress.get("downloaded", 0)
        total      = progress.get("total")
        eta_secs   = progress.get("eta_secs")
        n_done     = progress.get("n_done")
        total_files= progress.get("total_files")
        stage_map  = {"importing": "📥 Importing…", "art": "🎨 Fetching art…", "lyrics": "🎵 Fetching lyrics…", "done": "✅ Library updated"}
        if stage in stage_map:
            text += f"{stage_map[stage]}\n"
            text += f"`{'▓' * 18}` 100%\n"
        else:
            text += f"`{_bar(pct)}` {pct:.0f}%\n"
            parts = []
            if total:
                parts.append(f"{_fmt_size(downloaded)} / {_fmt_size(total)}")
            if speed_bps:
                parts.append(f"⚡ {_fmt_speed(speed_bps)}")
            if eta_secs:
                parts.append(f"⏱ {_fmt_eta(eta_secs)}")
            if parts:
                text += "`" + "  ".join(parts) + "`\n"
            if n_done is not None and total_files:
                text += f"Files: {n_done}/{total_files}\n"
    elif status == "done":
        text += f"`{'▓' * 18}` 100%\n"
        summary = (progress or {}).get("summary")
        if summary:
            text += f"`{summary}`\n"
        else:
            last = [l for l in lines[-2:] if l.strip()]
            if last:
                text += "```\n" + "\n".join(last) + "\n```"
    elif status == "error":
        last = [l for l in lines[-2:] if l.strip()]
        if last:
            text += "```\n" + "\n".join(last) + "\n```"
    elif status == "searching":
        last = [l for l in lines[-2:] if l.strip()]
        if last:
            text += "```\n" + "\n".join(last) + "\n```"
    else:
        last = [l for l in lines[-4:] if l.strip()]
        if last:
            text += "```\n" + "\n".join(last) + "\n```"
    return text

def _progress_keyboard(dl_id: str) -> str:
    with _dl_lock:
        status = _downloads.get(dl_id, {}).get("status", "queued")
    rows = []
    if status in ("downloading", "searching", "queued"):
        rows.append([
            {"text": "⏸ Pause",    "callback_data": f"dl:pause:{dl_id}"},
            {"text": "❌ Cancel",  "callback_data": f"dl:abort:{dl_id}"},
        ])
    elif status in ("importing", "lyrics", "art"):
        rows.append([{"text": "❌ Cancel", "callback_data": f"dl:abort:{dl_id}"}])
    elif status == "paused":
        rows.append([
            {"text": "▶️ Resume",  "callback_data": f"dl:resume:{dl_id}"},
            {"text": "❌ Cancel",  "callback_data": f"dl:abort:{dl_id}"},
        ])
    elif status == "error":
        rows.append([
            {"text": "🔁 Retry Soulseek", "callback_data": "dl:retry_slskd"},
            {"text": "▶️ YouTube",         "callback_data": "dl:retry_yt"},
        ])
    rows.append([{"text": "🗑 Clear", "callback_data": f"dl:clear:{dl_id}"}])
    return json.dumps({"inline_keyboard": rows})

def _monitor_download(chat_id: int, dl_id: str, msg_id: int):
    """Background thread: edit progress message every 2s until terminal state."""
    terminal        = {"done", "error"}
    last_text       = ""
    last_kb         = ""
    importing_since = None
    while True:
        time.sleep(2)
        text = _progress_text(dl_id)
        kb   = _progress_keyboard(dl_id)
        if text != last_text or kb != last_kb:
            _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                     text=text, parse_mode="Markdown", reply_markup=kb)
            last_text = text
            last_kb   = kb
        with _dl_lock:
            status = _downloads.get(dl_id, {}).get("status", "error")
        # Safety timeout: if watcher never links back, force done after 15 min
        if status in ("importing", "lyrics", "art"):
            if importing_since is None:
                importing_since = time.time()
            elif time.time() - importing_since > 900:
                with _dl_lock:
                    _downloads[dl_id]["status"] = "done"
                    if not (_downloads[dl_id].get("progress") or {}).get("summary"):
                        _downloads[dl_id]["progress"] = {"pct": 100, "stage": "done",
                                                          "summary": "✅ Pipeline completed"}
                status = "done"
        else:
            importing_since = None
        if status in terminal:
            final_text = _progress_text(dl_id)
            final_kb   = _progress_keyboard(dl_id)
            if final_text != last_text or final_kb != last_kb:
                _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                         text=final_text, parse_mode="Markdown", reply_markup=final_kb)
            break

def _slskd_download_chosen(dl_id: str, username: str, files_to_dl: list,
                            chat_id: int = 0, kind: str = "track"):
    """Queue already-chosen files from slskd and monitor (no search step)."""
    # Stall timeout: 5 min for a single track, 15 min for an album/folder
    stall_limit = 300 if kind == "track" else 900

    with _dl_lock:
        _downloads[dl_id].update({"status": "downloading",
                                   "started": datetime.now().isoformat(timespec="seconds")})
    payload = [{"filename": f["filename"], "size": f.get("size", 0)} for f in files_to_dl]
    dl_res  = _slskd("POST", f"/api/v0/transfers/downloads/{username}", payload)
    if isinstance(dl_res, dict) and "_error" in dl_res:
        _dl_log(dl_id, f"ERROR: Queue failed: {dl_res}")
        with _dl_lock:
            _downloads[dl_id].update({"status": "error",
                                       "finished": datetime.now().isoformat(timespec="seconds")})
        return

    queued_filenames = {f["filename"] for f in files_to_dl}
    total            = len(queued_filenames)
    ever_seen        = False
    confirmed_done: set = set()
    confirmed_fail: set = set()
    queued_since     = time.time()
    stall_notif_id: int = 0  # message_id of the current stall notification

    for _ in range(1800):
        time.sleep(2)
        with _dl_lock:
            st = _downloads[dl_id].get("status")
            if st == "paused":
                time.sleep(3); continue
            # "Keep waiting" button pressed — reset stall clock
            if _downloads[dl_id].pop("stall_reset", False):
                queued_since = time.time()
                stall_notif_id = 0
                _downloads[dl_id]["status"] = "downloading"

        transfers = _slskd("GET", "/api/v0/transfers/downloads")
        if isinstance(transfers, dict) and "_error" in transfers:
            continue

        states: dict = {}
        for peer_group in (transfers if isinstance(transfers, list) else []):
            if peer_group.get("username") != username:
                continue
            for d in peer_group.get("directories", []):
                for f in d.get("files", []):
                    if f.get("filename") in queued_filenames:
                        states[f["filename"]] = f

        if states:
            ever_seen = True
            for fname, finfo in states.items():
                s = finfo.get("state", "")
                if "Completed" in s and "Succeeded" in s:
                    confirmed_done.add(fname)
                elif "Completed" in s:
                    confirmed_fail.add(fname)

            # Reset stall clock once transfer actually starts moving
            active = [f for f in states.values()
                      if f.get("percentComplete", 0) > 0 or "InProgress" in f.get("state","")]
            if active:
                queued_since = time.time()  # no longer stalled

        elif not ever_seen:
            # Files not visible yet — check stall timeout
            if time.time() - queued_since > stall_limit and not stall_notif_id:
                waited = int(time.time() - queued_since)
                _dl_log(dl_id, f"Stalled — no transfer started after {waited}s")
                with _dl_lock:
                    _downloads[dl_id]["status"] = "stalled"
                if chat_id:
                    mins = stall_limit // 60
                    kb   = json.dumps({"inline_keyboard": [[
                        {"text": "▶️ Try YouTube instead", "callback_data": f"dl:stall_yt:{dl_id}"},
                        {"text": "⏳ Keep waiting",        "callback_data": f"dl:keep_waiting:{dl_id}"},
                        {"text": "❌ Cancel",              "callback_data": f"dl:cancel"},
                    ]]})
                    res = tg_send(chat_id,
                                  f"🕐 *Soulseek is not responding*\n"
                                  f"No transfer started after {mins} min.\n"
                                  f"Peer `{username}` may be offline or busy.",
                                  reply_markup=kb)
                    stall_notif_id = res.get("result", {}).get("message_id", 0)
                # Keep looping — user may press "Keep waiting"
                continue
            break

        cleared = queued_filenames - set(states.keys()) - confirmed_done - confirmed_fail
        n_done  = len(confirmed_done) + len(cleared)
        n_error = len(confirmed_fail)
        avg_pct = (sum(f.get("percentComplete", 0) for f in states.values()) / len(states)
                   if states else 100)
        tot_bytes  = sum(int(f.get("size", 0)) for f in states.values())
        done_bytes = sum(int(f.get("bytesTransferred", 0)) for f in states.values())
        avg_speed  = sum(float(f.get("averageSpeed", 0)) for f in states.values())
        eta_s = int((tot_bytes - done_bytes) / avg_speed) if avg_speed > 1 else None
        with _dl_lock:
            _downloads[dl_id]["progress"] = {
                "pct": avg_pct,
                "downloaded": done_bytes,
                "total": tot_bytes or None,
                "speed_bps": avg_speed,
                "eta_secs": eta_s,
                "stage": "downloading",
                "n_done": n_done,
                "total_files": total,
            }
        _dl_log(dl_id, f"{n_done}/{total} done, {n_error} errors — {avg_pct:.0f}% avg")

        if n_done + n_error >= total or (not states and ever_seen):
            ok = n_error == 0
            if ok:
                with _dl_lock:
                    _downloads[dl_id].update({
                        "status": "importing",
                        "progress": {"pct": 100, "stage": "importing"},
                    })
                    track = _downloads[dl_id].get("track", {})
                    artist = track.get("artist", "")
                    title  = track.get("title", "")
                    key = f"{artist} - {title}".lower().strip()
                    if key:
                        _pending_pipeline[key] = dl_id
                _dl_log(dl_id, "Download complete — waiting for beets import…")
            else:
                with _dl_lock:
                    _downloads[dl_id].update({
                        "status": "error",
                        "finished": datetime.now().isoformat(timespec="seconds"),
                    })
                _dl_log(dl_id, f"{n_error}/{total} files failed")
            return

    _dl_log(dl_id, "ERROR: Timed out")
    with _dl_lock:
        _downloads[dl_id].update({"status": "error",
                                   "finished": datetime.now().isoformat(timespec="seconds")})

def _start_dl(chat_id: int, session: dict):
    """Kick off download from the currently selected result."""
    idx  = session.get("idx", 0)
    r    = session.get("results", [])[idx]
    kind = session.get("kind", "track")
    meta = session.get("meta", {})

    dl_id = str(uuid.uuid4())[:8]
    label = meta.get("title") or session.get("query", "?")
    with _dl_lock:
        _downloads[dl_id] = {"track": {"artist": session.get("artist",""),
                                        "name": session.get("query",""),
                                        "title": meta.get("title") or session.get("query",""),
                                        "album": ""},
                              "status": "queued", "output": [],
                              "started": None, "finished": None}

    session["dl_id"] = dl_id
    _sess_set(chat_id, session)

    # Send progress message
    res    = tg_send(chat_id, _progress_text(dl_id), reply_markup=_progress_keyboard(dl_id))
    msg_id = res.get("result", {}).get("message_id")

    if r.get("source") == "yt":
        threading.Thread(target=_ytdl_download_worker,
                         args=(dl_id, r["video_id"],
                               session.get("artist", ""), label),
                         kwargs={"meta": meta},
                         daemon=True).start()
    else:
        username    = r["username"]
        files_to_dl = r.get("files", [])
        if kind == "track":
            q_name = session.get("query", "")
            files_to_dl = [
                next((f for f in files_to_dl
                      if _norm(q_name) in _norm(f["filename"].rsplit("\\", 1)[-1].rsplit(".", 1)[0])),
                     files_to_dl[0])
            ] if files_to_dl else []
            files_to_dl = [f for f in files_to_dl
                           if "." + f["filename"].rsplit(".", 1)[-1].lower() in AUDIO_DL_EXTS]
        threading.Thread(target=_slskd_download_chosen,
                         args=(dl_id, username, files_to_dl),
                         kwargs={"chat_id": chat_id, "kind": kind},
                         daemon=True).start()

    if msg_id:
        threading.Thread(target=_monitor_download,
                         args=(chat_id, dl_id, msg_id), daemon=True).start()


def handle_download_start(chat_id: int):
    _sess_set(chat_id, {"step": "ask_kind"})
    kb = _inline([
        [{"text": "🎵 Track",  "callback_data": "dl:kind:track"},
         {"text": "💿 Album",  "callback_data": "dl:kind:album"}],
        [{"text": "❌ Cancel", "callback_data": "dl:cancel"}],
    ])
    tg_send(chat_id, "What do you want to download?", reply_markup=kb)


_SEARCH_CANCEL_KB = json.dumps({"inline_keyboard": [[
    {"text": "❌ Cancel", "callback_data": "dl:cancel"},
]]})

def _do_slskd_search(chat_id: int, session: dict, msg_id: int):
    kind  = session.get("kind", "track")
    query = session.get("query", "")
    artist= session.get("artist", "")
    q     = f"{artist} {query}".strip() if artist else query

    # Fetch metadata in parallel with the slskd search
    meta_box = [{}]
    def _meta_worker():
        meta_box[0] = _fetch_meta(artist, query, kind)
    meta_thread = threading.Thread(target=_meta_worker, daemon=True)
    meta_thread.start()

    bg_search_id, folders = _slskd_search_progressive(q)

    # Wait for metadata (should already be done; cap at 5s extra)
    meta_thread.join(timeout=5)
    meta = meta_box[0]

    # Check if user cancelled while we were searching
    if _sess_get(chat_id).get("step") != "searching":
        if bg_search_id:
            with _bg_lock:
                _bg_searches.pop(bg_search_id, None)
        return

    if not folders:
        # Clean up bg entry if it was registered
        if bg_search_id:
            with _bg_lock:
                _bg_searches.pop(bg_search_id, None)
        session["step"] = "no_results"
        _sess_set(chat_id, session)
        _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                 text=f"😕 No results on Soulseek for *{_esc(q)}*", parse_mode="Markdown",
                 reply_markup=json.dumps({"inline_keyboard": [[
                     {"text": "🔁 Retry",              "callback_data": "dl:retry_slskd"},
                     {"text": "▶️ Retry with YouTube", "callback_data": "dl:retry_yt"},
                 ],[
                     {"text": "❌ Cancel",             "callback_data": "dl:cancel"},
                 ]]}))
        return

    for f in folders:
        f["source"] = "slskd"

    session.update({"step": "results", "results": folders, "idx": 0, "meta": meta,
                    "bg_search_id": bg_search_id})
    _sess_set(chat_id, session)

    _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
    _show_result(chat_id, session)


def _do_yt_search(chat_id: int, session: dict, msg_id: int):
    kind  = session.get("kind", "track")
    query = session.get("query", "")
    artist= session.get("artist", "")
    q     = f"{artist} {query}".strip() if artist else query

    items = _ytdl_search(q)

    # Check if user cancelled while we were searching
    if _sess_get(chat_id).get("step") != "searching":
        return

    if not items:
        kb = json.dumps({"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "dl:cancel"}]]})
        _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                 text=f"😕 No YouTube results for *{_esc(q)}*",
                 parse_mode="Markdown", reply_markup=kb)
        return

    # Build result objects compatible with _result_text
    yt_results = [dict(r, source="yt") for r in items]
    meta = _fetch_meta(artist, query, kind)

    # Check again after slow metadata fetch
    if _sess_get(chat_id).get("step") != "searching":
        return

    session.update({"step": "results", "results": yt_results,
                    "idx": 0, "meta": meta})
    _sess_set(chat_id, session)

    _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
    _show_result(chat_id, session)


def handle_callback(cq: dict):
    """Handle all dl:* inline keyboard callbacks."""
    cq_id   = cq.get("id", "")
    data    = cq.get("data", "")
    chat_id = cq.get("message", {}).get("chat", {}).get("id")
    if not chat_id:
        return
    log.info("Callback: %s from %s", data, chat_id)
    _tg_call("answerCallbackQuery", callback_query_id=cq_id)

    if data == "stats:refresh":
        msg_id = cq.get("message", {}).get("message_id")
        threading.Thread(target=_send_nas_stats, args=(chat_id, msg_id), daemon=True).start()
        return

    if data == "services:refresh":
        msg_id = cq.get("message", {}).get("message_id")
        threading.Thread(target=_send_services_status, args=(chat_id, msg_id), daemon=True).start()
        return

    if data == "maint:confirm":
        global _maint_running
        msg_id = cq.get("message", {}).get("message_id")
        with _maint_state_lock:
            if _maint_running:
                _tg_call("answerCallbackQuery", callback_query_id=cq_id,
                         text="⚠️ Already running!", show_alert=True)
                return
            _maint_running = True
        log.info("[maint] Starting manual library sync for chat %s msg %s", chat_id, msg_id)
        threading.Thread(target=_run_manual_maintenance, args=(chat_id, msg_id), daemon=True).start()
        return

    if data == "maint:cancel":
        msg_id = cq.get("message", {}).get("message_id")
        if msg_id:
            _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
        tg_send(chat_id, "Cancelled.", reply_markup=TG_KEYBOARD)
        return

    # ── NAS Control ───────────────────────────────────────────────────────────
    if data.startswith("nas:"):
        msg_id = cq.get("message", {}).get("message_id")
        action_key = data[4:]  # strip "nas:"

        if action_key == "cancel":
            if msg_id:
                _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
            return

        if action_key in _NAS_ACTIONS:
            # First tap: show confirmation
            action = _NAS_ACTIONS[action_key]
            kb = _inline([
                [{"text": "✅ Confirm", "callback_data": f"nas:do:{action_key}"},
                 {"text": "❌ Cancel",  "callback_data": "nas:cancel"}],
            ])
            if msg_id:
                _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                         text=action["confirm_text"], parse_mode="Markdown",
                         reply_markup=kb)
            return

        if action_key.startswith("do:"):
            key = action_key[3:]
            if key not in _NAS_ACTIONS:
                return
            threading.Thread(target=_handle_nas_action,
                             args=(chat_id, msg_id, key), daemon=True).start()
            return

    session = _sess_get(chat_id)
    parts   = data.split(":")

    if data == "dl:noop":
        return

    # ── Kind selection ────────────────────────────────────────────────────────
    if len(parts) == 3 and parts[0] == "dl" and parts[1] == "kind":
        kind = parts[2]
        session.update({"kind": kind, "step": "ask_query"})
        _sess_set(chat_id, session)
        label = "album" if kind == "album" else "track"
        # Edit the kind-selection message to remove buttons
        msg_id = cq.get("message", {}).get("message_id")
        if msg_id:
            _tg_call("editMessageReplyMarkup", chat_id=chat_id, message_id=msg_id,
                     reply_markup=json.dumps({"inline_keyboard": []}))
        tg_send(chat_id, f"Enter *artist — {label} title*:", reply_markup=_SEARCH_CANCEL_KB)
        return

    # ── Pagination ────────────────────────────────────────────────────────────
    if data == "dl:prev":
        session["idx"] = max(0, session.get("idx", 0) - 1)
        _sess_set(chat_id, session)
        _show_result(chat_id, session)
        return

    if data == "dl:next":
        results      = session.get("results", [])
        total        = len(results)
        idx          = session.get("idx", 0)
        bg_search_id = session.get("bg_search_id")

        if idx >= total - 1 and bg_search_id:
            # At last result — try to inject more from background
            with _bg_lock:
                bg = _bg_searches.get(bg_search_id, {})
                bg_done    = bg.get("done", False)
                bg_folders = bg.get("folders", [])

            if not bg_done:
                _tg_call("answerCallbackQuery", callback_query_id=cq_id,
                         text="⏳ Still searching, try again shortly…", show_alert=False)
                return

            # Background done — append new results (skip duplicates by username+folder)
            existing_keys = {(r["username"], r["folder"]) for r in results if r.get("source") == "slskd"}
            new_folders   = [
                dict(f, source="slskd") for f in bg_folders
                if (f["username"], f["folder"]) not in existing_keys
            ]
            if new_folders:
                results.extend(new_folders)
                session["results"] = results
                # Clean up bg state
                with _bg_lock:
                    _bg_searches.pop(bg_search_id, None)
                session.pop("bg_search_id", None)
                session["idx"] = idx + 1
            else:
                # No new results despite bg finishing
                with _bg_lock:
                    _bg_searches.pop(bg_search_id, None)
                session.pop("bg_search_id", None)
                _tg_call("answerCallbackQuery", callback_query_id=cq_id,
                         text="No more results found.", show_alert=False)
                _sess_set(chat_id, session)
                _show_result(chat_id, session)
                return
        else:
            session["idx"] = min(total - 1, idx + 1)

        _sess_set(chat_id, session)
        _show_result(chat_id, session)
        return

    # ── Download ──────────────────────────────────────────────────────────────
    if data == "dl:download":
        results = session.get("results", [])
        if not results:
            return
        # Remove result keyboard
        msg_id = _result_msgs.get(chat_id, (None,))[0]
        if msg_id:
            _tg_call("editMessageReplyMarkup", chat_id=chat_id, message_id=msg_id,
                     reply_markup=json.dumps({"inline_keyboard": []}))
        _start_dl(chat_id, session)
        return

    # ── Cancel ────────────────────────────────────────────────────────────────
    if data == "dl:cancel":
        msg_id = cq.get("message", {}).get("message_id")
        if msg_id:
            _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
        bg_id = session.get("bg_search_id")
        if bg_id:
            with _bg_lock:
                _bg_searches.pop(bg_id, None)
        _sess_set(chat_id, {})
        _result_msgs.pop(chat_id, None)
        tg_send(chat_id, "Cancelled.", reply_markup=TG_KEYBOARD)
        return

    # ── Retry Soulseek ────────────────────────────────────────────────────────
    if data == "dl:retry_slskd":
        old_msg_id = cq.get("message", {}).get("message_id")
        q = (session.get("artist", "") + " " + session.get("query", "")).strip()
        if old_msg_id:
            _tg_call("deleteMessage", chat_id=chat_id, message_id=old_msg_id)
        bg_id = session.pop("bg_search_id", None)
        if bg_id:
            with _bg_lock:
                _bg_searches.pop(bg_id, None)
        _result_msgs.pop(chat_id, None)
        session["step"] = "searching"
        _sess_set(chat_id, session)
        res    = tg_send(chat_id, f"🔍 Searching Soulseek for *{_esc(q)}*…",
                         reply_markup=_SEARCH_CANCEL_KB)
        new_id = res.get("result", {}).get("message_id")
        threading.Thread(target=_do_slskd_search,
                         args=(chat_id, session, new_id), daemon=True).start()
        return

    # ── Retry YouTube ─────────────────────────────────────────────────────────
    if data == "dl:retry_yt":
        old_msg_id = cq.get("message", {}).get("message_id")
        q = (session.get("artist", "") + " " + session.get("query", "")).strip()
        if old_msg_id:
            _tg_call("deleteMessage", chat_id=chat_id, message_id=old_msg_id)
        bg_id = session.pop("bg_search_id", None)
        if bg_id:
            with _bg_lock:
                _bg_searches.pop(bg_id, None)
        _result_msgs.pop(chat_id, None)
        session["step"] = "searching"
        _sess_set(chat_id, session)
        res    = tg_send(chat_id, f"🔍 Searching YouTube for *{_esc(q)}*…",
                         reply_markup=_SEARCH_CANCEL_KB)
        new_id = res.get("result", {}).get("message_id")
        threading.Thread(target=_do_yt_search,
                         args=(chat_id, session, new_id), daemon=True).start()
        return

    # ── Stall → YouTube fallback ─────────────────────────────────────────────
    if len(parts) == 3 and parts[0] == "dl" and parts[1] == "stall_yt":
        msg_id = cq.get("message", {}).get("message_id")
        q = (session.get("artist", "") + " " + session.get("query", "")).strip()
        # Delete the stall notification message
        if msg_id:
            _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
        # Send fresh searching message and kick off YouTube search
        res    = tg_send(chat_id, f"🔍 Searching YouTube for *{q}*…",
                         reply_markup=_SEARCH_CANCEL_KB)
        new_id = res.get("result", {}).get("message_id")
        session["step"] = "searching"
        _sess_set(chat_id, session)
        threading.Thread(target=_do_yt_search,
                         args=(chat_id, session, new_id), daemon=True).start()
        return

    # ── Stall → Keep waiting (reset timer) ───────────────────────────────────
    if len(parts) == 3 and parts[0] == "dl" and parts[1] == "keep_waiting":
        dl_id  = parts[2]
        msg_id = cq.get("message", {}).get("message_id")
        with _dl_lock:
            if dl_id not in _downloads:
                if msg_id: _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
                tg_send(chat_id, "⚠️ Download no longer tracked (bot was restarted). Tap *⬇️ Download Tracks* to start a new search.", reply_markup=TG_KEYBOARD)
                return
            _downloads[dl_id]["stall_reset"] = True
        if msg_id:
            _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
        tg_send(chat_id, "⏳ OK, still waiting for Soulseek… I'll let you know if it starts or stalls again.")
        return

    # ── Pause/Resume/Abort/Clear ──────────────────────────────────────────────
    if len(parts) >= 3 and parts[0] == "dl" and parts[1] in ("pause", "resume", "abort", "clear"):
        action = parts[1]
        dl_id  = parts[2]
        msg_id = cq.get("message", {}).get("message_id")
        not_tracked = False
        with _dl_lock:
            if dl_id not in _downloads:
                not_tracked = True
            else:
                d = _downloads[dl_id]
                if action == "pause" and d.get("status") == "downloading":
                    d["status"] = "paused"
                elif action == "resume" and d.get("status") == "paused":
                    d["status"] = "downloading"
                elif action == "abort":
                    d["status"] = "error"
                    _dl_log(dl_id, "Cancelled by user.")
                elif action == "clear":
                    pass  # just remove the message
        if not_tracked:
            if msg_id: _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
            tg_send(chat_id, "⚠️ Download no longer tracked (bot was restarted). Tap *⬇️ Download Tracks* to start a new search.", reply_markup=TG_KEYBOARD)
            return
        if action == "clear" and msg_id:
            _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
            _sess_set(chat_id, {})
            tg_send(chat_id, "Cleared.", reply_markup=TG_KEYBOARD)
            return
        if msg_id:
            _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                     text=_progress_text(dl_id), parse_mode="Markdown",
                     reply_markup=_progress_keyboard(dl_id))
        return


def handle_tg(chat_id, text):
    session = _sess_get(chat_id)
    step    = session.get("step")

    # ── Intercept query input ─────────────────────────────────────────────────
    if step == "ask_query":
        raw = text.strip()
        if " - " in raw:
            artist, rest = raw.split(" - ", 1)
        else:
            artist, rest = "", raw
        session.update({"artist": artist.strip(), "query": rest.strip(),
                        "step": "searching"})
        _sess_set(chat_id, session)
        q = (artist.strip() + " " + rest.strip()).strip()
        _result_msgs.pop(chat_id, None)
        res    = tg_send(chat_id, f"🔍 Searching Soulseek for *{q}*…",
                         reply_markup=_SEARCH_CANCEL_KB)
        msg_id = res.get("result", {}).get("message_id")
        threading.Thread(target=_do_slskd_search,
                         args=(chat_id, session, msg_id), daemon=True).start()
        return

    # ── Main menu commands ────────────────────────────────────────────────────
    t = text.lower()
    if "download" in t or "⬇️" in text:
        handle_download_start(chat_id)
        return

    if "stats" in t or "📊" in text:
        threading.Thread(target=_send_nas_stats, args=(chat_id,), daemon=True).start()
        return

    if "library" in t or "🗂" in text:
        kb = _inline([
            [{"text": "▶️ Run now", "callback_data": "maint:confirm"},
             {"text": "❌ Cancel", "callback_data": "maint:cancel"}],
        ])
        tg_send(chat_id, "🗂 *Library Sync*\nRuns: art fetch → organize → full lyrics scan → Navidrome rescan.\nThis takes ~15 min.", reply_markup=kb)
        return

    if "nas control" in t or "control" in t or "🔌" in text:
        tg_send(chat_id, "🔌 *NAS Control*\nChoose an action:", reply_markup=_nas_control_menu_kb())
        return

    if "services" in t or "🌐" in text:
        threading.Thread(target=_send_services_status, args=(chat_id,), daemon=True).start()
        return

    tg_send(chat_id, "Tap a button below.", reply_markup=TG_KEYBOARD)

def _check_service(svc: dict) -> bool:
    try:
        with urllib.request.urlopen(svc["check"], timeout=5) as r:
            return r.status < 500
    except Exception:
        return False

def _send_services_status(chat_id: int, msg_id: int | None = None):
    """Check all EXPOSED_SERVICES in parallel and send/edit a status message."""
    import concurrent.futures

    placeholder = "🌐 *Services*\n\n⏳ Checking…"
    if msg_id is None:
        sent = tg_send(chat_id, placeholder)
        msg_id = sent.get("result", {}).get("message_id") if sent else None
    else:
        tg_edit(chat_id, msg_id, placeholder)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(EXPOSED_SERVICES)) as pool:
        futures = {pool.submit(_check_service, svc): svc["name"] for svc in EXPOSED_SERVICES}
        results = {futures[f]: f.result() for f in concurrent.futures.as_completed(futures)}

    online  = sum(1 for ok in results.values() if ok)
    total   = len(EXPOSED_SERVICES)
    summary = f"{'✅' if online == total else '⚠️'} {online}/{total} online"

    lines = [f"🌐 *Services* — {summary}", ""]
    for svc in EXPOSED_SERVICES:
        ok     = results.get(svc["name"], False)
        status = "✅" if ok else "❌"
        lines.append(f"{status} {svc['icon']} [{svc['name']}]({svc['url']})")

    now = time.strftime("%H:%M")
    lines += ["", f"_Checked at {now}_"]

    refresh_kb = _inline([[{"text": "🔄 Refresh", "callback_data": "services:refresh"}]])
    if msg_id:
        tg_edit(chat_id, msg_id, "\n".join(lines), reply_markup=refresh_kb)
    else:
        tg_send(chat_id, "\n".join(lines), reply_markup=refresh_kb)

def _startup_notification():
    """Send a Telegram boot message if this is a fresh NAS startup (uptime < 3 min)."""
    try:
        uptime_secs = float(open("/proc/uptime").read().split()[0])
    except Exception:
        return
    if uptime_secs > 180:
        log.info("[startup] Bot restarted (uptime %ds) — skipping boot notification", int(uptime_secs))
        return

    log.info("[startup] Fresh NAS boot detected (uptime %ds) — waiting 60s for services…", int(uptime_secs))
    time.sleep(60)

    # Service health checks
    results = []
    for svc in EXPOSED_SERVICES:
        ok = _check_service(svc)
        results.append((svc["icon"], svc["name"], ok))

    # Container count
    container_count = "?"
    try:
        r = subprocess.run(["/usr/bin/docker", "ps", "-q"],
                           capture_output=True, text=True, timeout=8)
        container_count = str(len(r.stdout.strip().splitlines()))
    except Exception:
        pass

    # RAM usage
    mem_line = ""
    try:
        mem: dict = {}
        for line in open("/proc/meminfo"):
            k, _, v = line.partition(":")
            mem[k.strip()] = int(v.strip().split()[0])
        total = mem.get("MemTotal", 0)
        avail = mem.get("MemAvailable", 0)
        used  = total - avail
        pct   = used * 100 // total if total else 0
        mem_line = f"💾 RAM {pct}%"
    except Exception:
        pass

    # CPU temp
    import glob as _glob
    temp_line = ""
    try:
        temps = [int(open(tf).read().strip()) // 1000
                 for tf in sorted(_glob.glob("/sys/class/thermal/thermal_zone*/temp"))
                 if int(open(tf).read().strip()) // 1000 > 0]
        if temps:
            temp_line = f"🌡 {temps[0]}°C"
    except Exception:
        pass

    # Current uptime (after the 60s wait)
    try:
        up2 = float(open("/proc/uptime").read().split()[0])
        h, rem = divmod(int(up2), 3600)
        m = rem // 60
        uptime_str = f"{h}h {m}m" if h else f"{m}m"
    except Exception:
        uptime_str = "?"

    # Format services as 2-column grid
    svc_lines = []
    for i in range(0, len(results), 2):
        left  = results[i]
        right = results[i + 1] if i + 1 < len(results) else None
        l_str = f"{'✅' if left[2] else '❌'} {left[1]}"
        if right:
            r_str = f"{'✅' if right[2] else '❌'} {right[1]}"
            svc_lines.append(f"{l_str:<20}{r_str}")
        else:
            svc_lines.append(l_str)

    offline = [name for _, name, ok in results if not ok]
    status_icon = "✅" if not offline else "⚠️"

    lines = [f"🖥 *NAS is back online* {status_icon} · up {uptime_str}", ""]
    lines += [f"`{l}`" for l in svc_lines]
    lines += [""]
    meta = "  ".join(filter(None, [f"📦 {container_count} containers", temp_line, mem_line]))
    if meta:
        lines.append(meta)
    if offline:
        lines.append(f"\n⚠️ Offline: {', '.join(offline)}")

    _notify_tg("\n".join(lines), force=True)
    log.info("[startup] Boot notification sent (%d services, %d offline)", len(results), len(offline))


def telegram_loop():
    if not BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN not set — bot disabled"); return
    log.info("Telegram bot polling…")
    offset = 0
    while True:
        try:
            res = _tg_call("getUpdates", _timeout=35, offset=offset, timeout=30)
            if res.get("ok") or "result" in res:
                global _last_tg_success
                _last_tg_success = time.time()
            for upd in res.get("result", []):
                offset = upd["update_id"] + 1

                # ── Regular message ───────────────────────────────────────────
                msg = upd.get("message", {})
                if msg:
                    chat_id = msg.get("chat", {}).get("id")
                    if not chat_id or (ALLOWED_IDS and str(chat_id) not in ALLOWED_IDS):
                        continue
                    text = (msg.get("text") or "").strip()
                    if text:
                        handle_tg(chat_id, text)
                    continue

                # ── Inline button callback ────────────────────────────────────
                cq = upd.get("callback_query", {})
                if cq:
                    chat_id = cq.get("message", {}).get("chat", {}).get("id")
                    if not chat_id or (ALLOWED_IDS and str(chat_id) not in ALLOWED_IDS):
                        _tg_call("answerCallbackQuery", callback_query_id=cq.get("id",""))
                        continue
                    handle_callback(cq)
        except Exception as e:
            log.error("Telegram poll: %s", e); time.sleep(5)

# ── DNS watchdog ──────────────────────────────────────────────────────────────
def _dns_watchdog():
    """Test actual HTTPS reachability to Telegram every 60s.
    Only restarts if HTTP fails AND getUpdates hasn't succeeded for 10+ min.
    This avoids restarting a working bot just because a fresh DNS lookup fails."""
    global _dns_fail_since
    while True:
        time.sleep(60)
        http_ok = False
        try:
            req = urllib.request.Request(
                "https://api.telegram.org",
                headers={"User-Agent": "nas-controller-watchdog/1.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                http_ok = r.status in (200, 404, 403)  # any real HTTP response = reachable
        except Exception:
            pass

        now = time.time()
        poll_age = now - _last_tg_success  # seconds since last successful getUpdates

        if http_ok or poll_age < _DNS_RESTART_AFTER:
            # Either HTTP works, or polling succeeded recently — bot is fine
            if _dns_fail_since is not None:
                log.info("DNS watchdog: Telegram reachable again — resetting timer")
            _dns_fail_since = None
        else:
            # HTTP unreachable AND no successful poll for threshold duration
            if _dns_fail_since is None:
                _dns_fail_since = now
                log.warning("DNS watchdog: Telegram unreachable — 10-min restart timer started (last poll: %.0fs ago)", poll_age)
            else:
                elapsed = now - _dns_fail_since
                log.warning("DNS watchdog: still unreachable (%.0fs / %ds, last poll: %.0fs ago)", elapsed, _DNS_RESTART_AFTER, poll_age)
                if elapsed >= _DNS_RESTART_AFTER:
                    log.warning("DNS watchdog: threshold reached — restarting nas-controller container")
                    subprocess.run(
                        ["/usr/bin/docker", "restart", "--time", "0", "nas-controller"],
                        capture_output=True, timeout=30)

# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _load_history()
    _load_seen()
    _load_discover()
    # slskd ≥0.25 validates (not creates) its incomplete dir at startup — ensure it exists
    Path("/media/sdb/Musics/Soulseek/.incomplete").mkdir(parents=True, exist_ok=True)
    threading.Thread(target=telegram_loop,          daemon=True).start()
    threading.Thread(target=watcher_loop,            daemon=True).start()
    threading.Thread(target=_dns_watchdog,           daemon=True).start()
    threading.Thread(target=_startup_notification,   daemon=True).start()
    app.run(host="0.0.0.0", port=8888, debug=False)
