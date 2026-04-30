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
    {"name": "AudioMuse",   "icon": "🎧", "url": "https://audiomuse.bastien-nas.duckdns.org",  "check": "http://host.docker.internal:8001"},
    {"name": "Gitea",       "icon": "🐙", "url": "https://gitea.bastien-nas.duckdns.org",      "check": "http://host.docker.internal:8070"},
    {"name": "Slskd",       "icon": "🔍", "url": "https://slskd.bastien-nas.duckdns.org",      "check": "http://host.docker.internal:8082"},
    {"name": "Uptime Kuma", "icon": "📡", "url": "https://uptime.bastien-nas.duckdns.org",     "check": "http://host.docker.internal:8071"},
    {"name": "Grafana",     "icon": "📊", "url": "https://grafana.bastien-nas.duckdns.org",    "check": "http://host.docker.internal:3030"},
    {"name": "Orly API",    "icon": "🚀", "url": "https://api.bastien-nas.duckdns.org",        "check": "http://host.docker.internal:4000"},
    {"name": "NAS",         "icon": "🖥",  "url": "https://nas.bastien-nas.duckdns.org",        "check": "http://host.docker.internal:8888"},
]


# ── Telegram keyboard ────────────────────────────────────────────────────────
TG_KEYBOARD = json.dumps({
    "keyboard": [[{"text": "⬇️ Download Tracks"}]],
    "resize_keyboard": True,
    "persistent": True,
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
            dirs.sort()
            for fname in sorted(files):
                if os.path.splitext(fname)[1].lower() not in AUDIO_EXTS: continue
                fpath = os.path.join(root, fname)
                try: mtime = os.path.getmtime(fpath)
                except OSError: continue
                if _seen_files.get(fpath) != mtime:
                    _seen_files[fpath] = mtime
                    new_by_dir.setdefault(root, []).append(fpath)
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

        # In batch mode, suppress noisy per-dir start messages
        if not is_batch:
            _notify_tg(f"🔔 *New music detected!*\n📁 `{short}` ({count} track{'s' if count!=1 else ''})\n▶️ Running beets import…")

        r1 = subprocess.run(
            f'docker exec beets beet import "{beets_dir}" -q',
            shell=True, capture_output=True, text=True, timeout=1800)

        lyrics_summary = ""
        if is_last:
            # fetchart + embedart run on whole library — do once after all imports
            subprocess.run(
                'docker exec beets beet fetchart ; docker exec beets beet embedart -y',
                shell=True, capture_output=True, text=True, timeout=1800)
            # Organize: move Soulseek downloads into library structure
            subprocess.run(
                'docker exec beets python3 /config/beet-organize.py',
                shell=True, capture_output=True, text=True, timeout=1800)
            # Refresh ALL dirs: fetchart/embedart/organize modify files across the whole library
            _refresh_all_watch_dirs()
            # Lyrics also run once at the end
            if not is_batch:
                _notify_tg("▶️ Running lyrics fetch…")
            r2 = subprocess.run("python3 /DATA/AppData/beets/all-lyrics.py",
                shell=True, capture_output=True, text=True, timeout=3600)
            lyrics_summary = next(
                (l.strip() for l in (r2.stdout + r2.stderr).splitlines() if "Done:" in l), "")
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

def _slskd(method: str, path: str, body=None):
    url  = f"{SLSKD_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req  = urllib.request.Request(url, data=data, method=method,
               headers={"X-API-Key": SLSKD_API_KEY, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read()) if r.length != 0 else {}
    except urllib.error.HTTPError as e:
        return {"_error": e.code, "_body": e.read().decode()}

def _dl_log(dl_id: str, msg: str):
    with _dl_lock:
        _downloads[dl_id]["output"].append(msg)
        if len(_downloads[dl_id]["output"]) > 500:
            _downloads[dl_id]["output"] = _downloads[dl_id]["output"][-500:]

AUDIO_DL_EXTS = {".flac", ".mp3", ".m4a", ".ogg", ".opus"}

def _peer_folders(results: list, prefer_flac: bool = True) -> list:
    """
    Group every peer's files by their parent folder.
    Returns list of dicts sorted by (flac_count desc, total_size desc).
    """
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
    try:
        with urllib.request.urlopen(url, data, timeout=_timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        lvl = log.debug if "timed out" in str(e).lower() else log.warning
        lvl("Telegram %s: %s", method, e); return {}

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

def tg_send(chat_id, text, reply_markup=None):
    kwargs = dict(chat_id=chat_id, text=text, parse_mode="Markdown")
    if reply_markup is not None:
        kwargs["reply_markup"] = reply_markup
    return _tg_call("sendMessage", **kwargs)

def tg_edit(chat_id, msg_id, text):
    _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
             text=text, parse_mode="Markdown")

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

def _itunes_meta(artist: str, title: str, kind: str) -> dict:
    """Fetch iTunes metadata (title, artist, album, artwork_url, year)."""
    entity = "album" if kind == "album" else "song"
    q = urllib.parse.quote(f"{artist} {title}")
    url = f"https://itunes.apple.com/search?term={q}&entity={entity}&limit=1"
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read())
        results = data.get("results", [])
        if not results:
            return {}
        item = results[0]
        artwork = item.get("artworkUrl100", "").replace("100x100bb", "600x600bb")
        return {
            "title":       item.get("trackName") or item.get("collectionName", title),
            "artist":      item.get("artistName", artist),
            "album":       item.get("collectionName", ""),
            "year":        str(item.get("releaseDate", ""))[:4],
            "artwork_url": artwork,
            "genre":       item.get("primaryGenreName", ""),
        }
    except Exception:
        return {}

def _slskd_search_only(query: str) -> list:
    """Run a slskd search and return ranked folder list (no download)."""
    res = _slskd("POST", "/api/v0/searches",
                 {"searchText": query, "responseLimit": 50, "fileLimit": 1000})
    if "_error" in res:
        return []
    search_id = res.get("id")
    if not search_id:
        return []
    for _ in range(45):
        time.sleep(1)
        info = _slskd("GET", f"/api/v0/searches/{search_id}")
        if "Completed" in info.get("state", "") or info.get("isComplete"):
            break
    resp    = _slskd("GET", f"/api/v0/searches/{search_id}/responses")
    results = resp if isinstance(resp, list) else resp.get("responses", [])
    _slskd("DELETE", f"/api/v0/searches/{search_id}")
    return _peer_folders(results)

def _ytdl_search(query: str) -> list:
    """Search YouTube Music via yt-dlp, return up to 5 candidates."""
    import subprocess as sp
    cmd = [
        "yt-dlp", "--dump-json", "--flat-playlist", "--no-warnings",
        "--default-search", "ytsearch5",
        f"ytsearch5:{query}"
    ]
    try:
        out = sp.check_output(cmd, timeout=30, stderr=sp.DEVNULL)
        items = [json.loads(line) for line in out.decode().splitlines() if line.strip()]
        results = []
        for item in items[:5]:
            results.append({
                "video_id": item.get("id", ""),
                "title":    item.get("title", ""),
                "channel":  item.get("channel") or item.get("uploader", ""),
                "duration": item.get("duration_string") or item.get("duration", ""),
                "url":      item.get("url") or f"https://www.youtube.com/watch?v={item.get('id','')}",
            })
        return results
    except Exception:
        return []

def _ytdl_download_worker(dl_id: str, video_id: str, artist: str, title: str):
    """Download a YouTube track as FLAC into the Soulseek downloads dir."""
    out_dir = os.path.join(DOWNLOADS_DIR, f"{artist} - {title}")
    os.makedirs(out_dir, exist_ok=True)
    cmd = [
        "yt-dlp",
        "--extract-audio", "--audio-format", "flac", "--audio-quality", "0",
        "--embed-metadata", "--add-metadata",
        "--output", os.path.join(out_dir, "%(title)s.%(ext)s"),
        "--no-warnings",
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    with _dl_lock:
        _downloads[dl_id].update({"status": "downloading",
                                   "started": datetime.now().isoformat(timespec="seconds")})
    try:
        import subprocess as sp
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        for line in proc.stdout:
            _dl_log(dl_id, line.rstrip())
        proc.wait()
        ok = proc.returncode == 0
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
        lines.append(f"*{meta['title']}*")
    if meta.get("artist"):
        lines.append(f"🎤 {meta['artist']}")
    if meta.get("album"):
        lines.append(f"💿 {meta['album']}" + (f" ({meta['year']})" if meta.get('year') else ""))
    if meta.get("genre"):
        lines.append(f"🎸 {meta['genre']}")
    lines.append("")

    src = r.get("source", "slskd")
    if src == "slskd":
        flac   = r.get("flac_count", 0)
        nfiles = len(r.get("files", []))
        size   = r.get("total_size", 0) // (1024 * 1024)
        folder = r.get("folder", "").rsplit("\\", 1)[-1]
        lines.append(f"📁 `{folder}`")
        lines.append(f"👤 {r['username']}  •  {nfiles} file(s)  •  {size} MB" +
                     (f"  •  {flac} FLAC" if flac else ""))
        lines.append("🟢 Soulseek")
    else:
        lines.append(f"🎬 {r.get('title','')}")
        lines.append(f"📺 {r.get('channel','')}" +
                     (f"  •  {r.get('duration','')}" if r.get('duration') else ""))
        lines.append("🔴 YouTube")

    lines.append(f"\n_{idx + 1}/{total}_")
    return "\n".join(lines)

def _result_keyboard(idx: int, total: int) -> str:
    nav_row = []
    if idx > 0:
        nav_row.append({"text": "← Prev",   "callback_data": f"dl:prev"})
    nav_row.append(    {"text": f"{idx+1}/{total}", "callback_data": "dl:noop"})
    if idx < total - 1:
        nav_row.append({"text": "Next →",   "callback_data": f"dl:next"})
    action_row = [
        {"text": "⬇️ Download", "callback_data": "dl:download"},
        {"text": "❌ Cancel",   "callback_data": "dl:cancel"},
    ]
    rows = [nav_row, action_row] if nav_row else [action_row]
    return json.dumps({"inline_keyboard": rows})

def _show_result(chat_id: int, session: dict):
    idx     = session.get("idx", 0)
    text    = _result_text(session, idx)
    total   = len(session.get("results", []))
    kb      = _result_keyboard(idx, total)
    meta    = session.get("meta", {})
    msg_id  = session.get("result_msg_id")

    # Send photo if we have artwork and no message yet
    if meta.get("artwork_url") and not msg_id:
        r = _tg_call("sendPhoto", chat_id=chat_id,
                     photo=meta["artwork_url"], caption=text,
                     parse_mode="Markdown", reply_markup=kb)
        new_id = r.get("result", {}).get("message_id")
        if new_id:
            session["result_msg_id"] = new_id
            _sess_set(chat_id, session)
        return

    if msg_id:
        if meta.get("artwork_url"):
            _tg_call("editMessageCaption", chat_id=chat_id, message_id=msg_id,
                     caption=text, parse_mode="Markdown", reply_markup=kb)
        else:
            tg_edit(chat_id, msg_id, text)
            _tg_call("editMessageReplyMarkup", chat_id=chat_id, message_id=msg_id,
                     reply_markup=kb)
    else:
        r = tg_send(chat_id, text, reply_markup=kb)
        new_id = r.get("result", {}).get("message_id")
        if new_id:
            session["result_msg_id"] = new_id
            _sess_set(chat_id, session)

def _progress_text(dl_id: str) -> str:
    with _dl_lock:
        d = _downloads.get(dl_id, {})
    status = d.get("status", "queued")
    lines  = d.get("output", [])
    last   = [l for l in lines[-6:] if l.strip()]
    icons  = {"queued": "⏳", "searching": "🔍", "downloading": "⬇️",
               "done": "✅", "error": "❌", "paused": "⏸"}
    icon   = icons.get(status, "⏳")
    text   = f"{icon} *Download* — {status}\n"
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
    elif status == "paused":
        rows.append([
            {"text": "▶️ Resume",  "callback_data": f"dl:resume:{dl_id}"},
            {"text": "❌ Cancel",  "callback_data": f"dl:abort:{dl_id}"},
        ])
    rows.append([{"text": "🗑 Clear", "callback_data": f"dl:clear:{dl_id}"}])
    return json.dumps({"inline_keyboard": rows})

def _monitor_download(chat_id: int, dl_id: str, msg_id: int):
    """Background thread: edit progress message every 4s until terminal state."""
    terminal = {"done", "error"}
    while True:
        time.sleep(4)
        text = _progress_text(dl_id)
        kb   = _progress_keyboard(dl_id)
        _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                 text=text, parse_mode="Markdown", reply_markup=kb)
        with _dl_lock:
            status = _downloads.get(dl_id, {}).get("status", "error")
        if status in terminal:
            break

def _slskd_download_chosen(dl_id: str, username: str, files_to_dl: list):
    """Queue already-chosen files from slskd and monitor (no search step)."""
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

    for _ in range(1800):
        time.sleep(2)
        with _dl_lock:
            if _downloads[dl_id].get("status") == "paused":
                time.sleep(3); continue

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
        elif not ever_seen:
            break

        cleared = queued_filenames - set(states.keys()) - confirmed_done - confirmed_fail
        n_done  = len(confirmed_done) + len(cleared)
        n_error = len(confirmed_fail)
        avg_pct = (sum(f.get("percentComplete", 0) for f in states.values()) / len(states)
                   if states else 100)
        _dl_log(dl_id, f"{n_done}/{total} done, {n_error} errors — {avg_pct:.0f}% avg")

        if n_done + n_error >= total or (not states and ever_seen):
            ok = n_error == 0
            with _dl_lock:
                _downloads[dl_id].update({
                    "status": "done" if ok else "error",
                    "finished": datetime.now().isoformat(timespec="seconds"),
                })
            _dl_log(dl_id, "Done!" if ok else f"{n_error}/{total} files failed")
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
                         daemon=True).start()

    if msg_id:
        threading.Thread(target=_monitor_download,
                         args=(chat_id, dl_id, msg_id), daemon=True).start()


def handle_download_start(chat_id: int):
    _sess_set(chat_id, {"step": "ask_kind"})
    kb = _inline([[
        {"text": "🎵 Track",  "callback_data": "dl:kind:track"},
        {"text": "💿 Album",  "callback_data": "dl:kind:album"},
    ]])
    tg_send(chat_id, "What do you want to download?", reply_markup=kb)


def _do_slskd_search(chat_id: int, session: dict, msg_id: int):
    kind  = session.get("kind", "track")
    query = session.get("query", "")
    artist= session.get("artist", "")
    q     = f"{artist} {query}".strip() if artist else query

    folders = _slskd_search_only(q)

    if not folders:
        # No results
        session["step"] = "no_results"
        _sess_set(chat_id, session)
        kb = _inline([[
            {"text": "🔁 Retry",              "callback_data": "dl:retry_slskd"},
            {"text": "▶️ Retry with YouTube", "callback_data": "dl:retry_yt"},
            {"text": "❌ Cancel",             "callback_data": "dl:cancel"},
        ]])
        _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                 text=f"😕 No results on Soulseek for *{q}*", parse_mode="Markdown",
                 reply_markup=json.dumps({"inline_keyboard": [[
                     {"text": "🔁 Retry",              "callback_data": "dl:retry_slskd"},
                     {"text": "▶️ Retry with YouTube", "callback_data": "dl:retry_yt"},
                 ],[
                     {"text": "❌ Cancel",             "callback_data": "dl:cancel"},
                 ]]}))
        return

    # Attach source tag to each folder
    for f in folders:
        f["source"] = "slskd"

    meta = _itunes_meta(artist, query, kind)

    session.update({"step": "results", "results": folders[:10],
                    "idx": 0, "meta": meta})
    _sess_set(chat_id, session)

    # Delete the "Searching…" message
    _tg_call("deleteMessage", chat_id=chat_id, message_id=msg_id)
    _show_result(chat_id, session)


def _do_yt_search(chat_id: int, session: dict, msg_id: int):
    kind  = session.get("kind", "track")
    query = session.get("query", "")
    artist= session.get("artist", "")
    q     = f"{artist} {query}".strip() if artist else query

    items = _ytdl_search(q)

    if not items:
        kb = json.dumps({"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "dl:cancel"}]]})
        _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                 text=f"😕 No YouTube results for *{q}*",
                 parse_mode="Markdown", reply_markup=kb)
        return

    # Build result objects compatible with _result_text
    yt_results = [dict(r, source="yt") for r in items]
    meta = _itunes_meta(artist, query, kind)

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
    _tg_call("answerCallbackQuery", callback_query_id=cq_id)

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
        tg_send(chat_id, f"Enter *artist — {label} title*:", reply_markup=TG_KEYBOARD)
        return

    # ── Pagination ────────────────────────────────────────────────────────────
    if data == "dl:prev":
        session["idx"] = max(0, session.get("idx", 0) - 1)
        _sess_set(chat_id, session)
        _show_result(chat_id, session)
        return

    if data == "dl:next":
        total = len(session.get("results", []))
        session["idx"] = min(total - 1, session.get("idx", 0) + 1)
        _sess_set(chat_id, session)
        _show_result(chat_id, session)
        return

    # ── Download ──────────────────────────────────────────────────────────────
    if data == "dl:download":
        results = session.get("results", [])
        if not results:
            return
        # Remove result keyboard
        msg_id = session.get("result_msg_id")
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
        _sess_set(chat_id, {})
        tg_send(chat_id, "Cancelled.", reply_markup=TG_KEYBOARD)
        return

    # ── Retry Soulseek ────────────────────────────────────────────────────────
    if data == "dl:retry_slskd":
        msg_id = cq.get("message", {}).get("message_id")
        q = (session.get("artist", "") + " " + session.get("query", "")).strip()
        if msg_id:
            _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                     text=f"🔍 Searching Soulseek for *{q}*…", parse_mode="Markdown",
                     reply_markup=json.dumps({"inline_keyboard": []}))
        threading.Thread(target=_do_slskd_search,
                         args=(chat_id, session, msg_id), daemon=True).start()
        return

    # ── Retry YouTube ─────────────────────────────────────────────────────────
    if data == "dl:retry_yt":
        msg_id = cq.get("message", {}).get("message_id")
        q = (session.get("artist", "") + " " + session.get("query", "")).strip()
        if msg_id:
            _tg_call("editMessageText", chat_id=chat_id, message_id=msg_id,
                     text=f"🔍 Searching YouTube for *{q}*…", parse_mode="Markdown",
                     reply_markup=json.dumps({"inline_keyboard": []}))
        threading.Thread(target=_do_yt_search,
                         args=(chat_id, session, msg_id), daemon=True).start()
        return

    # ── Pause/Resume/Abort/Clear ──────────────────────────────────────────────
    if len(parts) >= 3 and parts[0] == "dl" and parts[1] in ("pause", "resume", "abort", "clear"):
        action = parts[1]
        dl_id  = parts[2]
        msg_id = cq.get("message", {}).get("message_id")
        with _dl_lock:
            d = _downloads.get(dl_id, {})
            if action == "pause" and d.get("status") == "downloading":
                _downloads[dl_id]["status"] = "paused"
            elif action == "resume" and d.get("status") == "paused":
                _downloads[dl_id]["status"] = "downloading"
            elif action == "abort":
                _downloads[dl_id]["status"] = "error"
                _dl_log(dl_id, "Cancelled by user.")
            elif action == "clear":
                pass  # just remove the message
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
        res    = tg_send(chat_id, f"🔍 Searching Soulseek for *{q}*…")
        msg_id = res.get("result", {}).get("message_id")
        threading.Thread(target=_do_slskd_search,
                         args=(chat_id, session, msg_id), daemon=True).start()
        return

    # ── Main menu commands ────────────────────────────────────────────────────
    t = text.lower()
    if "download" in t or "⬇️" in text:
        handle_download_start(chat_id)
        return

    tg_send(chat_id, "Tap *⬇️ Download Tracks* to get started.", reply_markup=TG_KEYBOARD)

def telegram_loop():
    if not BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN not set — bot disabled"); return
    log.info("Telegram bot polling…")
    offset = 0
    while True:
        try:
            res = _tg_call("getUpdates", _timeout=35, offset=offset, timeout=30)
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
                    data = cq.get("data", "")
                    if data.startswith("dl:"):
                        handle_callback(cq)
        except Exception as e:
            log.error("Telegram poll: %s", e); time.sleep(5)

# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _load_history()
    _load_seen()
    _load_discover()
    threading.Thread(target=telegram_loop, daemon=True).start()
    threading.Thread(target=watcher_loop,  daemon=True).start()
    app.run(host="0.0.0.0", port=8888, debug=False)
