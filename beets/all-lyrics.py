#!/usr/bin/env python3
"""
Fetch lyrics for all audio files lacking a .lrc sidecar.
Fallback chain: lrclib exact -> lrclib search -> lyrics.ovh -> genius (plain only)

Usage:
  python3 all-lyrics.py                        # scan all MUSIC_DIRS
  python3 all-lyrics.py --dirs /path/a /path/b # scan only specific dirs
"""
import argparse, os, time, subprocess, urllib.request, urllib.parse, json, re, html as _html
from html.parser import HTMLParser

LOG_PATH   = "/data/logs/flac-lyrics.log"
SKIP_FILE  = "/DATA/AppData/beets/lyrics_skipped.json"
ALL_DIRS   = ["/media/sdb/Musics", "/media/sdb/Evyy Musics"]
AUDIO_EXTS = {".flac", ".m4a", ".mp3", ".aac", ".ogg", ".opus"}
UA = "lrclib-sidecar/1.0"
GENIUS_TOKEN = os.getenv("GENIUS_ACCESS_TOKEN", "")

ap = argparse.ArgumentParser()
ap.add_argument("--dirs", nargs="*", default=None,
                help="Directories to scan (default: all music dirs)")
args, _ = ap.parse_known_args()
MUSIC_DIRS = args.dirs if args.dirs else ALL_DIRS

# Load skip list — tracks confirmed unfindable, never retry
try:
    _skip_set = set(json.load(open(SKIP_FILE)))
except Exception:
    _skip_set = set()

def _save_skip():
    try:
        open(SKIP_FILE, "w").write(json.dumps(sorted(_skip_set)))
    except Exception:
        pass

log_fh = open(LOG_PATH, "w", buffering=1)
def log(msg):
    print(msg, flush=True)
    log_fh.write(msg + "\n")
    log_fh.flush()

def ffprobe_tags(path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_entries", "format_tags", path],
            capture_output=True, text=True, timeout=10)
        raw = json.loads(r.stdout).get("format", {}).get("tags", {})
        return {k.lower(): v for k, v in raw.items()}
    except Exception:
        return {}

def strip_feat(artist):
    return re.split(r'\s+(feat\.?|ft\.?|featuring)\s+', artist, flags=re.IGNORECASE)[0].strip()

def _http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status, json.loads(r.read())

def lrclib_exact(title, artist, album):
    main_artist = strip_feat(artist)
    attempts = [
        {"track_name": title, "artist_name": artist, "album_name": album},
        {"track_name": title, "artist_name": artist},
    ]
    if main_artist != artist:
        attempts += [
            {"track_name": title, "artist_name": main_artist, "album_name": album},
            {"track_name": title, "artist_name": main_artist},
        ]
    for params in attempts:
        url = "https://lrclib.net/api/get?" + urllib.parse.urlencode(params)
        try:
            status, data = _http_get(url)
            if status == 200 and data:
                if data.get("syncedLyrics"):
                    return data["syncedLyrics"], "synced"
                if data.get("plainLyrics"):
                    return data["plainLyrics"], "plain"
        except Exception:
            pass
    return None, None

def lrclib_search(title, artist):
    main_artist = strip_feat(artist)
    for art in ([artist, main_artist] if main_artist != artist else [artist]):
        url = "https://lrclib.net/api/search?" + urllib.parse.urlencode(
            {"artist_name": art, "track_name": title})
        try:
            status, results = _http_get(url)
            if status == 200 and results:
                for r in results:
                    if r.get("syncedLyrics"):
                        return r["syncedLyrics"], "synced(search)"
                    if r.get("plainLyrics"):
                        return r["plainLyrics"], "plain(search)"
        except Exception:
            pass
    return None, None

def lyricsovh(title, artist):
    try:
        url = "https://api.lyrics.ovh/v1/{}/{}".format(
            urllib.parse.quote(artist), urllib.parse.quote(title))
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
            lyrics = data.get("lyrics", "").strip()
            if lyrics:
                return lyrics, "plain(ovh)"
    except Exception:
        pass
    return None, None

class _GeniusParser(HTMLParser):
    """Extract lyrics from Genius song page (data-lyrics-container divs)."""
    def __init__(self):
        super().__init__()
        self._in = False; self._depth = 0
        self._buf = []; self.parts = []
    def handle_starttag(self, tag, attrs):
        if tag == "div" and dict(attrs).get("data-lyrics-container") == "true":
            self._in = True; self._depth = 1; return
        if not self._in: return
        if tag == "br":
            self._buf.append("\n")
        elif tag == "div":
            self._depth += 1
    def handle_endtag(self, tag):
        if not self._in: return
        if tag == "div":
            self._depth -= 1
            if self._depth <= 0:
                self._in = False
                self.parts.append("".join(self._buf).strip())
                self._buf = []
    def handle_data(self, data):
        if self._in: self._buf.append(data)
    def handle_entityref(self, name):
        if self._in: self._buf.append(_html.unescape(f"&{name};"))
    def handle_charref(self, name):
        if self._in: self._buf.append(_html.unescape(f"&#{name};"))

def genius(title, artist):
    if not GENIUS_TOKEN:
        return None, None
    try:
        q = urllib.parse.quote(f"{strip_feat(artist)} {title}")
        req = urllib.request.Request(
            f"https://api.genius.com/search?q={q}",
            headers={"Authorization": f"Bearer {GENIUS_TOKEN}", "User-Agent": UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            hits = json.loads(r.read()).get("response", {}).get("hits", [])
        if not hits:
            return None, None
        t_norm = re.sub(r"[^\w\s]", "", title.lower()).strip()
        song_url = None
        for hit in hits[:5]:
            res = hit.get("result", {})
            ht = re.sub(r"[^\w\s]", "", res.get("title", "").lower()).strip()
            if t_norm in ht or ht in t_norm:
                song_url = res.get("url"); break
        if not song_url:
            song_url = hits[0]["result"].get("url")
        if not song_url:
            return None, None
        req2 = urllib.request.Request(
            song_url,
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})
        with urllib.request.urlopen(req2, timeout=20) as r:
            html_bytes = r.read()
        parser = _GeniusParser()
        parser.feed(html_bytes.decode("utf-8", errors="replace"))
        text = "\n\n".join(p for p in parser.parts if p).strip()
        if len(text) < 50:
            return None, None
        return text, "plain(genius)"
    except Exception:
        pass
    return None, None

def fetch_lyrics(title, artist, album):
    lyrics, kind = lrclib_exact(title, artist, album)
    if not lyrics:
        lyrics, kind = lrclib_search(title, artist)
    if not lyrics:
        lyrics, kind = lyricsovh(title, artist)
    if not lyrics:
        lyrics, kind = genius(title, artist)
    return lyrics, kind

found = skipped = missing = errors = 0

log(f"=== All-format lyrics fetch starting (dirs: {MUSIC_DIRS}) ===")

for music_dir in MUSIC_DIRS:
    if not os.path.isdir(music_dir):
        continue
    log(f"Walking: {music_dir}")
    for root, dirs, files in os.walk(music_dir):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        dirs.sort()
        for fname in sorted(files):
            ext = os.path.splitext(fname)[1].lower()
            if ext not in AUDIO_EXTS:
                continue
            fpath = os.path.join(root, fname)
            lpath = os.path.splitext(fpath)[0] + ".lrc"
            if os.path.exists(lpath):
                skipped += 1
                continue
            if fpath in _skip_set:
                skipped += 1
                continue
            tags = ffprobe_tags(fpath)
            title  = tags.get("title", "")
            artist = (tags.get("artist") or tags.get("albumartist")
                      or tags.get("album artist") or "")
            album  = tags.get("album", "")
            if not title or not artist:
                log(f"  SKIP no-tags: {fname}")
                errors += 1
                continue
            log(f"  {artist} - {title}")
            lyrics, kind = fetch_lyrics(title, artist, album)
            time.sleep(1)
            if lyrics:
                try:
                    open(lpath, "w", encoding="utf-8").write(lyrics)
                    log(f"    -> {kind} OK")
                    found += 1
                except Exception as e:
                    log(f"    -> write error: {e}")
                    errors += 1
            else:
                log(f"    -> not found")
                _skip_set.add(fpath)
                _save_skip()
                missing += 1

log(f"\n=== Done: written={found} skipped={skipped} missing={missing} errors={errors} ===")
log_fh.close()
