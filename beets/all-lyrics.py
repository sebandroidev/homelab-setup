#!/usr/bin/env python3
"""
Fetch lyrics for all audio files lacking a .lrc sidecar.
Fallback chain: lrclib exact -> lrclib search -> lyrics.ovh (plain only)
"""
import os, time, subprocess, urllib.request, urllib.parse, json, re

LOG_PATH = "/data/logs/flac-lyrics.log"
MUSIC_DIRS = ["/media/sdb/Musics", "/media/sdb/Evyy Musics"]
AUDIO_EXTS = {".flac", ".m4a", ".mp3", ".aac", ".ogg", ".opus"}
UA = "lrclib-sidecar/1.0"

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

def fetch_lyrics(title, artist, album):
    lyrics, kind = lrclib_exact(title, artist, album)
    if not lyrics:
        lyrics, kind = lrclib_search(title, artist)
    if not lyrics:
        lyrics, kind = lyricsovh(title, artist)
    return lyrics, kind

found = skipped = missing = errors = 0

log("=== All-format lyrics fetch starting ===")

for music_dir in MUSIC_DIRS:
    if not os.path.isdir(music_dir):
        continue
    log(f"Walking: {music_dir}")
    for root, dirs, files in os.walk(music_dir):
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
                missing += 1

log(f"\n=== Done: written={found} skipped={skipped} missing={missing} errors={errors} ===")
log_fh.close()
