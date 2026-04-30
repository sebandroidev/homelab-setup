#!/usr/bin/env python3
"""
Fetch lyrics from lrclib.net for all audio formats lacking a .lrc sidecar.
Writes .lrc sidecar files (works for .flac, .m4a, .mp3, .ogg, etc.)
"""
import os, time, subprocess, urllib.request, urllib.parse, json

LOG_PATH = "/data/logs/flac-lyrics.log"
MUSIC_DIRS = ["/media/sdb/Musics", "/media/sdb/Evyy Musics"]
AUDIO_EXTS = {".flac", ".m4a", ".mp3", ".aac", ".ogg", ".opus"}
UA = "lrclib-sidecar/1.0"

log_fh = open(LOG_PATH, "w", buffering=1)
def log(msg):
    log_fh.write(msg + "\n")
    log_fh.flush()

def ffprobe_tags(path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_entries", "format_tags", path],
            capture_output=True, text=True, timeout=10
        )
        raw = json.loads(r.stdout).get("format", {}).get("tags", {})
        return {k.lower(): v for k, v in raw.items()}
    except Exception:
        return {}

def strip_feat(artist):
    import re
    return re.split(r'\s+(feat\.?|ft\.?|featuring)\s+', artist, flags=re.IGNORECASE)[0].strip()

def lrclib(title, artist, album):
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
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=15) as r:
                if r.status == 200:
                    data = json.loads(r.read())
                    if data and (data.get("syncedLyrics") or data.get("plainLyrics")):
                        return data
        except Exception:
            pass
    return None

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
            data = lrclib(title, artist, album)
            time.sleep(1)
            lyrics = data.get("syncedLyrics") or data.get("plainLyrics") if data else None
            kind   = "synced" if (data and data.get("syncedLyrics")) else "plain"
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
