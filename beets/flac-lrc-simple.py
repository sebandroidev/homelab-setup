import os, time, subprocess, urllib.request, urllib.parse, json, sys

LOG = open("/DATA/AppData/beets/flac-lyrics.log", "w", buffering=1)

def log(msg):
    LOG.write(msg + "\n")
    LOG.flush()

MUSIC_DIRS = ["/media/sdb/Musics", "/media/sdb/Evyy Musics"]
UA = "lrclib-flac-sidecar/1.0"

log("=== Starting FLAC lyrics fetch ===")

def ffprobe_tags(path):
    r = subprocess.run(["ffprobe","-v","quiet","-print_format","json","-show_entries","format_tags",path],
                       capture_output=True, text=True, timeout=15)
    if r.returncode != 0:
        return {}
    try:
        raw = json.loads(r.stdout).get("format",{}).get("tags",{})
        return {k.lower(): v for k,v in raw.items()}
    except:
        return {}

def ffprobe_duration(path):
    r = subprocess.run(["ffprobe","-v","quiet","-print_format","json","-show_entries","format=duration",path],
                       capture_output=True, text=True, timeout=15)
    try:
        return float(json.loads(r.stdout)["format"]["duration"])
    except:
        return 0

def lrclib(title, artist, album, dur):
    for p in [{"track_name":title,"artist_name":artist,"album_name":album,"duration":int(dur)},
              {"track_name":title,"artist_name":artist,"duration":int(dur)}]:
        url = "https://lrclib.net/api/get?" + urllib.parse.urlencode(p)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=10) as r:
                if r.status == 200:
                    return json.loads(r.read())
        except:
            pass
    return None

found = skipped = missing = errors = 0

for music_dir in MUSIC_DIRS:
    if not os.path.isdir(music_dir):
        log(f"SKIP dir (not found): {music_dir}")
        continue
    log(f"Walking: {music_dir}")
    for root, dirs, files in os.walk(music_dir):
        dirs.sort()
        for fname in sorted(files):
            if not fname.lower().endswith(".flac"):
                continue
            fpath = os.path.join(root, fname)
            lpath = fpath[:-5] + ".lrc"
            if os.path.exists(lpath):
                skipped += 1
                continue
            tags = ffprobe_tags(fpath)
            title  = tags.get("title","")
            artist = tags.get("artist","") or tags.get("albumartist","")
            album  = tags.get("album","")
            if not title or not artist:
                log(f"  SKIP no-tags: {fname}")
                errors += 1
                continue
            dur = ffprobe_duration(fpath)
            log(f"  -> {artist} - {title}")
            data = lrclib(title, artist, album, dur)
            time.sleep(1.5)
            if data and data.get("syncedLyrics"):
                open(lpath,"w",encoding="utf-8").write(data["syncedLyrics"])
                log(f"     synced OK")
                found += 1
            elif data and data.get("plainLyrics"):
                open(lpath,"w",encoding="utf-8").write(data["plainLyrics"])
                log(f"     plain OK")
                found += 1
            else:
                log(f"     not found")
                missing += 1

log(f"\n=== Done: written={found} skipped={skipped} missing={missing} errors={errors} ===")
LOG.close()
