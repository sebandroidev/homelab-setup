#!/usr/bin/env python3
"""
beet-organize.py — Reorganize Soulseek music into clean library structure.

Steps:
  0. Prune ghost entries
  1. Scan .lrc sidecars
  2. Read tags from Soulseek audio files
  3. Move files to $albumartist/$year - $album/ structure
  4. Update beet DB with new paths
  5. Move .lrc sidecars
  6. Delete junk, remove empty dirs
  7. Detect duplicates → print DUPLICATES_JSON line for bot to handle
"""

import json
import os
import re
import shutil
import sqlite3
import subprocess
import unicodedata
from collections import defaultdict
from pathlib import Path

BEETSDIR   = "/config"
BEET_BIN   = "/lsiopy/bin/beet"
DB_PATH    = "/config/musiclibrary.db"
MUSIC_ROOT = Path("/music")
SOULSEEK   = MUSIC_ROOT / "Soulseek"
AUDIO_EXTS = {".flac", ".mp3", ".m4a", ".ogg", ".opus", ".wav", ".aiff", ".aac"}
JUNK_EXTS  = {".nfo", ".sfv", ".md5", ".nzb", ".srr", ".ffp", ".accurip"}

SAFE_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
ENV = {
    **os.environ,
    "BEETSDIR": BEETSDIR,
    "PATH": "/lsiopy/bin:/usr/local/bin:/usr/bin:/bin",
}

FORMAT_RANK = {"flac": 0, "alac": 1, "wav": 2, "aiff": 3,
               "ogg": 4, "opus": 5, "m4a": 6, "mp3": 7, "aac": 8}


def db_path_to_abs(db_path: str) -> Path:
    p = db_path.strip()
    if p.startswith("/"):
        return Path(p)
    return MUSIC_ROOT / p


def abs_to_db_path(abs_path: Path) -> str:
    try:
        return str(abs_path.relative_to(MUSIC_ROOT))
    except ValueError:
        return str(abs_path)


def safe_name(s: str) -> str:
    s = SAFE_RE.sub("_", s.strip())
    return s.strip(". ") or "_"


def get_tag(audio, *keys):
    for key in keys:
        val = audio.get(key)
        if val:
            if hasattr(val, '__iter__') and not isinstance(val, str):
                val = str(val[0])
            val = str(val).strip()
            if val:
                return val
    return ""


def read_tags(path: Path) -> dict | None:
    try:
        import mutagen
        audio = mutagen.File(path, easy=True)
        if audio is None:
            return None
        albumartist = (
            get_tag(audio, "albumartist", "TPE2") or
            get_tag(audio, "artist", "TPE1")
        )
        year   = get_tag(audio, "date", "year", "TDRC")[:4]
        album  = get_tag(audio, "album", "TALB")
        track  = get_tag(audio, "tracknumber", "TRCK").split("/")[0].split("\\")[0]
        title  = get_tag(audio, "title", "TIT2")
        return {
            "albumartist": albumartist,
            "year": year if year.isdigit() else "",
            "album": album,
            "track": track if track.isdigit() else "0",
            "title": title,
        }
    except Exception:
        return None


def compute_dest(tags: dict, ext: str) -> Path:
    artist = safe_name(tags["albumartist"])
    yr     = tags["year"].zfill(4) if tags["year"] else "0000"
    album  = safe_name(tags["album"])
    tr     = str(int(tags["track"])).zfill(2) if tags["track"].isdigit() else "00"
    title  = safe_name(tags["title"]) if tags["title"] else "Unknown"
    folder = MUSIC_ROOT / artist / f"{yr} - {album}"
    return folder / f"{tr} - {title}{ext}"


def db_connect():
    return sqlite3.connect(DB_PATH)


def prune_ghosts(con) -> int:
    rows = con.execute("SELECT id, path FROM items").fetchall()
    ghost_ids = []
    for iid, raw_path in rows:
        p = raw_path.decode("utf-8", errors="replace") if isinstance(raw_path, bytes) else raw_path
        abs_p = db_path_to_abs(p)
        if not abs_p.exists():
            ghost_ids.append(iid)
    if ghost_ids:
        con.executemany("DELETE FROM items WHERE id = ?", [(i,) for i in ghost_ids])
        con.execute("""
            DELETE FROM albums WHERE id NOT IN (
                SELECT DISTINCT album_id FROM items WHERE album_id IS NOT NULL
            )
        """)
        con.commit()
    return len(ghost_ids)


def find_lrc_pairs(root: Path) -> dict:
    pairs = {}
    for lrc in root.rglob("*.lrc"):
        for ext in AUDIO_EXTS:
            candidate = lrc.with_suffix(ext)
            if candidate.exists():
                pairs[candidate] = lrc
                break
    return pairs


def delete_junk(root: Path) -> int:
    deleted = 0
    for f in root.rglob("*"):
        if not f.is_file():
            continue
        low = f.name.lower()
        if f.suffix.lower() in JUNK_EXTS:
            f.unlink(); deleted += 1
        elif low.endswith(".old") or low.endswith(".bak"):
            f.unlink(); deleted += 1
    return deleted


def remove_empty_dirs(root: Path) -> int:
    removed = 0
    for dirpath, _, _ in os.walk(root, topdown=False):
        d = Path(dirpath)
        if d == root:
            continue
        try:
            if not any(d.iterdir()):
                d.rmdir(); removed += 1
        except Exception:
            pass
    return removed


def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def detect_duplicates(con) -> list:
    """Return duplicate groups: [{keep, delete: [...]}, ...]"""
    rows = con.execute(
        "SELECT id, path, title, artist, albumartist, album, format, bitrate FROM items"
    ).fetchall()

    groups = defaultdict(list)
    for iid, raw_path, title, artist, albumartist, album, fmt, bitrate in rows:
        path = raw_path.decode("utf-8", errors="replace") if isinstance(raw_path, bytes) else (raw_path or "")
        aa   = (albumartist or artist or "").strip()
        t    = (title or "").strip()
        key  = (_norm(aa), _norm(t))
        if not key[0] or not key[1]:
            continue
        abs_path = db_path_to_abs(path)
        size = abs_path.stat().st_size if abs_path.exists() else 0
        groups[key].append({
            "id":      iid,
            "path":    str(abs_path),
            "title":   t,
            "artist":  aa,
            "album":   (album or "").strip(),
            "format":  (fmt or "").lower(),
            "bitrate": bitrate or 0,
            "size":    size,
        })

    result = []
    for key, items in groups.items():
        if len(items) < 2:
            continue
        # Keep best: lowest format rank, then biggest size
        def score(item):
            return (FORMAT_RANK.get(item["format"], 99), -item["size"])
        items.sort(key=score)
        result.append({"keep": items[0], "delete": items[1:]})

    return result


def main():
    print("=" * 60)
    print("  beet-organize — Soulseek library reorganizer")
    print("=" * 60)

    if not SOULSEEK.exists():
        print("✅ No Soulseek folder — nothing to do.")
        print("DUPLICATES_JSON: []")
        return

    con = db_connect()

    print("\n[0/6] Pruning ghost library entries…")
    pruned = prune_ghosts(con)
    print(f"      Removed {pruned} ghost entries")

    print("\n[1/6] Scanning .lrc sidecars…")
    lrc_pairs = find_lrc_pairs(SOULSEEK)
    print(f"      Found {len(lrc_pairs)} .lrc sidecars")

    print("\n[2/6] Reading tags from Soulseek audio files…")
    audio_files = [f for f in SOULSEEK.rglob("*") if f.is_file() and f.suffix.lower() in AUDIO_EXTS]
    print(f"      Found {len(audio_files)} audio files")

    print("\n[3/6] Moving tracks to organized structure…")
    path_map  = {}
    moved = already_there = skipped_no_meta = failed = 0

    for src in audio_files:
        tags = read_tags(src)
        if not tags or not tags["albumartist"] or not tags["year"] or not tags["album"]:
            skipped_no_meta += 1
            continue
        dest = compute_dest(tags, src.suffix.lower())
        if src == dest:
            already_there += 1
            continue
        if dest.exists():
            stem, ext = dest.stem, dest.suffix
            for n in range(1, 100):
                candidate = dest.parent / f"{stem} ({n}){ext}"
                if not candidate.exists():
                    dest = candidate
                    break
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dest))
            path_map[src] = dest
            moved += 1
        except Exception as e:
            print(f"      ⚠  {src.name}: {e}")
            failed += 1

    print(f"      Moved: {moved} | Already correct: {already_there} "
          f"| No metadata: {skipped_no_meta} | Failed: {failed}")

    print("\n[4/6] Updating library DB paths…")
    db_updated = 0
    for old_abs, new_abs in path_map.items():
        old_rel = abs_to_db_path(old_abs)
        new_rel = abs_to_db_path(new_abs)
        for old_stored in [old_rel, str(old_abs)]:
            stored_bytes = old_stored.encode()
            count = con.execute(
                "UPDATE items SET path = ? WHERE path = ?",
                (new_rel.encode(), stored_bytes)
            ).rowcount
            if count:
                db_updated += 1
                break
    con.commit()
    print(f"      Updated {db_updated} DB entries")

    print("\n[5/6] Moving .lrc sidecars…")
    lrc_moved = lrc_skipped = 0
    for audio_old, lrc_old in lrc_pairs.items():
        audio_new = path_map.get(audio_old)
        if not audio_new or not lrc_old.exists():
            continue
        lrc_new = audio_new.with_suffix(".lrc")
        lrc_new.parent.mkdir(parents=True, exist_ok=True)
        if not lrc_new.exists():
            shutil.move(str(lrc_old), str(lrc_new))
            lrc_moved += 1
        else:
            lrc_skipped += 1
    print(f"      Moved: {lrc_moved} | Already there: {lrc_skipped}")

    con.close()

    print("\n[6/6] Cleaning junk and empty dirs…")
    deleted = delete_junk(MUSIC_ROOT)
    removed = remove_empty_dirs(MUSIC_ROOT)
    print(f"      Junk deleted: {deleted} | Empty dirs removed: {removed}")

    print("\n[7/7] Detecting duplicates…")
    con2 = db_connect()
    dups = detect_duplicates(con2)
    con2.close()
    print(f"      Found {len(dups)} duplicate group(s)")
    # Emit structured line for the bot to parse
    print(f"DUPLICATES_JSON: {json.dumps(dups)}")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
