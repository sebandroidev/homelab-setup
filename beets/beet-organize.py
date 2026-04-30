#!/usr/bin/env python3
"""
beet-organize.py — Reorganize Soulseek music into clean library structure.

Beet stores paths relative to `directory: /music` for files under /music.
This script:
  1. Prunes ghost entries (file missing on disk) — using MUSIC_ROOT-relative resolution
  2. Reads tags directly from audio files via mutagen
  3. Moves files to $albumartist/$year - $album/ structure
  4. Updates beet DB with new relative paths
  5. Moves .lrc sidecars alongside new audio locations
  6. Deletes junk files (.nfo, .sfv, .md5, *.old, etc.)
  7. Removes empty directories
  8. Runs beet update to sync remaining metadata

Run inside beets container: python3 /config/beet-organize.py
"""

import os
import re
import shutil
import sqlite3
import subprocess
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


def db_path_to_abs(db_path: str) -> Path:
    """Convert beet DB path (possibly relative) to absolute filesystem path."""
    p = db_path.strip()
    if p.startswith("/"):
        return Path(p)
    return MUSIC_ROOT / p


def abs_to_db_path(abs_path: Path) -> str:
    """Convert absolute path to beet DB format (relative if under MUSIC_ROOT)."""
    try:
        return str(abs_path.relative_to(MUSIC_ROOT))
    except ValueError:
        return str(abs_path)


def safe_name(s: str) -> str:
    s = SAFE_RE.sub("_", s.strip())
    return s.strip(". ") or "_"


def get_tag(audio, *keys):
    """Extract first non-empty tag value from mutagen audio object."""
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
    """Read relevant tags from an audio file. Returns None if unreadable."""
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
    """Replicate beet path template: $albumartist/$year - $album/$track - $title"""
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
    """Delete DB entries where the file no longer exists on disk."""
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
    """Return {audio_path: lrc_path} for same-dir, same-stem pairs."""
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


def main():
    print("=" * 60)
    print("  beet-organize — Soulseek library reorganizer")
    print("=" * 60)

    if not SOULSEEK.exists():
        print("✅ No Soulseek folder — nothing to do.")
        return

    con = db_connect()

    # Step 0: prune ghosts
    print("\n[0/6] Pruning ghost library entries…")
    pruned = prune_ghosts(con)
    print(f"      Removed {pruned} ghost entries")

    # Step 1: scan .lrc pairs before any moves
    print("\n[1/6] Scanning .lrc sidecars…")
    lrc_pairs = find_lrc_pairs(SOULSEEK)
    print(f"      Found {len(lrc_pairs)} .lrc sidecars")

    # Step 2: gather all audio files in Soulseek and read their tags
    print("\n[2/6] Reading tags from Soulseek audio files…")
    audio_files = [f for f in SOULSEEK.rglob("*") if f.is_file() and f.suffix.lower() in AUDIO_EXTS]
    print(f"      Found {len(audio_files)} audio files")

    # Step 3: move files to organized structure
    print("\n[3/6] Moving tracks to organized structure…")
    path_map  = {}  # old_abs → new_abs (for .lrc and DB update)
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

        # Handle filename collision
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

    # Step 4: update DB paths for moved files
    print("\n[4/6] Updating library DB paths…")
    db_updated = 0
    for old_abs, new_abs in path_map.items():
        old_rel = abs_to_db_path(old_abs)
        new_rel = abs_to_db_path(new_abs)
        # Match by old relative or absolute path
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

    # Step 5: move .lrc sidecars
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

    # Step 6: cleanup
    print("\n[6/6] Cleaning junk and empty dirs…")
    deleted = delete_junk(MUSIC_ROOT)
    removed = remove_empty_dirs(MUSIC_ROOT)
    print(f"      Junk deleted: {deleted} | Empty dirs removed: {removed}")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
