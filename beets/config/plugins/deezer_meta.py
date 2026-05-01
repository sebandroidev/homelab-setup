"""
deezer_meta — beets plugin
Fills missing metadata (album, year, genre, clean title) and cover art from
the Deezer API, iTunes Search API, and MusicBrainz Cover Art Archive.

Commands:
  beet deezerfix [query]   Fill missing album/year/genre from Deezer
  beet artfix [query]      Fetch and embed missing cover art
  beet promoclean [query]  Strip DJ-promo artifacts from titles (BPM, Main, Clean…)

Auto-hook: runs on every as-is import (no MusicBrainz ID).
"""
import os, re, json, urllib.request, urllib.parse

from beets.plugins import BeetsPlugin
from beets import ui

_API          = "https://api.deezer.com"
_ITUNES_API   = "https://itunes.apple.com/search"
_CAA_API      = "https://coverartarchive.org/release/{}/front"
_UA           = "beets-deezer-meta/1.0"

_PROMO_RE = [
    re.compile(r'\s+\d{2,3}\s*$'),
    re.compile(r'\s*\(\s*Main\s*\)\s*', re.I),
    re.compile(r'\s*\(\s*Clean\s*\)\s*', re.I),
    re.compile(r'\s*\(\s*Dirty\s*\)\s*', re.I),
    re.compile(r'\s*\(\s*Explicit\s*\)\s*', re.I),
    re.compile(r'\s*\(\s*Radio\s*Edit\s*\)\s*', re.I),
    re.compile(r'\s*\(\s*Extended\s*Mix\s*\)\s*', re.I),
    re.compile(r'\s*\(\s*Instrumental\s*\)\s*', re.I),
    re.compile(r'\s*\[\s*Explicit\s*\]\s*', re.I),
    # YouTube video title suffixes
    re.compile(r'\s*[\(\[]\s*Official\s+Music\s+Video\s*[\)\]]\s*', re.I),
    re.compile(r'\s*[\(\[]\s*Official\s+Video\s*[\)\]]\s*', re.I),
    re.compile(r'\s*[\(\[]\s*Official\s+Audio\s*[\)\]]\s*', re.I),
    re.compile(r'\s*[\(\[]\s*Lyrics?\s+Video\s*[\)\]]\s*', re.I),
    re.compile(r'\s*[\(\[]\s*Lyrics?\s*[\)\]]\s*', re.I),
    re.compile(r'\s*[\(\[]\s*(?:HD|HQ|4K)\s*[\)\]]\s*', re.I),
    re.compile(r'\s*[\(\[]\s*Audio\s*[\)\]]\s*', re.I),
    re.compile(r'\s*[\(\[]\s*Visualizer\s*[\)\]]\s*', re.I),
]

_COLLAB_RE = re.compile(
    r'\s+(ft\.?|feat\.?|featuring|x\s+|vs\.?)\s+.*$', re.I)


def _clean_promo(title):
    for pat in _PROMO_RE:
        title = pat.sub(' ', title)
    return ' '.join(title.split())


def _main_artist(artist):
    return _COLLAB_RE.sub('', artist).strip()


def _strip_artist_prefix(title, artist):
    """Remove 'Artist - ' prefix that yt-dlp bakes into video titles."""
    for art in [artist, _main_artist(artist)]:
        if art and title.lower().startswith(art.lower() + ' - '):
            return title[len(art) + 3:].strip()
    return title


def _http(url):
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.loads(r.read())


def _download_bytes(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.read()
    except Exception:
        return None


def _deezer_search(artist, title):
    """Progressive search: full artist → main artist → strip feat from title → title only."""
    main = _main_artist(artist)
    core = _COLLAB_RE.sub('', title).strip()
    attempts = []
    if artist:
        attempts.append('artist:"{}" track:"{}"'.format(artist, title))
    if main and main.lower() != artist.lower():
        attempts.append('artist:"{}" track:"{}"'.format(main, title))
    if core and core.lower() != title.lower():
        attempts.append('artist:"{}" track:"{}"'.format(main or artist, core))
    attempts.append('track:"{}"'.format(title))
    if core and core.lower() != title.lower():
        attempts.append('track:"{}"'.format(core))

    for q in attempts:
        url = _API + "/search?" + urllib.parse.urlencode({"q": q, "limit": 5})
        try:
            data = _http(url).get("data", [])
            if data:
                return data[0]
        except Exception:
            pass
    return None


def _deezer_album(album_id):
    try:
        return _http("{}/album/{}".format(_API, album_id))
    except Exception:
        return None


def _safe(item, field, default=""):
    """Read an item field without raising AttributeError."""
    return getattr(item, field, default) or default


# ── Cover art ─────────────────────────────────────────────────────────────────

def _has_embedded_art(path):
    """
    Return True if the audio file already has proper JPEG cover art.
    PNG art (e.g. YouTube thumbnails embedded by yt-dlp) is NOT considered
    proper — artfix will replace it with a sourced JPEG.
    """
    try:
        ext = os.path.splitext(path)[1].lower()
        import mutagen
        f = mutagen.File(path)
        if f is None:
            return False
        if ext == ".flac":
            pics = getattr(f, "pictures", [])
            return any(p.mime == "image/jpeg" for p in pics)
        if ext == ".mp3":
            apics = f.tags.getall("APIC") if f.tags else []
            return any(getattr(a, "mime", "") == "image/jpeg" for a in apics)
    except Exception:
        pass
    return False


def _fetch_art_url(artist, album_name, album_data, mb_albumid=""):
    """
    Return (img_bytes, source_label) trying in order:
      1. Deezer cover_xl / cover_big
      2. iTunes Search API
      3. MusicBrainz Cover Art Archive (needs mb_albumid)
    Returns (None, None) if all sources fail.
    """
    # 1. Deezer
    if album_data:
        for key in ("cover_xl", "cover_big", "cover_medium"):
            url = album_data.get(key, "")
            if url and not url.endswith("2fa39ec4-c44e-b7cc-9e08-31e89b61e43b"):
                # Deezer placeholder image check (generic grey square)
                data = _download_bytes(url)
                if data and len(data) > 5000:
                    return data, "deezer ({})".format(key)

    # 2. iTunes Search API
    q = "{} {}".format(artist, album_name or "").strip()
    if q:
        try:
            url = _ITUNES_API + "?" + urllib.parse.urlencode(
                {"term": q, "media": "music", "entity": "album", "limit": 5})
            results = _http(url).get("results", [])
            for r in results:
                art = r.get("artworkUrl100", "")
                if art:
                    art = re.sub(r'/\d+x\d+bb\.', '/600x600bb.', art)
                    data = _download_bytes(art)
                    if data and len(data) > 5000:
                        return data, "itunes"
        except Exception:
            pass

    # 3. MusicBrainz Cover Art Archive
    if mb_albumid:
        url = _CAA_API.format(mb_albumid)
        data = _download_bytes(url)
        if data and len(data) > 5000:
            return data, "musicbrainz caa"

    return None, None


def _embed_art(item, img_data, log):
    """Save cover.jpg to item directory and embed art into the audio file."""
    path = item.path.decode() if isinstance(item.path, bytes) else item.path
    ext  = os.path.splitext(path)[1].lower()

    # Write cover.jpg for Navidrome / file managers
    cover_path = os.path.join(os.path.dirname(path), "cover.jpg")
    try:
        with open(cover_path, "wb") as f:
            f.write(img_data)
    except Exception as e:
        log.warning("artfix: could not write cover.jpg: {}", e)

    # Embed into audio file
    try:
        if ext == ".flac":
            from mutagen.flac import FLAC, Picture
            audio = FLAC(path)
            pic = Picture()
            pic.type = 3
            pic.mime = "image/jpeg"
            pic.data = img_data
            audio.clear_pictures()
            audio.add_picture(pic)
            audio.save()
        elif ext == ".mp3":
            from mutagen.id3 import ID3, APIC
            try:
                audio = ID3(path)
            except Exception:
                from mutagen.id3 import ID3NoHeaderError
                audio = ID3()
            audio.delall("APIC")
            audio.add(APIC(encoding=3, mime="image/jpeg",
                           type=3, desc="Cover", data=img_data))
            audio.save(path)
        else:
            log.warning("artfix: unsupported format {} — cover.jpg written only", ext)
            return True  # cover.jpg still useful for Navidrome
        log.info("artfix: embedded {} bytes", len(img_data))
        return True
    except Exception as e:
        log.warning("artfix: embed failed: {}", e)
        return False


def _apply_art(item, artist, album_name, album_data, mb_albumid, log):
    """Fetch and embed cover art if the file doesn't already have it."""
    path = item.path.decode() if isinstance(item.path, bytes) else item.path
    if _has_embedded_art(path):
        return False
    img, src = _fetch_art_url(artist, album_name, album_data, mb_albumid)
    if not img:
        log.info("artfix: no art found for {} - {}", artist, album_name or item.title)
        return False
    log.info("artfix: {} -> {} - {}", src, artist, album_name or item.title)
    return _embed_art(item, img, log)


# ── Metadata ──────────────────────────────────────────────────────────────────

def _apply_deezer(item, log):
    """Look up item on Deezer, fill missing fields, fetch art. Returns True if changed."""
    artist    = _safe(item, "artist") or _safe(item, "albumartist")
    raw_title = _safe(item, "title")
    clean_title = _strip_artist_prefix(_clean_promo(raw_title), artist)

    if not clean_title:
        return False

    changed = False

    # Normalize yt-dlp YYYYMMDD upload dates stored as year
    cur_year = _safe(item, "year", 0)
    if isinstance(cur_year, int) and cur_year > 9999:
        item.year = cur_year // 10000
        log.info("deezerfix: year {} -> {}", cur_year, item.year)
        changed = True

    track = _deezer_search(artist, clean_title)
    if not track:
        log.info("deezerfix: {} - {}: not found on Deezer", artist, clean_title)
        if changed:
            item.try_write()
            item.store()
        return changed

    album_data = _deezer_album(track["album"]["id"])

    # Clean title
    if clean_title != raw_title:
        log.info("deezerfix: title {!r} -> {!r}", raw_title, clean_title)
        item.title = clean_title
        changed = True

    # Album
    if not _safe(item, "album") and album_data:
        item.album = album_data.get("title") or track["album"]["title"]
        log.info("deezerfix: album -> {}", item.album)
        changed = True

    # Year
    cur_year = _safe(item, "year", 0)
    if (not cur_year or cur_year == 0) and album_data:
        rd = album_data.get("release_date", "")
        if rd and len(rd) >= 4:
            item.year = int(rd[:4])
            log.info("deezerfix: year -> {}", item.year)
            changed = True

    # Genre — overwrite only garbage values
    cur_genre = _safe(item, "genre")
    bad_genre = not cur_genre or cur_genre.lower() in ("artist", "music", "other", "")
    if bad_genre and album_data:
        genres = album_data.get("genres", {}).get("data", [])
        if genres:
            item.genre = genres[0]["name"]
            log.info("deezerfix: genre -> {}", item.genre)
            changed = True

    # Artist casing fix (e.g. "Tyler Icu" -> "Tyler ICU")
    deezer_artist = track.get("artist", {}).get("name", "")
    cur_artist = _safe(item, "artist")
    if (deezer_artist and cur_artist
            and deezer_artist.lower() == cur_artist.lower()
            and deezer_artist != cur_artist):
        log.info("deezerfix: artist {!r} -> {!r}", cur_artist, deezer_artist)
        item.artist = deezer_artist
        cur_aa = _safe(item, "albumartist")
        if cur_aa and cur_aa.lower() == deezer_artist.lower():
            item.albumartist = deezer_artist
        changed = True

    # Cover art
    album_name = _safe(item, "album") or (album_data.get("title") if album_data else "")
    mb_albumid = _safe(item, "mb_albumid")
    if _apply_art(item, artist, album_name, album_data, mb_albumid, log):
        changed = True

    if changed:
        item.try_write()
        item.store()
        try:
            lib = item._db
            album_obj = lib.get_album(item.album_id) if item.album_id else None
            if album_obj:
                album_changed = False
                if not (album_obj.year or 0) and (item.year or 0):
                    album_obj.year = item.year
                    album_changed = True
                if not album_obj.album and item.album:
                    album_obj.album = item.album
                    album_changed = True
                if item.albumartist and album_obj.albumartist != item.albumartist:
                    album_obj.albumartist = item.albumartist
                    album_changed = True
                if album_changed:
                    album_obj.store()
        except Exception:
            pass

    return changed


class DeezerMetaPlugin(BeetsPlugin):
    name = "deezer_meta"

    def __init__(self):
        super().__init__()
        self.register_listener("album_imported", self._on_album_imported)
        self.register_listener("item_imported",  self._on_item_imported)

    def _on_album_imported(self, lib, album):
        if _safe(album, "mb_albumid"):
            return
        for item in album.items():
            _apply_deezer(item, self._log)

    def _on_item_imported(self, lib, item):
        if not _safe(item, "mb_trackid"):
            _apply_deezer(item, self._log)

    def commands(self):
        fix = ui.Subcommand("deezerfix",
                            help="Fill missing metadata (album/year/genre) from Deezer")
        fix.func = self._cmd_deezerfix

        art = ui.Subcommand("artfix",
                            help="Fetch and embed missing cover art (Deezer → iTunes → MusicBrainz)")
        art.func = self._cmd_artfix

        clean = ui.Subcommand("promoclean",
                              help="Strip DJ-promo artifacts from track titles")
        clean.func = self._cmd_promoclean

        return [fix, art, clean]

    def _cmd_deezerfix(self, lib, opts, args):
        if args:
            items = list(lib.items(ui.decargs(args)))
        else:
            by_album = list(lib.items("album::^$"))
            by_year  = list(lib.items("year:0"))
            seen, items = set(), []
            for it in by_album + by_year:
                if it.id not in seen:
                    seen.add(it.id)
                    items.append(it)

        self._log.info("deezerfix: checking {} items", len(items))
        fixed = sum(1 for it in items if _apply_deezer(it, self._log))
        self._log.info("deezerfix: updated {}/{} items", fixed, len(items))

    def _cmd_artfix(self, lib, opts, args):
        items = list(lib.items(ui.decargs(args))) if args else list(lib.items())
        self._log.info("artfix: checking {} items", len(items))
        fixed = 0
        for item in items:
            artist     = _safe(item, "artist") or _safe(item, "albumartist")
            album_name = _safe(item, "album")
            mb_albumid = _safe(item, "mb_albumid")
            # Try to get Deezer album data for richer art search
            title      = _clean_promo(_safe(item, "title"))
            track      = _deezer_search(artist, title) if artist and title else None
            album_data = _deezer_album(track["album"]["id"]) if track else None
            if _apply_art(item, artist, album_name, album_data, mb_albumid, self._log):
                item.try_write()
                item.store()
                fixed += 1
        self._log.info("artfix: embedded art for {}/{} items", fixed, len(items))

    def _cmd_promoclean(self, lib, opts, args):
        items = list(lib.items(ui.decargs(args))) if args else list(lib.items())
        changed = 0
        for item in items:
            clean = _clean_promo(item.title or "")
            if clean != (item.title or ""):
                self._log.info("promoclean: {!r} -> {!r}", item.title, clean)
                item.title = clean
                item.try_write()
                item.store()
                changed += 1
        self._log.info("promoclean: cleaned {} titles", changed)
