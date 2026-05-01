"""
deezer_meta — beets plugin
Fills missing metadata (album, year, genre, clean title) from the Deezer API.

Commands:
  beet deezerfix [query]   Fill missing album/year/genre from Deezer
  beet promoclean [query]  Strip DJ-promo artifacts from titles (BPM, Main, Clean…)

Auto-hook: runs on every as-is import (no MusicBrainz ID).
"""
import re, json, urllib.request, urllib.parse

from beets.plugins import BeetsPlugin
from beets import ui

_API = "https://api.deezer.com"
_UA  = "beets-deezer-meta/1.0"

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
]

_COLLAB_RE = re.compile(
    r'\s+(ft\.?|feat\.?|featuring|x\s+|vs\.?)\s+.*$', re.I)


def _clean_promo(title):
    for pat in _PROMO_RE:
        title = pat.sub('', title).strip()
    return title


def _main_artist(artist):
    return _COLLAB_RE.sub('', artist).strip()


def _http(url):
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=12) as r:
        return json.loads(r.read())


def _deezer_search(artist, title):
    """Progressive search: full artist → main artist → title only."""
    main = _main_artist(artist)
    attempts = []
    if artist:
        attempts.append('artist:"{}" track:"{}"'.format(artist, title))
    if main and main.lower() != artist.lower():
        attempts.append('artist:"{}" track:"{}"'.format(main, title))
    attempts.append('track:"{}"'.format(title))

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


def _apply_deezer(item, log):
    """Look up item on Deezer and fill missing fields. Returns True if changed."""
    artist    = _safe(item, "artist") or _safe(item, "albumartist")
    raw_title = _safe(item, "title")
    clean_title = _clean_promo(raw_title)

    if not clean_title:
        return False

    track = _deezer_search(artist, clean_title)
    if not track:
        log.info("deezerfix: {} - {}: not found on Deezer", artist, clean_title)
        return False

    album_data = _deezer_album(track["album"]["id"])
    changed = False

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

    if changed:
        item.try_write()
        item.store()
        # Also sync the Album record so beet move / path templates use correct values
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

        clean = ui.Subcommand("promoclean",
                              help="Strip DJ-promo artifacts from track titles")
        clean.func = self._cmd_promoclean

        return [fix, clean]

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
