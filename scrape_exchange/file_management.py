'''
Manages files produced by scraping tools in a base directory and its
'uploaded' subdirectory.

Responsibilities:
- Ensure the 'uploaded' subdirectory exists.
- Detect when a file in the base directory has already been superseded by a
  newer copy in 'uploaded' and delete the stale base-directory copy.
- Detect when a lower-ranked variant of a file (e.g. video-min-*) can be
  removed because a higher-ranked variant (e.g. video-dlp-*) for the same
  content identifier exists and is more recent.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''

import logging

import aiofiles
import aiofiles.os
import brotli
import orjson

from collections.abc import Iterator
from pathlib import Path

VIDEO_FILE_PREFIX: str = 'video-'
CHANNEL_FILE_PREFIX: str = 'channel-'

logger = logging.getLogger(__name__)

# Default prefix rankings per content-type group.
# Within each group the list is ordered from *least* preferred (index 0) to
# *most* preferred (last index).  Scrapers may supply their own rankings when
# constructing AssetFileManagement.
DEFAULT_PREFIX_RANKINGS: dict[str, list[str]] = {
    'video': ['video-min-', 'video-dlp-'],
    'channel': ['channel-'],
}

# Suffixes that mark a file as a status indicator (failure, not-found,
# unresolved, unavailable).  Marker files always have the lowest priority:
# writing a marker file never causes another file to be deleted, and the
# regular write/cleanup logic does not consider marker files as variants of
# any other file.
MARKER_SUFFIXES: tuple[str, ...] = (
    '.failed', '.not_found', '.unresolved', '.unavailable',
)


class AssetFileManagement:
    '''
    Manages scraped-asset files stored under a base directory.

    :param base_dir: Root directory where scraping tools write their output.
        A subdirectory named 'uploaded' is created here automatically.
    :param prefix_rankings: Optional mapping of content-type group names to
        an ordered list of filename prefixes, from least preferred to most
        preferred.  When omitted the module-level DEFAULT_PREFIX_RANKINGS are
        used.
    '''

    def __init__(
        self,
        base_dir: str,
        prefix_rankings: dict[str, list[str]] | None = None,
    ) -> None:
        self.base_dir: Path = Path(base_dir)
        self.uploaded_dir: Path = self.base_dir / 'uploaded'
        self.uploaded_dir.mkdir(parents=True, exist_ok=True)

        self.prefix_rankings: dict[str, list[str]] = (
            prefix_rankings
            if prefix_rankings is not None
            else DEFAULT_PREFIX_RANKINGS
        )

        # Flat mapping: prefix -> (group_name, rank_index)
        self._prefix_info: dict[str, tuple[str, int]] = {}
        for group, prefixes in self.prefix_rankings.items():
            for rank, prefix in enumerate(prefixes):
                self._prefix_info[prefix] = (group, rank)

        # Sort prefixes longest-first so that the most specific match wins
        # when prefixes share a common leading substring.
        self._sorted_prefixes: list[str] = sorted(
            self._prefix_info, key=len, reverse=True
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_filename(
        self, filename: str
    ) -> tuple[str | None, str | None, str | None]:
        '''
        Decompose *filename* into (group, prefix, identifier).

        The identifier is the portion of the filename that follows the prefix
        and is shared across all ranked variants of the same content item
        (e.g. the video ID plus extension).

        Returns ``(None, None, None)`` when no known prefix matches.
        '''
        for prefix in self._sorted_prefixes:
            if filename.startswith(prefix):
                group, _ = self._prefix_info[prefix]
                identifier: str = filename[len(prefix):]
                return group, prefix, identifier
        return None, None, None

    @staticmethod
    def is_marker(filename: str) -> bool:
        '''
        Return ``True`` if *filename* ends with one of the recognized
        :data:`MARKER_SUFFIXES`.

        Marker files (``.failed``, ``.not_found``, ``.unresolved``) always
        have the lowest priority: they are never auto-deleted by the cleanup
        logic, and writing one never causes another file to be deleted.
        '''
        return any(filename.endswith(suffix) for suffix in MARKER_SUFFIXES)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def sync_with_uploaded(self, filename: str) -> bool:
        '''
        Check whether *filename* in the base directory has been superseded by
        a same-named copy in the uploaded directory.

        If the uploaded copy exists and its modification time is strictly more
        recent than the base-directory copy, the base-directory copy is
        deleted.

        :param filename: Bare filename (no directory component) to inspect.
        :returns: ``True`` if the base-directory file was deleted,
            ``False`` otherwise.
        '''
        base_file: Path = self.base_dir / filename
        uploaded_file: Path = self.uploaded_dir / filename

        if not base_file.exists():
            return False

        if not uploaded_file.exists():
            return False

        try:
            base_mtime: float = base_file.stat().st_mtime
            uploaded_mtime: float = uploaded_file.stat().st_mtime
        except OSError as exc:
            logger.warning(
                'Could not stat files for sync check',
                exc=exc,
                extra={'filename': filename},
            )
            return False

        if uploaded_mtime > base_mtime:
            try:
                base_file.unlink()
                logger.debug(
                    'Deleted base file: uploaded copy is more recent',
                    extra={
                        'base_file': base_file,
                        'uploaded_file': uploaded_file,
                    },
                )
                return True
            except OSError as exc:
                logger.warning(
                    'Failed to delete base file',
                    exc=exc,
                    extra={'base_file': base_file},
                )

        return False

    def is_superseded(self, filename: str) -> bool:
        '''
        Return True if *filename* in the base directory has been superseded
        by a same-or-higher-ranked variant in the uploaded directory.

        A file is considered superseded when **either** condition holds:

        1. The same filename exists in :attr:`uploaded_dir` with a
           modification time greater than or equal to the base copy's mtime.
        2. A higher-ranked prefix variant of the same content identifier
           exists in :attr:`uploaded_dir` with a modification time greater
           than or equal to the base copy's mtime.

        Equal mtimes are treated as superseded — i.e. ties go to the
        higher-ranked / uploaded variant.  Higher-ranked variants that live
        in :attr:`base_dir` (rather than ``uploaded/``) are *not* considered
        here; for that case use :meth:`cleanup_lower_ranked`.

        :param filename: Bare filename to test.  If it does not exist in
            :attr:`base_dir`, returns ``False``.
        :returns: ``True`` if a superseding variant was found, ``False``
            otherwise.
        '''
        base_path: Path = self.base_dir / filename
        try:
            base_mtime: float = base_path.stat().st_mtime
        except OSError:
            return False

        # Check the same-name uploaded copy first.
        same_uploaded: Path = self.uploaded_dir / filename
        if same_uploaded.exists():
            try:
                if same_uploaded.stat().st_mtime >= base_mtime:
                    return True
            except OSError:
                pass

        group, prefix, identifier = self._parse_filename(filename)
        if group is None or prefix is None or identifier is None:
            return False

        current_rank: int = self._prefix_info[prefix][1]
        for p in self.prefix_rankings[group]:
            rank: int = self._prefix_info[p][1]
            if rank <= current_rank:
                continue
            candidate: Path = self.uploaded_dir / f'{p}{identifier}'
            try:
                if candidate.stat().st_mtime >= base_mtime:
                    return True
            except OSError:
                continue

        return False

    def cleanup_lower_ranked(self, filename: str) -> list[str]:
        '''
        Remove lower-ranked prefix variants of the same content identifier
        when a higher-ranked variant exists and is more recent.

        Given *filename*, the method determines its prefix group and
        identifier, then searches both the base and uploaded directories for
        all variants (all prefixes in the same group, same identifier).  Any
        variant whose prefix rank is lower than the highest-ranked variant
        found *and* whose modification time is older than that highest-ranked
        variant is deleted.

        Example: if ``video-dlp-ABC.json.br`` exists and is newer than
        ``video-min-ABC.json.br``, the ``video-min`` copy is deleted
        regardless of which directory each lives in.

        :param filename: Bare filename used to determine the content group and
            identifier.  Need not be a file that currently exists on disk.
        :returns: List of absolute paths (as strings) of files that were
            deleted.
        '''

        group: str | None
        _: str | None
        identifier: str | None

        group, _, identifier = self._parse_filename(filename)
        if group is None or identifier is None:
            return []

        group_prefixes: list[str] = self.prefix_rankings[group]

        # Collect every existing variant: (prefix, dir_label) -> (Path, mtime)
        found: dict[tuple[str, str], tuple[Path, float]] = {}
        for p in group_prefixes:
            candidate_name: str = p + identifier
            for dir_label, directory in (
                ('base', self.base_dir),
                ('uploaded', self.uploaded_dir),
            ):
                candidate: Path = directory / candidate_name
                if candidate.exists():
                    try:
                        mtime: float = candidate.stat().st_mtime
                        found[(p, dir_label)] = (candidate, mtime)
                    except OSError as exc:
                        logger.warning(
                            'Could not stat candidate',
                            exc=exc,
                            extra={'candidate': candidate},
                        )

        if not found:
            return []

        # Determine the highest rank present and its most recent mtime.
        best_rank: int = -1
        best_mtime: float = 0.0
        for p in group_prefixes:
            rank: int = self._prefix_info[p][1]
            for dir_label in ('base', 'uploaded'):
                key = (p, dir_label)
                if key not in found:
                    continue
                _, mtime = found[key]
                if (rank > best_rank
                        or (rank == best_rank and mtime > best_mtime)):
                    best_rank = rank
                    best_mtime = mtime

        deleted: list[str] = []
        for p in group_prefixes:
            rank = self._prefix_info[p][1]
            if rank >= best_rank:
                # Same or higher rank — never delete these.
                continue
            for dir_label in ('base', 'uploaded'):
                key = (p, dir_label)
                if key not in found:
                    continue
                candidate, mtime = found[key]
                if mtime < best_mtime:
                    try:
                        candidate.unlink()
                        deleted.append(str(candidate))
                        logger.debug(
                            'Deleted lower-ranked file because '
                            'higher-ranked variant exists',
                            extra={
                                'candidate': candidate,
                                'rank': rank,
                                'best_rank': best_rank,
                                'best_mtime': best_mtime,
                            },
                        )
                    except OSError as exc:
                        logger.warning(
                            'Failed to delete lower-ranked file',
                            exc=exc,
                            extra={'candidate': candidate},
                        )

        return deleted

    def list_base(self, prefix: str | None = None,
                  suffix: str | None = None) -> list[str]:
        '''
        Return the bare filenames of regular files inside the base directory,
        optionally filtered by *prefix* and/or *suffix*.

        :param prefix: If given, only filenames starting with this string are
            returned.
        :param suffix: If given, only filenames ending with this string are
            returned.
        :returns: List of bare filenames (no directory component).  Order is
            unspecified.
        '''
        return self._list_dir(self.base_dir, prefix, suffix)

    def list_uploaded(self, prefix: str | None = None,
                      suffix: str | None = None) -> list[str]:
        '''
        Return the bare filenames of regular files inside the uploaded
        directory, optionally filtered by *prefix* and/or *suffix*.

        :param prefix: If given, only filenames starting with this string are
            returned.
        :param suffix: If given, only filenames ending with this string are
            returned.
        :returns: List of bare filenames (no directory component).  Order is
            unspecified.
        '''
        return self._list_dir(self.uploaded_dir, prefix, suffix)

    def iter_assets(
        self, group: str, suffix: str = '.json.br',
    ) -> Iterator[tuple[str, bool, float]]:
        '''
        Yield every asset file belonging to *group* under the base and
        uploaded directories.

        Each yielded tuple is ``(identifier, is_uploaded, mtime)``:

        * ``identifier`` — the bare content identifier with the matched
          group prefix and *suffix* stripped (e.g. for
          ``video-min-ABC.json.br`` the identifier is ``ABC``).
        * ``is_uploaded`` — ``True`` if the file lives in
          :attr:`uploaded_dir`, ``False`` if it lives in :attr:`base_dir`.
        * ``mtime`` — modification time as epoch seconds.

        Files whose name does not end with *suffix*, files belonging to a
        different group, and marker files are skipped.

        :param group: Prefix-group name (key of :attr:`prefix_rankings`).
        :param suffix: File suffix to require; defaults to ``'.json.br'``.
        :raises ValueError: If *group* is not a known prefix group.
        '''
        if group not in self.prefix_rankings:
            raise ValueError(f'Unknown asset group: {group}')

        for is_uploaded, directory in (
            (False, self.base_dir),
            (True, self.uploaded_dir),
        ):
            for entry in directory.iterdir():
                parsed = self._parse_asset_entry(entry, group, suffix)
                if parsed is None:
                    continue
                identifier, mtime = parsed
                yield identifier, is_uploaded, mtime

    def _parse_asset_entry(
        self, entry: Path, group: str, suffix: str,
    ) -> tuple[str, float] | None:
        '''
        Validate *entry* as a member of *group* with the given *suffix* and
        return ``(bare_identifier, mtime)`` if so, otherwise ``None``.
        '''
        if not entry.is_file() or not entry.name.endswith(suffix):
            return None
        parsed_group, _, identifier = self._parse_filename(entry.name)
        if parsed_group != group or identifier is None:
            return None
        try:
            mtime: float = entry.stat().st_mtime
        except OSError as exc:
            logger.warning(
                'Could not stat entry',
                exc=exc,
                extra={'entry': entry},
            )
            return None
        return identifier[: -len(suffix)], mtime

    @staticmethod
    def _list_dir(directory: Path, prefix: str | None,
                  suffix: str | None) -> list[str]:
        return [
            entry.name for entry in directory.iterdir()
            if entry.is_file()
            and (prefix is None or entry.name.startswith(prefix))
            and (suffix is None or entry.name.endswith(suffix))
        ]

    async def read_file(self, filename: str) -> dict:
        '''
        Read a brotli-compressed JSON file from the base directory.

        :param filename: Bare filename to read (must exist in base_dir).
        :returns: Deserialised content as a dict.
        :raises FileNotFoundError: If the file does not exist in base_dir.
        :raises brotli.error: If decompression fails.
        :raises orjson.JSONDecodeError: If the decompressed content is not
            valid JSON.
        '''
        return await self._read_path(self.base_dir / filename)

    async def read_uploaded(self, filename: str) -> dict:
        '''
        Read a brotli-compressed JSON file from the uploaded directory.

        Symmetric with :meth:`read_file` for files that have already been
        moved into ``uploaded/`` (e.g. via :meth:`mark_uploaded`).

        :param filename: Bare filename to read (must exist in uploaded_dir).
        :returns: Deserialised content as a dict.
        :raises FileNotFoundError: If the file does not exist in
            uploaded_dir.
        :raises brotli.error: If decompression fails.
        :raises orjson.JSONDecodeError: If the decompressed content is not
            valid JSON.
        '''
        return await self._read_path(self.uploaded_dir / filename)

    @staticmethod
    async def _read_path(path: Path) -> dict:
        async with aiofiles.open(path, 'rb') as f:
            data: bytes = await f.read()
        return orjson.loads(brotli.decompress(data))

    async def write_file(self, filename: str, data: dict) -> None:
        '''
        Serialise *data* to JSON, compress with brotli, and write the result
        to *filename* inside the base directory.  After writing, any file in
        the base or uploaded directory that has the same content identifier but
        a lower-ranked prefix, or the same prefix in the uploaded directory,
        is deleted if its modification time is older than the file just
        written.

        :param filename: Bare filename to write (will be created or
            overwritten).
        :param data: Dict to serialise and store.
        :raises OSError: If the file cannot be written.
        '''
        path: Path = self.base_dir / filename
        async with aiofiles.open(path, 'wb') as f:
            await f.write(brotli.compress(
                orjson.dumps(data, option=orjson.OPT_INDENT_2),
                quality=11,
                mode=brotli.MODE_TEXT,
            ))
        if self.is_marker(filename):
            # Marker files (.failed/.not_found/.unresolved) have the lowest
            # priority and must never trigger deletion of other files.
            return
        await self._cleanup_stale_files(filename)

    async def _cleanup_stale_files(self, filename: str) -> None:
        '''
        After writing *filename* to the base directory, remove any file in
        either the base or uploaded directory whose modification time is older
        than the file just written and whose priority is lower than or equal to
        that of *filename*.

        Equal-priority means the same prefix (same filename) in the uploaded
        directory.  Lower-priority means a lower-ranked prefix within the same
        content-type group for the same identifier.
        '''
        written_path: Path = self.base_dir / filename
        try:
            written_mtime: float = (
                await aiofiles.os.stat(written_path)
            ).st_mtime
        except OSError as exc:
            logger.warning(
                'Could not stat written path after write',
                exc=exc,
                extra={'written_path': written_path},
            )
            return

        group, prefix, identifier = self._parse_filename(filename)

        if group is None or identifier is None:
            # No known prefix group — only handle the equal-priority copy in
            # the uploaded directory (same filename, different location).
            await self._delete_if_older(self.uploaded_dir / filename,
                                        written_mtime)
            return

        current_rank: int = self._prefix_info[prefix][1]

        for p in self.prefix_rankings[group]:
            rank: int = self._prefix_info[p][1]
            candidate_name: str = p + identifier
            for directory in (self.base_dir, self.uploaded_dir):
                candidate: Path = directory / candidate_name
                if candidate == written_path:
                    continue
                if rank < current_rank or (
                    rank == current_rank and directory == self.uploaded_dir
                ):
                    await self._delete_if_older(candidate, written_mtime)

    async def _delete_if_older(self, path: Path, reference_mtime: float
                               ) -> None:
        '''Delete *path* if it exists and its mtime is older than
        *reference_mtime*.'''
        try:
            mtime: float = (await aiofiles.os.stat(path)).st_mtime
        except OSError:
            return
        if mtime < reference_mtime:
            try:
                await aiofiles.os.remove(path)
                logger.debug(
                    'Deleted stale file',
                    extra={
                        'path': path,
                        'mtime': mtime,
                        'reference_mtime': reference_mtime,
                    },
                )
            except OSError as exc:
                logger.warning(
                    'Failed to delete stale file',
                    exc=exc,
                    extra={'path': path},
                )

    # ------------------------------------------------------------------
    # Status / lifecycle helpers
    # ------------------------------------------------------------------

    async def delete(self, filename: str, fail_ok: bool = True) -> None:
        '''
        Delete *filename* from the base directory.

        :param filename: Bare filename (no directory component) inside
            ``base_dir`` to remove.
        :param fail_ok: When ``True`` (the default), any ``OSError`` raised
            by the underlying ``unlink`` is propagated to the caller.  When
            ``False``, any ``OSError`` (including the file not existing,
            permission errors, etc.) is caught and logged at warning level
            and the call returns normally.
        :raises OSError: If the underlying ``unlink`` fails and *fail_ok* is
            ``True``.
        '''
        await self._delete_path(self.base_dir / filename, fail_ok)

    async def delete_uploaded(self, filename: str,
                              fail_ok: bool = True) -> None:
        '''
        Delete *filename* from the uploaded directory.

        Symmetric with :meth:`delete` for files that live in ``uploaded/``.

        :param filename: Bare filename (no directory component) inside
            ``uploaded_dir`` to remove.
        :param fail_ok: When ``True`` (the default), any ``OSError`` raised
            by the underlying ``unlink`` is propagated to the caller.  When
            ``False``, any ``OSError`` is caught, logged at warning level,
            and the call returns normally.
        :raises OSError: If the underlying ``unlink`` fails and *fail_ok* is
            ``True``.
        '''
        await self._delete_path(self.uploaded_dir / filename, fail_ok)

    @staticmethod
    async def _delete_path(path: Path, fail_ok: bool) -> None:
        try:
            await aiofiles.os.remove(path)
            logger.debug('Deleted path', extra={'path': path})
        except OSError as exc:
            if fail_ok:
                raise
            logger.warning(
                'Failed to delete path; ignoring',
                exc=exc,
                extra={
                    'path': path,
                    'exc_type': type(exc).__name__,
                },
            )

    async def mark_uploaded(self, filename: str) -> Path:
        '''
        Move *filename* from the base directory into the uploaded directory.

        :param filename: Bare filename (no directory component) inside
            ``base_dir`` to move.
        :returns: The new path inside the uploaded directory.
        :raises OSError: If the rename fails.
        '''
        src: Path = self.base_dir / filename
        dst: Path = self.uploaded_dir / filename
        await aiofiles.os.rename(src, dst)
        logger.debug(
            'Marked src as uploaded',
            extra={'src': src, 'dst': dst},
        )
        return dst

    async def mark_failed(self, filename: str) -> str:
        '''
        Rename *filename* in the base directory by appending ``.failed`` so
        future runs can recognize it as a previously failed upload.

        :param filename: Bare filename inside ``base_dir`` to mark.
        :returns: The new filename (``{filename}.failed``).
        :raises OSError: If the rename fails.
        '''
        return await self._rename_with_suffix(filename, '.failed')

    async def mark_unavailable(self, filename: str) -> str:
        '''
        Rename *filename* in the base directory by appending ``.unavailable``
        so future runs can recognize it as a permanently unavailable item
        (e.g. a YouTube video that has been removed, made private, or is
        otherwise no longer accessible upstream).

        :param filename: Bare filename inside ``base_dir`` to mark.
        :returns: The new filename (``{filename}.unavailable``).
        :raises OSError: If the rename fails.
        '''
        return await self._rename_with_suffix(filename, '.unavailable')

    async def _rename_with_suffix(self, filename: str, suffix: str) -> str:
        new_name: str = f'{filename}{suffix}'
        src: Path = self.base_dir / filename
        dst: Path = self.base_dir / new_name
        await aiofiles.os.rename(src, dst)
        logger.debug(
            'Renamed file',
            extra={'src': src, 'dst': dst},
        )
        return new_name

    async def mark_not_found(self, name: str,
                             content: str | None = None) -> Path:
        '''
        Create a ``{name}.not_found`` marker file in the base directory.

        Used by scrapers to record that a content item could not be located
        upstream so future runs can skip it without re-attempting.

        :param name: Bare prefix+identifier (e.g. ``channel-FOO``).  The
            ``.not_found`` suffix is appended automatically.
        :param content: Optional text content to write into the marker file;
            when omitted, an empty file is created.
        :returns: The path of the marker file.
        '''
        return await self._touch_marker(f'{name}.not_found', content)

    async def mark_unresolved(self, name: str,
                              content: str | None = None) -> Path:
        '''
        Create a ``{name}.unresolved`` marker file in the base directory.

        Used by scrapers to record that an identifier (e.g. a YouTube channel
        ID) could not be resolved to a usable handle so future runs can skip
        it without re-attempting.

        :param name: Bare prefix+identifier (e.g. ``channel-UCxyz``).  The
            ``.unresolved`` suffix is appended automatically.
        :param content: Optional text content to write into the marker file;
            when omitted, an empty file is created.
        :returns: The path of the marker file.
        '''
        return await self._touch_marker(f'{name}.unresolved', content)

    def marker_path(self, name: str, suffix: str) -> Path:
        '''
        Return the base-directory path where a marker file for *name* with
        *suffix* would live. Does **not** create the file; callers who want
        to touch the marker should use :meth:`mark_not_found` or
        :meth:`mark_unresolved`.

        :param name: Bare prefix+identifier (e.g. ``channel-UCxyz``).
        :param suffix: Marker suffix including the leading dot (e.g.
            ``.unresolved``).
        :raises ValueError: If *suffix* is not a recognized marker suffix.
        :returns: The path the marker file would have on disk.
        '''
        filename: str = f'{name}{suffix}'
        if not self.is_marker(filename):
            raise ValueError(
                f'{suffix!r} is not a recognized marker suffix '
                f'(must be one of {MARKER_SUFFIXES})'
            )
        return self.base_dir / filename

    async def _touch_marker(self, marker_filename: str,
                            content: str | None) -> Path:
        '''
        Create a marker file in the base directory.  Marker files are subject
        to the rules described in :data:`MARKER_SUFFIXES`: writing them never
        triggers cleanup of other files.

        :raises ValueError: If *marker_filename* does not end with a
            recognized marker suffix.
        '''
        if not self.is_marker(marker_filename):
            raise ValueError(
                f'{marker_filename!r} is not a marker filename '
                f'(must end with one of {MARKER_SUFFIXES})'
            )
        path: Path = self.base_dir / marker_filename
        async with aiofiles.open(path, 'w') as f:
            if content is not None:
                await f.write(content)
        logger.debug('Touched marker file', extra={'path': path})
        return path
