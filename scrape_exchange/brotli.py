'''Shared brotli + JSON read/write helpers.

The Scrape Exchange persists most assets (channel files, video
files, sentinel records) as brotli-compressed JSON. This module
centralises the read/write contract so individual call sites don't
each reinvent compression options, atomic-write semantics, or
recovery from corrupted files.

Public API:

* :func:`brotli_write` -- JSON-encode a value, brotli-compress
  the bytes, write atomically to *path*.
* :func:`brotli_read` -- brotli-decompress *path* and parse the
  JSON. On :class:`brotli.error`, try to salvage the bytes
  (chunked decoder), then close any truncated JSON
  (brace/bracket balancing), and -- if recovery worked --
  atomically rewrite the file with the recovered value before
  returning it.

The recovery path is conservative: it returns *something* only
when at least one structural form of recovery parses. Unrelated
errors (file-not-found, OSError during write, JSONDecodeError on
a non-brotli error) propagate.
'''

from __future__ import annotations

import json
import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Any

import brotli
import orjson


_CHUNK: int = 4096

#: Default brotli quality. The codebase uses 9 throughout (vs
#: brotli's library default of 11) -- at our file scale (millions
#: of files) the wall-clock difference is roughly 3-5x in
#: compression speed with output size only 1-2% larger.
DEFAULT_QUALITY: int = 9


TMP_SUBDIR_NAME: str = '.tmp'
WORLD_READABLE_FILE_MODE: int = 0o644


def brotli_write(
    path: Path | str,
    data: Any,
    *,
    quality: int = DEFAULT_QUALITY,
    mode: int = brotli.MODE_TEXT,
    indent: int | None = 2,
    sort_keys: bool = True,
) -> None:
    '''Serialize *data* to JSON, compress, atomic-write to *path*.

    Uses orjson for serialization (fast path the codebase already
    standardised on) and writes via ``tempfile.mkstemp`` +
    ``Path.replace`` so readers never see a half-written file
    even under concurrent operator scripts.

    The temp file lives in ``<target.parent>/.tmp/`` rather than
    alongside *target* so that callers iterating the parent
    directory (asset listings, upload scans) never see in-flight
    temp files. ``.tmp/`` is a sub-directory of the target's
    parent, so the rename remains atomic.

    :param quality: brotli compression quality (0-11). Default 9
        matches the codebase's tuning.
    :param mode: brotli mode (``MODE_TEXT`` / ``MODE_GENERIC`` /
        ``MODE_FONT``). Default ``MODE_TEXT`` matches the existing
        scraper / channel-file writers.
    :param indent: pretty-print indent passed to orjson. ``2``
        (default) matches ``orjson.OPT_INDENT_2`` used by the live
        scrapers; pass ``None`` for compact (recovered/repaired)
        files.
    :param sort_keys: stable key ordering for diff-friendly output.
    '''
    target: Path = Path(path)
    option: int = 0
    if indent == 2:
        option |= orjson.OPT_INDENT_2
    elif indent is not None and indent != 0:
        raise ValueError(
            'orjson only supports indent=2 or indent=None/0; '
            f'got indent={indent}',
        )
    if sort_keys:
        option |= orjson.OPT_SORT_KEYS
    encoded: bytes = brotli.compress(
        orjson.dumps(data, option=option),
        quality=quality,
        mode=mode,
    )
    tmp_dir: Path = target.parent / TMP_SUBDIR_NAME
    tmp_dir.mkdir(exist_ok=True)
    fd: int
    tmp: str
    fd, tmp = tempfile.mkstemp(dir=str(tmp_dir))
    try:
        with open(fd, 'wb') as fh:
            fh.write(encoded)
        Path(tmp).replace(target)
        target.chmod(WORLD_READABLE_FILE_MODE)
    except Exception:
        Path(tmp).unlink(missing_ok=True)
        raise


def brotli_read(path: Path | str) -> Any:
    '''Decompress + parse a brotli-compressed JSON file.

    Fast path: full ``brotli.decompress`` + ``orjson.loads``. The
    file is left untouched and the parsed value is returned.

    Slow / recovery path (only when ``brotli.error`` is raised on
    the fast path):

    1. Run :func:`_best_effort_decompress` to salvage as many
       bytes as the chunked decoder will yield before hitting the
       corruption.
    2. Try :func:`_best_effort_json` -- handles trailing garbage
       after a complete JSON value.
    3. If that fails, try :func:`_close_truncated_json` --
       brace/bracket-balances the salvaged prefix and closes the
       structure (handles mid-write truncation).
    4. If anything recovered, rewrite the file via
       :func:`brotli_write` so subsequent readers take the fast
       path and the corrupted bytes don't persist on disk.

    Re-raises ``brotli.error`` only when no recovery strategy
    yielded a parseable value.
    '''
    target: Path = Path(path)
    raw: bytes = target.read_bytes()
    try:
        return orjson.loads(brotli.decompress(raw))
    except brotli.error as exc:
        salvaged: bytes = _best_effort_decompress(raw)
        recovered: Any = _best_effort_json(salvaged)
        if recovered is None:
            text: str = salvaged.decode(
                'utf-8', errors='replace',
            )
            recovered = _close_truncated_json(text)
        if recovered is None:
            logging.error(
                'brotli decompression failed and no JSON '
                'could be recovered from the salvaged bytes',
                extra={
                    'path': str(target),
                    'salvaged_bytes': len(salvaged),
                },
            )
            raise
        logging.warning(
            'Recovered corrupted brotli file; rewriting '
            'compressed copy in place',
            extra={
                'path': str(target),
                'salvaged_bytes': len(salvaged),
                'brotli_error': str(exc),
            },
        )
        brotli_write(target, recovered)
        return recovered


async def brotli_write_async(
    path: Path | str,
    data: Any,
    *,
    quality: int = DEFAULT_QUALITY,
    mode: int = brotli.MODE_TEXT,
    indent: int | None = 2,
    sort_keys: bool = True,
) -> None:
    '''Async wrapper for :func:`brotli_write`.

    Runs the compress + atomic-write on a worker thread so the
    caller's event loop is not blocked while a multi-MB channel
    file compresses.
    '''
    await asyncio.to_thread(
        brotli_write,
        path,
        data,
        quality=quality,
        mode=mode,
        indent=indent,
        sort_keys=sort_keys,
    )


async def brotli_read_async(path: Path | str) -> Any:
    '''Async wrapper for :func:`brotli_read`.

    Runs decompress + parse (and any recovery + rewrite) on a
    worker thread so the caller's event loop is not blocked.
    '''
    return await asyncio.to_thread(brotli_read, path)


def _best_effort_decompress(
    raw: bytes, chunk_size: int = _CHUNK,
) -> bytes:
    '''Return as many decompressed bytes as can be salvaged.

    Tries a one-shot ``brotli.decompress`` first. On failure,
    feeds the input through ``brotli.Decompressor`` in
    ``chunk_size``-byte slices and keeps everything emitted
    before the first ``brotli.error``.

    Smaller ``chunk_size`` values salvage more bytes (the failing
    chunk is dropped, so a 1-byte chunk loses 1 byte vs ``_CHUNK``
    losing the whole 4 KiB) at the cost of more decoder calls. Use
    the default for most cases; pass ``chunk_size=1`` for
    deep-salvage retries on files where bigger chunks emitted
    nothing.
    '''
    try:
        return brotli.decompress(raw)
    except brotli.error:
        pass
    dec: brotli.Decompressor = brotli.Decompressor()
    out: bytearray = bytearray()
    for i in range(0, len(raw), chunk_size):
        try:
            out.extend(
                dec.process(raw[i:i + chunk_size]),
            )
        except brotli.error:
            break
    return bytes(out)


def _best_effort_json(data: bytes) -> Any:
    '''Extract a JSON value from possibly-truncated bytes.

    Strategy: full ``json.loads`` -> ``raw_decode`` (tolerates
    trailing garbage) -> progressive truncation at the last
    ``}`` until something parses or no candidates remain.
    Returns ``None`` if nothing parses.
    '''
    if not data:
        return None
    text: str = data.decode('utf-8', errors='replace')
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    try:
        obj, _ = json.JSONDecoder().raw_decode(text)
        return obj
    except json.JSONDecodeError:
        pass
    last: int = text.rfind('}')
    while last > 0:
        try:
            return json.loads(text[: last + 1])
        except json.JSONDecodeError:
            pass
        last = text.rfind('}', 0, last)
    return None


def _close_truncated_json(text: str) -> Any:
    '''Best-effort close of a truncated JSON object.

    Walks the text tracking string/escape state and the brace/
    bracket stack, recording a ``safe_pos`` boundary every time
    a value-completing comma or a closing bracket is reached,
    plus the depth of the open-stack at that boundary. At EOF
    the prefix up to ``safe_pos`` is appended with the matching
    closing characters and parsed.

    Catches the common mid-write truncation case
    :func:`_best_effort_json` misses (no closing ``}`` survives
    in the salvaged bytes).
    '''
    if not text.startswith('{'):
        return None
    stack: list[str] = []
    in_string: bool = False
    escaped: bool = False
    expecting_value: bool = False
    safe_pos: int = 0
    safe_stack: list[str] = []
    for i, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == '"':
                in_string = False
                if expecting_value:
                    safe_pos = i + 1
                    safe_stack = list(stack)
                    expecting_value = False
        elif ch == '"':
            in_string = True
        elif ch == ':':
            expecting_value = True
        elif ch == '{':
            stack.append('{')
            expecting_value = False
        elif ch == '[':
            stack.append('[')
            expecting_value = True
        elif ch == '}':
            if not stack or stack[-1] != '{':
                return None
            stack.pop()
            safe_pos = i + 1
            safe_stack = list(stack)
            expecting_value = False
        elif ch == ']':
            if not stack or stack[-1] != '[':
                return None
            stack.pop()
            safe_pos = i + 1
            safe_stack = list(stack)
            expecting_value = False
        elif ch == ',':
            safe_pos = i
            safe_stack = list(stack)
            expecting_value = (
                bool(stack) and stack[-1] == '['
            )
    closer: str = ''.join(
        '}' if b == '{' else ']' for b in reversed(safe_stack)
    )
    try:
        obj: object = json.loads(text[:safe_pos] + closer)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None
