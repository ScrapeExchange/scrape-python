# Channel Map Redis Backend

Move the channel_map (channel_id to channel_handle mapping) from a
shared CSV file to Redis when `REDIS_DSN` is configured, eliminating
cross-process race conditions and enabling consistent reads across
hosts.

## Problem

The channel map is stored in `channel_map.csv`. Both the channel
scraper and the RSS scraper append to this file concurrently from
multiple worker processes with no cross-process locking. The channel
scraper uses an `asyncio.Lock` (intra-process only), while the RSS
scraper has no locking at all. This causes interleaved writes and
potential data loss when `channel_cleanup.py` rewrites the file while
scrapers are running.

## Solution

Introduce a `ChannelMap` abstraction with `FileChannelMap` and
`RedisChannelMap` backends, following the existing `VideoClaim` and
`CreatorQueue` pattern. The file-based path remains fully functional
for users without Redis.

## Interface

New file: `scrape_exchange/channel_map.py`

```python
class ChannelMap(ABC):
    async def get(self, channel_id: str) -> str | None
    async def get_all(self) -> dict[str, str]
    async def put(self, channel_id: str, handle: str) -> None
    async def put_many(self, mapping: dict[str, str]) -> None
    async def contains(self, channel_id: str) -> bool
    async def size(self) -> int
```

### FileChannelMap

Wraps the existing CSV read/append logic. Uses `asyncio.Lock` for
intra-process write safety (unchanged behavior for non-Redis users).

- `get_all()`: reads CSV into `dict[str, str]`, skipping comments
  and blank lines
- `put()`: appends `channel_id,handle\n` under the lock
- `put_many()`: appends all entries under a single lock acquisition
- `contains()` / `get()`: delegates to an in-memory cache populated
  by `get_all()`, refreshed on each `get_all()` call
- `size()`: returns length of in-memory cache
- Constructor takes `file_path: str`
- If the CSV file does not exist on first read, returns empty dict

### RedisChannelMap

Uses a Redis hash at key `yt:channel_map`.

- `get()`: `HGET yt:channel_map <channel_id>`
- `get_all()`: `HGETALL yt:channel_map`
- `put()`: `HSET yt:channel_map <channel_id> <handle>`
- `put_many()`: `HSET yt:channel_map` with multiple field-value pairs
- `contains()`: `HEXISTS yt:channel_map <channel_id>`
- `size()`: `HLEN yt:channel_map`
- No TTL; entries are permanent (matching current behavior)
- Constructor takes `redis_dsn: str`
- Connection via `redis.asyncio.from_url(redis_dsn,
  decode_responses=True)` (same pattern as `RedisVideoClaim` and
  `RedisCreatorQueue`)

### NullChannelMap

No-op implementation. Returns empty results for all reads, discards
all writes. For contexts where the channel map is not needed.

## Backend Selection

Same pattern as `VideoClaim`:

- If `redis_dsn` is set in settings: `RedisChannelMap(redis_dsn)`
- Otherwise: `FileChannelMap(settings.channel_map_file)`

## Scraper Changes

### Channel scraper (yt_channel_scrape.py)

- `read_channel_map()` at startup replaced by
  `channel_map.get_all()`
- `review_unresolved_ids()` write path calls
  `channel_map.put(channel_id, name)` instead of appending to CSV
- The `asyncio.Lock` (`map_lock`) is removed; `FileChannelMap`
  handles it internally, `RedisChannelMap` is inherently atomic
- `read_channels()` uses `channel_map.contains()` /
  `channel_map.get()` for lookups instead of the in-memory dict

### RSS scraper (yt_rss_scrape.py)

- `get_channelmap()` calls `channel_map.get_all()` for the mapping
- The filesystem scanning logic (scanning `channel_data_dir` for
  `.json.br` files and appending discoveries to CSV) is removed;
  the channel scraper is the authoritative source of mappings
- The `yt_rss_channel_map_size` gauge continues to work via
  `channel_map.size()`

### Video scraper

No changes. The video scraper does not use the channel map.

## Export/Import Tool

New file: `tools/export_channel_map.py`

Bidirectional tool for moving data between Redis and CSV.

### Export (Redis to CSV)

```
python tools/export_channel_map.py --output channel_map.csv
```

- Reads all entries via `RedisChannelMap.get_all()`
- Writes to CSV sorted by handle (consistent with
  `merge_channel_maps.py` output format)
- Requires `REDIS_DSN` to be set

### Import (CSV to Redis)

```
python tools/export_channel_map.py --import-file channel_map.csv
```

- Reads CSV via `FileChannelMap.get_all()`
- Bulk-loads into Redis via `RedisChannelMap.put_many()`
- Requires `REDIS_DSN` to be set
- Used for initial migration from file-based to Redis-based
  channel map

## Removed Tools

- `tools/channel_cleanup.py`: removed. Deduplication is no longer
  needed — Redis hash is naturally deduplicated (last write wins).
- `tools/merge_channel_maps.py`: removed. Multiple hosts share
  one Redis store, so there is nothing to merge. For initial
  migration, use `export_channel_map.py --import-file` to load
  an existing CSV into Redis.

## Error Handling

- `RedisChannelMap`: if Redis is unreachable at startup, fail fast.
  No silent fallback to file. If the user configured `REDIS_DSN`,
  they expect Redis.
- `FileChannelMap`: if the CSV file does not exist on first read,
  return an empty dict (matching current `read_channel_map()`
  behavior).

## Testing

All tests in `tests/unit/test_channel_map.py`:

- `FileChannelMap`: read/write/contains against a temp CSV file,
  including edge cases (comments, blank lines, duplicate IDs,
  missing file)
- `RedisChannelMap`: same operations using `fakeredis` (already a
  dev dependency)
- `export_channel_map.py`: import/export round-trip test

## Redis Key Summary

| Key | Type | Contents |
|-----|------|----------|
| `yt:channel_map` | Hash | field=channel_id, value=channel_handle |
