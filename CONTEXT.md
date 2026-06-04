# scrape-python

YouTube scraping fleet that feeds the scrape.exchange API. Defines how
we identify, deduplicate, and refresh **Channels** and their content.

## Language

### Channel identity

**Channel**:
A YouTube creator account that owns videos and an RSS feed. Identified
by three properties — `channel_id`, `channel_handle`, `title` — any one
of which can be missing at intake time.

**channel_id**:
The opaque, stable, YouTube-assigned identifier (e.g. `UC...`). The
only property guaranteed unique and immutable across a channel's
lifetime.

**channel_handle**:
The `@handle` slug a creator chooses (e.g. `@LinusTechTips`). Roughly
unique at a point in time but can be changed by the creator. Validation
rules from YouTube docs (3–30 chars, Latin/script-specific minima,
limited punctuation), in practice slightly more permissive — but **never
contains whitespace or `/`**.
_Avoid_: Just "channel" as a field name — historically meant handle and
caused the title/handle mixup we are cleaning up.

**title**:
The human-readable display name shown in the UI. Not unique, not
stable. May contain spaces, slashes, and any unicode.
_Avoid_: "name", "channel_name", "display name" — all refer to the same
thing.

### Intake artifacts

**channels.lst**:
An export/backup snapshot of the channel scrape queue, in the repo
root (or configured via `YOUTUBE_CHANNEL_LIST`). Produced on demand
by `yt_channel_queue export`; not read during normal scraping (the
scraper consumes the Redis-backed queue described under
[Channel workflow state](#channel-workflow-state)). Canonical line
format unchanged: JSONL with the keys `channel_id`, `channel_handle`,
`title`, `status` (missing values are explicit `null`), plus an
optional `comment` key that the cleanup tool preserves verbatim but
never inspects. The file tolerates raw non-JSONL lines on input
(handle, url, id, title) — the cleanup tool canonicalises them on
the next write. File-level preamble is not supported; documentation
goes in `comment` keys.

Historically (pre-2026-05) `channels.lst` was the intake list the
scraper read at startup; that role has moved to the Redis channel
scrape queue. The file is retained for backup and one-shot recovery.
_Avoid_: "channel list", "the list" — say `channels.lst`.

**channels.lst.dropped**:
Audit sidecar — raw lines the cleanup tool refused to keep, each
annotated with the reason. Never read; only written.

**channel scrape queue**:
The Redis-backed work queue that drives `yt_channel_scrape.py`.
Lives under the `youtube:channel:*` keyspace. The authoritative
source for "which channels are queued to be scraped, in what
order". Each entry is a **Channel** plus its current
[workflow state](#channel-workflow-state). Managed via the
`yt_channel_queue` CLI.
_Avoid_: "the queue" without qualifier — also describes the RSS
queue (`rss:youtube:*`) and the video request directory.

**external JSONL (DB export)**:
A per-line `{channel_id, channel_handle, title}` snapshot produced
from the scrape.exchange database. Used as a high-trust corrective
input during a one-time reconciliation against the Redis maps.

### Channel state

**scraped**:
A **Channel** for which a `channel-{channel_id}.json.br` file exists
under `YOUTUBE_CHANNEL_DATA_DIR` (base or `uploaded/`). The file is
always keyed on `channel_id`; the legacy `channel-{channel_handle}`
form is renamed to the id form by the one-time
`yt_migrate_channel_files_to_id` cut-over and is not written by any
current code path. Use as the `status` value in `channels.lst` JSONL
output.

**new**:
A **Channel** known to the system (in `channels.lst` or a Redis map)
but lacking a scraped file. Orthogonal to the channel's
**workflow state** (see below): a `new` channel may currently be
`pending_resolution`, `scheduled`, `not_found`, etc. The two axes
answer different questions — `scraped`/`new` describes "do we have
data?", workflow state describes "what's the scraper doing about
it?".

### Channel workflow state

A separate axis from `scraped` / `new`. Each **Channel** known to the
scrape queue is in exactly one of the following states at any
moment. Operators see these names in `yt_channel_queue` output, log
messages, and Prometheus metrics. Internal Redis layout for each
state lives in the channel-scrape-queue spec.

Active states (scraper is or will be acting on the channel):

**pending_resolution**:
Handle is known; the channel_id has not yet been looked up. The
resolve phase will pick it up next wave.

**scheduled**:
Channel is known by id and is queued for its next scrape at a
specific `scheduled_time`. The default state for a successfully-scraped
channel awaiting its tier-based refresh.

Terminal states (scraper does not act on the channel until an
operator restores it):

**not_found**:
YouTube returned a 404/410 on resolve or scrape. Channel doesn't
exist (or is no longer addressable by this id/handle).

**invalid_handle**:
The handle string failed parsing — wrong format, didn't satisfy the
handle hard rules in this glossary. No YouTube call was made.

**inconsistent_identity**:
`ChannelIdentityStore.bind()` flagged a conflict between an existing
`(id, handle)` pair and the one we just resolved. Requires operator
adjudication via the importer/cleanup tooling.

**terminated**:
The channel page reports a YouTube-terminated channel.

**unresolved**:
The resolve phase exhausted its retry budget on transient errors.
Distinct from `not_found` (which is a definitive negative from
YouTube) — `unresolved` means we never got a definitive answer.

**removed**:
The operator tombstoned this channel via `yt_channel_queue remove`.
Future imports will not re-add it unless `--force` is passed.

**soft_unavailable**:
The scrape returned a transient error (network/region/temporary
block). The channel is parked with a `next_retry_at` timestamp; an
automated reaper will move it back to `scheduled` on its own.

**hard_unavailable**:
Three soft-unavailable attempts in a row failed. Operationally
distinct from `soft_unavailable`: needs operator action to come
back. Most often correlates with channels that are functionally
gone but didn't trigger a clean `not_found` or `terminated` signal.

### Redis identity maps

Three Redis hashes give every direction of lookup between the identity
properties. Each is keyed by *the thing in its name* and stores the
*other* property as the value.

**name_map** (`youtube:name_map`):
`title → channel_id`. Keyed by display name. Lookup: "given this title,
what's the channel?"
_Note_: Misleadingly named — the key is what we call `title`, not
`name`. Rename deferred.

**creator_map** (`youtube:creator_map`):
`channel_id → channel_handle`. Lookup: "given this id, what's the
current handle?"

**handle_map** (`youtube:handle_map`):
`channel_handle → channel_id`. **Newly added** to close the lookup
triangle. Persisted alongside `creator_map` whenever the scraper
resolves a handle. Kept as a denormalised reverse rather than
inverting `creator_map` on demand because both maps are expected to
grow to millions of entries — inversion would dominate cleanup-tool
runtimes.

## Relationships

- A **Channel** has exactly one **channel_id** (stable) and at most one
  **channel_handle** and **title** at any given moment.
- **creator_map** ↔ **handle_map** are *inverses* and must stay
  consistent — every `(id, handle)` in creator_map must appear as
  `(handle, id)` in handle_map.
- **name_map** is many-to-one in principle (two channels can share a
  title) but in our data we treat it as one-to-one and surface
  collisions as conflicts.

### Deduplication

Two intake entries refer to the same **Channel** when:

1. **By id** — both resolve to the same `channel_id`. Resolution
   pipeline: take the entry's own `channel_id` if present, else look up
   `handle_map[handle]`, else look up `name_map[title]`. Redis lookups
   only — no HTTP resolution from the cleanup tool (too slow).
2. **By normalized handle** (only when both sides still lack id) —
   their handles match case-insensitively after stripping leading `@`
   and surrounding whitespace. When grouped this way, the canonical
   surviving handle is the **mixed-case** variant (preserves the
   creator's intended casing).

Entries that cannot be deduped by either rule stay as separate records
with `status: "new"` and `channel_id: null`. Resolving them against
YouTube is the scraper's job, not the cleanup tool's.

Titles are **never** used for fuzzy-matching — too noisy.

### Conflict resolution

A "conflict" is a disagreement between two sources of the kind
captured under "Source precedence". Conflicts between levels 1–4 are
resolved automatically. Conflicts involving **`channels.lst`** OR
between Redis identity maps (e.g.
`name_map[title]` ≠ `handle_map[handle]^-1` for the same channel) are
surfaced interactively. Each conflict carries a **type tag** (e.g.
"`channels.lst` handle differs from creator_map handle"); every
interactive prompt offers a "**skip all mismatches of this type**"
option that suppresses the rest of that type for the run and leaves
those conflicts unresolved.

### Source precedence (channel identity)

When multiple sources disagree on a channel's identity fields, this
order applies (highest authority first):

1. **Clean scraped file** — a `channel-*.json.br` file under
   `YOUTUBE_CHANNEL_DATA_DIR` (base or `uploaded/`) whose contents use
   the `channel_handle` field (not legacy `channel`) and whose value
   passes the handle hard rules (no whitespace, no `/`).
2. **External JSONL (DB export)** — the per-line
   `{channel_id, channel_handle, title}` file produced from the
   scrape.exchange database.
3. **Legacy scraped file** — a `channel-*.json.br` file that uses the
   legacy `channel` field or whose handle value fails the hard rules.
4. **Redis identity maps** — `creator_map`, `handle_map`, `name_map`.
5. **channels.lst** — export/backup snapshot; historically the intake
   list, but post-2026-05 the Redis channel scrape queue takes that
   role. Treated as the lowest-trust source on the rare occasions it
   is consulted (one-shot recovery if Redis is lost).

Levels 1 and 3 split because a portion of the scraped corpus
pre-dates the migration from the legacy `channel` field name to
`channel_handle`. Treating all scraped files as equally authoritative
would let those legacy files overwrite cleaner data from the DB
export.

Disagreements within levels 1–4 are resolved automatically (higher
beats lower). Disagreements between level 4 and level 5 are
surfaced interactively for human judgement, since that's where the
historical mixups live.

## Flagged ambiguities

- **"channel" as a field name** — historically used in scraped files
  and in `channels.lst` JSONL objects to mean `channel_handle`, but
  sometimes filled with a `title` value. Resolved: the canonical key is
  `channel_handle`; cleanup must detect and re-label legacy `channel`
  fields.
- **`creator_map` Redis key name** — opaque; describes neither the key
  nor the value. Kept for now to avoid a rename project; flagged for
  later.
- **`yt_resolve_channel_ids.py`** is on a deprecation path. Its
  resolution logic migrates into a shared library
  (`scrape_exchange/youtube/channel_identity.py`); `yt_channel_scrape.py`
  takes over inline channel-id resolution from there. The old tool
  remains untouched and operators stop invoking it once the scraper
  cutover is live.
