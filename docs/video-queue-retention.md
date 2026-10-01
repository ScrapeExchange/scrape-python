# Video queue diagnostic retention

Terminal video IDs remain in Redis indefinitely to prevent duplicate work.
Diagnostic details are retained for 30 days. Run the cleanup command daily
to replace older terminal JSON records with `{"ts": <original timestamp>}`
and delete their separate metadata hashes. This reduces memory per record;
the exact terminal-ID index will still grow with the catalogue.

The command supports the YouTube and TikTok video queues. It does not
modify uploaded-ID sets, channel queues, identity maps, claims, or rate
limits. No Redis eviction policy or encoding configuration is changed.

## Rollout

1. Deploy the updated `video_scrape_queue.py` to **all producers and
   consumers** first, including RSS, channel, discovery and operator tools.
   Restart long-running processes. Older producers check only metadata
   and can re-enqueue terminal IDs after metadata is deleted or expires.
2. Supply the connection through `REDIS_DSN` or the local `.env` file.
   Do not put credentials in shell commands or committed files.
3. Inspect a limited, non-mutating dry run:

   ```sh
   uv run python -m tools.video_queue_retention --limit 1000
   ```

4. Run the full dry run, then apply:

   ```sh
   uv run python -m tools.video_queue_retention
   uv run python -m tools.video_queue_retention --apply
   ```

5. Schedule the apply command daily in the same environment. For TikTok,
   run it separately with `--platform tiktok`.

`--batch-size` defaults to 200 and `--pause-seconds` to 0.05. Updates run
one record at a time, with a pause after each batch. `--limit` bounds
records examined, not records changed, and is intended for inspection;
repeated limited runs are not a replacement for a complete daily scan.

## Output and safety

Output is JSON. In dry-run mode, `records_compacted`, `metadata_deleted`
and `ttls_added` count proposed actions. In apply mode they count completed
actions. `invalid` counts malformed, missing or future timestamps;
`skipped` counts concurrent changes or conflicting active queue state.
Investigate these records separately; cleanup leaves them untouched.

Cleanup compares each scanned record with its current value atomically.
It also checks queue membership and metadata state before changing data.
Recent metadata without a TTL receives the remaining portion of the
30-day lifetime, measured from the original terminal timestamp. Existing
TTLs are preserved. Old records without metadata are compacted too.
Re-running after interruption is safe. HSCAN can repeat records when the
hash changes during scanning, so counters are operational estimates.

Compare Redis `used_memory` and sampled `MEMORY USAGE` before and after.
Process RSS may remain elevated because the allocator retains freed pages;
record counts do not directly predict DRAM savings. No saving is claimed
until measured on the target server.

Keep the tombstone-aware queue code deployed after cleanup. Rolling back
to metadata-only deduplication would make compacted terminal IDs eligible
for normal enqueueing again.
