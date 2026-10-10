'''Fake bulk exchange for scrape_upload tests: records POSTs and lets
tests hold finalize until released.'''

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch

from scrape_exchange.bulk_upload import (
    BulkBatchOutcome,
    BulkUploadState,
    delete_bulk_state,
    write_bulk_state,
)
from scrape_exchange.file_management import AssetFileManagement


class FakeBulkExchange:
    '''Stand-in for post_/finalize_prepared_bulk_batch.

    ``posts`` records ``(config, batch_buf, batch_records)`` per POST.
    When ``hold_finalize`` is True, finalize waits on ``release``.
    ``finalize_error`` makes finalize raise it. ``post_outcome`` makes
    POST return it as its error outcome. ``write_state`` makes POST
    write a real ``.bulk`` state file, which a completed finalize
    deletes (like the real one). ``finalize_status`` other than
    ``'completed'`` makes finalize give up with that status and keep
    the state file and the source files.
    '''

    def __init__(self, *, hold_finalize: bool = False) -> None:
        self.posts: list[tuple[Any, bytes, list[tuple[str, str]]]] = []
        self.finalized: list[str] = []
        self.hold_finalize: bool = hold_finalize
        self.release: asyncio.Event = asyncio.Event()
        self.finalize_error: Exception | None = None
        self.post_error: Exception | None = None
        self.post_outcome: BulkBatchOutcome | None = None
        self.write_state: bool = False
        self.finalize_status: str = 'completed'
        self.in_finalize: int = 0
        self.max_in_finalize: int = 0

    async def post(
        self, batch_buf: bytes, batch_records: list[tuple[str, str]],
        config: Any, client: Any, fm: AssetFileManagement,
    ) -> tuple[str, str, BulkBatchOutcome | None]:
        del client
        if self.post_error is not None:
            raise self.post_error
        self.posts.append((config, batch_buf, list(batch_records)))
        if self.post_outcome is not None:
            return '', '', self.post_outcome
        number: int = len(self.posts)
        job_id: str = f'job{number}'
        if self.write_state:
            await write_bulk_state(fm, BulkUploadState(
                job_id=job_id, batch_id=f'batch{number}',
                schema_owner=config.schema_owner,
                schema_version=config.schema_version,
                platform=config.platform, entity=config.entity,
                upload_filename=f'{job_id}.jsonl',
                batch_records=list(batch_records),
            ))
        return job_id, f'batch{number}', None

    async def finalize(
        self, job_id: str, batch_id: str,
        err: BulkBatchOutcome | None,
        batch_records: list[tuple[str, str]], config: Any, client: Any,
        fm: AssetFileManagement, **kwargs: Any,
    ) -> BulkBatchOutcome:
        del batch_id, config, client, kwargs
        if err is not None:
            return err
        self.in_finalize += 1
        self.max_in_finalize = max(self.max_in_finalize, self.in_finalize)
        try:
            if self.hold_finalize:
                await self.release.wait()
            if self.finalize_error is not None:
                raise self.finalize_error
            if self.finalize_status != 'completed':
                return BulkBatchOutcome(
                    status=self.finalize_status, job_id=job_id,
                    success=0, failed=0, missing=0,
                )
            for _content_id, filename in batch_records:
                await fm.mark_uploaded(filename)
            await delete_bulk_state(fm, job_id)
            self.finalized.append(job_id)
            return BulkBatchOutcome(
                status='completed', job_id=job_id,
                success=len(batch_records), failed=0, missing=0,
                success_ids={cid for cid, _ in batch_records},
            )
        finally:
            self.in_finalize -= 1

    @contextlib.contextmanager
    def patched(self) -> Iterator[FakeBulkExchange]:
        '''Patch scrape_upload's POST/finalize/resume entry points.'''
        with (
            patch(
                'tools.scrape_upload.post_prepared_bulk_batch',
                side_effect=self.post,
            ),
            patch(
                'tools.scrape_upload.finalize_prepared_bulk_batch',
                side_effect=self.finalize,
            ),
            patch(
                'tools.scrape_upload.resume_pending_bulk_uploads',
                side_effect=_no_resume,
            ),
        ):
            yield self


async def _no_resume(*args: Any, **kwargs: Any) -> None:
    del args, kwargs

