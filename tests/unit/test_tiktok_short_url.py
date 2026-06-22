'''Unit tests for scrape_exchange.tiktok.short_url.'''

import unittest

from scrape_exchange.tiktok import short_url as su


class TestShortUrlRecognition(unittest.TestCase):

    def test_is_short_url_vm_and_vt(self) -> None:
        self.assertTrue(
            su.is_tiktok_short_url('https://vm.tiktok.com/ZGJEytV2E/'),
        )
        self.assertTrue(
            su.is_tiktok_short_url('https://vt.tiktok.com/ZGJEytV2E'),
        )

    def test_is_short_url_rejects_profile_and_other(self) -> None:
        self.assertFalse(
            su.is_tiktok_short_url(
                'https://www.tiktok.com/@charlidamelio',
            ),
        )
        self.assertFalse(su.is_tiktok_short_url('charlidamelio'))
        self.assertFalse(su.is_tiktok_short_url('https://example.com/x'))

    def test_normalize_strips_query_and_trailing_slash(self) -> None:
        self.assertEqual(
            su.normalize_tiktok_short_url(
                'https://vm.tiktok.com/ZGJEytV2E/?x=y',
            ),
            'https://vm.tiktok.com/ZGJEytV2E',
        )

    def test_normalize_preserves_vt_subdomain(self) -> None:
        self.assertEqual(
            su.normalize_tiktok_short_url('http://vt.tiktok.com/abc123'),
            'https://vt.tiktok.com/abc123',
        )

    def test_normalize_returns_none_for_non_short_url(self) -> None:
        self.assertIsNone(
            su.normalize_tiktok_short_url('https://www.tiktok.com/@a'),
        )

    def test_extract_handle_from_profile_url(self) -> None:
        self.assertEqual(
            su.extract_handle_from_resolved_url(
                'https://www.tiktok.com/@daily.dose.of_cs',
            ),
            'daily.dose.of_cs',
        )

    def test_extract_handle_from_video_url_with_query(self) -> None:
        self.assertEqual(
            su.extract_handle_from_resolved_url(
                'https://www.tiktok.com/@daily.dose.of_cs/video/'
                '7296177730775731489?is_from_webapp=1',
            ),
            'daily.dose.of_cs',
        )

    def test_extract_handle_returns_none_for_non_tiktok(self) -> None:
        self.assertIsNone(
            su.extract_handle_from_resolved_url(
                'https://example.com/@nope',
            ),
        )


class TestClassifyResolution(unittest.TestCase):

    def test_resolved_from_video_url(self) -> None:
        res = su._classify_resolution(
            200, 'https://www.tiktok.com/@alice/video/7296177730775731489',
        )
        self.assertIs(res.outcome, su.ShortUrlOutcome.RESOLVED)
        self.assertEqual(res.handle, 'alice')

    def test_resolved_via_og_url_interstitial(self) -> None:
        body: str = (
            '<html><head><meta property="og:url" '
            'content="https://www.tiktok.com/@bob/video/7"></head></html>'
        )
        res = su._classify_resolution(
            200, 'https://vm.tiktok.com/ZGJEytV2E', body,
        )
        self.assertIs(res.outcome, su.ShortUrlOutcome.RESOLVED)
        self.assertEqual(res.handle, 'bob')

    def test_429_is_transient(self) -> None:
        for status in (403, 408, 425, 429, 500, 503):
            res = su._classify_resolution(status, None)
            self.assertIs(
                res.outcome, su.ShortUrlOutcome.TRANSIENT,
                msg=f'status {status}',
            )

    def test_404_and_410_are_unavailable(self) -> None:
        for status in (404, 410):
            res = su._classify_resolution(status, None)
            self.assertIs(res.outcome, su.ShortUrlOutcome.UNAVAILABLE)

    def test_non_tiktok_destination_unavailable(self) -> None:
        res = su._classify_resolution(200, 'https://example.com/spam')
        self.assertIs(res.outcome, su.ShortUrlOutcome.UNAVAILABLE)
        self.assertIsNone(res.handle)

    def test_other_4xx_unavailable(self) -> None:
        res = su._classify_resolution(400, None)
        self.assertIs(res.outcome, su.ShortUrlOutcome.UNAVAILABLE)


import httpx
from unittest.mock import AsyncMock, MagicMock, patch


def _client_returning(resp: object) -> MagicMock:
    '''Build a mock standing in for httpx.AsyncClient(...) whose async
    context manager yields a client with get() -> resp.'''
    client: MagicMock = MagicMock()
    client.get = AsyncMock(return_value=resp)
    ctx: MagicMock = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=client)
    ctx.__aexit__ = AsyncMock(return_value=False)
    factory: MagicMock = MagicMock(return_value=ctx)
    return factory


class TestResolveCreatorShortUrl(unittest.IsolatedAsyncioTestCase):

    async def test_resolved_handle_via_redirect(self) -> None:
        resp = httpx.Response(
            200,
            request=httpx.Request(
                'GET',
                'https://www.tiktok.com/@alice/video/7296177730775731489',
            ),
        )
        factory = _client_returning(resp)
        with patch.object(su.httpx, 'AsyncClient', factory):
            res = await su.resolve_creator_short_url(
                'https://vm.tiktok.com/ZGJEytV2E',
                proxy='http://127.0.0.1:8080',
                timeout=10.0,
                user_agent='ua',
            )
        self.assertIs(res.outcome, su.ShortUrlOutcome.RESOLVED)
        self.assertEqual(res.handle, 'alice')

    async def test_transport_error_is_transient(self) -> None:
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(
            return_value=MagicMock(
                get=AsyncMock(side_effect=httpx.ConnectError('boom')),
            ),
        )
        ctx.__aexit__ = AsyncMock(return_value=False)
        with patch.object(
            su.httpx, 'AsyncClient', MagicMock(return_value=ctx),
        ):
            res = await su.resolve_creator_short_url(
                'https://vm.tiktok.com/ZGJEytV2E',
                proxy='http://127.0.0.1:8080',
                timeout=10.0,
                user_agent='ua',
            )
        self.assertIs(res.outcome, su.ShortUrlOutcome.TRANSIENT)


if __name__ == '__main__':
    unittest.main()
