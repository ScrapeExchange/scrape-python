import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tools.tt_discover_search import (
    DiscoveredTikTokUser,
    JsonlUserSink,
    TikTokDiscoverSearchSettings,
    TikTokCategoryScrapeResult,
    TikTokCategoryTarget,
    TikTokCallType,
    PlaywrightTimeoutError,
    _close_browser_resources,
    _extract_users_from_page,
    _goto_with_limit,
    _new_context,
    append_search_term_targets,
    choose_random_search_terms,
    discover_category_targets,
    discover_category_urls,
    extract_username_from_url,
    extract_usernames_from_text,
    is_tiktok_anti_bot_response,
    main_async,
    normalise_explore_url,
    parse_url_list,
    scrape_category,
)


class _FakeContext:
    def __init__(self) -> None:
        self.timeouts: list[float] = []
        self.navigation_timeouts: list[float] = []

    def set_default_timeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def set_default_navigation_timeout(self, timeout: float) -> None:
        self.navigation_timeouts.append(timeout)


class _CloseFailingResource:
    def __init__(self) -> None:
        self.close_calls: int = 0

    async def close(self) -> None:
        self.close_calls += 1
        raise RuntimeError('already closed')


class _FakeBrowser:
    def __init__(self) -> None:
        self.context = _FakeContext()
        self.context_kwargs: dict | None = None

    async def new_context(self, **kwargs):
        self.context_kwargs = kwargs
        return self.context


class _FakeBrowserType:
    def __init__(self) -> None:
        self.launched: bool = False
        self.launch_kwargs: dict | None = None
        self.browser = _FakeBrowser()

    async def launch(self, **kwargs):
        self.launched = True
        self.launch_kwargs = kwargs
        return self.browser


class _FailingBrowserType:
    async def launch(self, **kwargs):
        del kwargs
        raise AssertionError('chromium should not be launched')


class _FakePlaywright:
    def __init__(self) -> None:
        self.firefox = _FakeBrowserType()
        self.chromium = _FailingBrowserType()


class _FailingAsyncClient:
    def __init__(self, **kwargs) -> None:
        del kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb

    async def get(self, url, params=None):
        del url, params
        raise RuntimeError('offline')


class _FakeLimiter:
    def __init__(self) -> None:
        self.acquired: list[tuple[object, str | None]] = []

    async def acquire(self, call_type, proxy=None):
        self.acquired.append((call_type, proxy))


class _FakeMouse:
    def __init__(self) -> None:
        self.wheels: list[tuple[int, int]] = []

    async def wheel(self, x_delta, y_delta):
        self.wheels.append((x_delta, y_delta))


class _FakeLocator:
    def __init__(self, text: str = 'Explore popular creators') -> None:
        self.text = text

    async def inner_text(self, timeout):
        del timeout
        return self.text


class _TimeoutLocator:
    async def inner_text(self, timeout):
        del timeout
        raise PlaywrightTimeoutError('body timeout')


class _FakePage:
    def __init__(self) -> None:
        self.url = 'https://www.tiktok.com/explore'
        self.goto_kwargs: dict | None = None
        self.selector_kwargs: dict | None = None

    async def goto(self, url, **kwargs):
        self.url = url
        self.goto_kwargs = kwargs
        return SimpleNamespace(status=200)

    async def wait_for_selector(self, selector, **kwargs):
        self.selector_kwargs = {'selector': selector, **kwargs}

    def locator(self, selector):
        del selector
        return _FakeLocator()


class _BodyTimeoutExtractPage:
    async def eval_on_selector_all(self, selector, script):
        del selector, script
        return ['https://www.tiktok.com/@link.creator']

    def locator(self, selector):
        del selector
        return _TimeoutLocator()

    async def evaluate(self, script):
        del script
        return []


class _FakeMenuScrapePage:
    def __init__(self, click_result: bool = True) -> None:
        self.url = 'https://www.tiktok.com/explore'
        self.clicked_labels: list[str] = []
        self.click_result = click_result
        self.mouse = _FakeMouse()

    async def goto(self, url, **kwargs):
        del kwargs
        self.url = url
        return SimpleNamespace(status=200)

    async def wait_for_selector(self, selector, **kwargs):
        del selector, kwargs

    async def eval_on_selector_all(self, selector, script):
        del selector, script
        return ['https://www.tiktok.com/@menu.creator']

    async def evaluate(self, script, *args):
        if args:
            label: str = args[0]
            self.clicked_labels.append(label)
            return self.click_result
        del script
        return 100

    def locator(self, selector):
        del selector
        return _FakeLocator('Explore @text.creator')

    async def wait_for_timeout(self, timeout):
        del timeout


class _FakeCategoryPage:
    async def eval_on_selector_all(self, selector, script):
        del selector, script
        return [
            {
                'href': '/channel/fifa-world-cup-2026',
                'role': '',
                'tag': 'a',
                'text': 'FIFA World Cup 2026',
            },
            {
                'href': '/tag/comedy',
                'role': '',
                'tag': 'a',
                'text': 'Comedy',
            },
            {
                'href': '/search/video?q=Singing%20and%20Dancing',
                'role': '',
                'tag': 'a',
                'text': 'Singing and Dancing',
            },
            {
                'href': '',
                'role': 'button',
                'tag': 'button',
                'text': 'Football',
            },
            {
                'href': '',
                'role': 'button',
                'tag': 'button',
                'text': 'Log in',
            },
            {
                'href': '',
                'role': 'button',
                'tag': 'button',
                'text': '4.4M',
            },
        ]


class _FakeTopMenuPage:
    def __init__(self, labels: list[str]) -> None:
        self.labels = labels

    async def eval_on_selector_all(self, selector, script):
        del selector, script
        return [
            {
                'href': '',
                'role': 'button',
                'tag': 'button',
                'text': label,
            }
            for label in self.labels
        ]


class _FakeEmptyCategoryPage:
    async def eval_on_selector_all(self, selector, script):
        del selector, script
        return []


class _FakeDataScrapePage:
    def __init__(self) -> None:
        self.url = 'https://www.tiktok.com/explore'
        self.mouse = _FakeMouse()

    async def goto(self, url, **kwargs):
        del kwargs
        self.url = url
        return SimpleNamespace(status=200)

    async def wait_for_selector(self, selector, **kwargs):
        del selector, kwargs

    async def eval_on_selector_all(self, selector, script):
        del selector, script
        return []

    async def evaluate(self, script, *args):
        del args
        if 'querySelectorAll' in script:
            return ['json.creator', 'another_creator']
        return 100

    def locator(self, selector):
        del selector
        return _FakeLocator('Explore popular creators')

    async def wait_for_timeout(self, timeout):
        del timeout


class TestTikTokDiscoverBrowse(unittest.TestCase):
    def test_extract_username_from_profile_url(self) -> None:
        self.assertEqual(
            extract_username_from_url(
                'https://www.tiktok.com/@creator.name/video/123'
            ),
            'creator.name',
        )

    def test_extract_username_rejects_reserved_paths(self) -> None:
        self.assertIsNone(
            extract_username_from_url('https://www.tiktok.com/explore')
        )

    def test_extract_usernames_from_text(self) -> None:
        self.assertEqual(
            extract_usernames_from_text(
                'Watch @good.creator and @other_123 today'
            ),
            {'good.creator', 'other_123'},
        )

    def test_extract_users_body_timeout_keeps_link_users(self) -> None:
        import asyncio

        async def run():
            return await _extract_users_from_page(
                _BodyTimeoutExtractPage(),
                category_url='https://www.tiktok.com/explore',
            )

        with self.assertLogs(
            'tools.tt_discover_search',
            level='WARNING',
        ) as logs:
            users = asyncio.run(run())

        self.assertEqual(set(users), {'link.creator'})
        self.assertIn(
            'Timed out reading TikTok page body text',
            logs.output[0],
        )

    def test_anti_bot_detects_status_and_challenge_text(self) -> None:
        self.assertTrue(
            is_tiktok_anti_bot_response(
                status_code=429,
                url='https://www.tiktok.com/explore',
                body_text='',
            )
        )
        self.assertTrue(
            is_tiktok_anti_bot_response(
                status_code=200,
                url='https://www.tiktok.com/explore',
                body_text='Please verify to continue',
            )
        )

    def test_anti_bot_allows_normal_page(self) -> None:
        self.assertFalse(
            is_tiktok_anti_bot_response(
                status_code=200,
                url='https://www.tiktok.com/explore',
                body_text='Explore popular creators and videos',
            )
        )

    def test_parse_url_list_accepts_commas_and_lines(self) -> None:
        self.assertEqual(
            parse_url_list('https://a.example/a, https://b.example/b\nc'),
            ['https://a.example/a', 'https://b.example/b', 'c'],
        )

    def test_normalise_explore_url_accepts_same_origin_explore(self) -> None:
        self.assertEqual(
            normalise_explore_url(
                '/explore/sports?lang=en',
                origin_url='https://www.tiktok.com/explore',
            ),
            'https://www.tiktok.com/explore/sports?lang=en',
        )

    def test_normalise_explore_url_rejects_other_origin(self) -> None:
        self.assertIsNone(
            normalise_explore_url(
                'https://example.com/explore/sports',
                origin_url='https://www.tiktok.com/explore',
            )
        )

    def test_normalise_explore_url_accepts_category_routes(self) -> None:
        origin_url: str = 'https://www.tiktok.com/explore'

        self.assertEqual(
            normalise_explore_url(
                '/channel/fifa-world-cup-2026',
                origin_url=origin_url,
            ),
            'https://www.tiktok.com/channel/fifa-world-cup-2026',
        )
        self.assertEqual(
            normalise_explore_url('/tag/comedy', origin_url=origin_url),
            'https://www.tiktok.com/tag/comedy',
        )
        self.assertEqual(
            normalise_explore_url(
                '/search/video?q=Singing%20and%20Dancing',
                origin_url=origin_url,
            ),
            'https://www.tiktok.com/search/video?q=Singing%20and%20Dancing',
        )

    def test_discover_category_urls_includes_text_only_buttons(self) -> None:
        async def run() -> list[str]:
            return await discover_category_urls(
                _FakeCategoryPage(),
                explore_url='https://www.tiktok.com/explore',
            )

        import asyncio
        urls: list[str] = asyncio.run(run())

        self.assertEqual(urls[0], 'https://www.tiktok.com/explore')
        self.assertIn(
            'https://www.tiktok.com/channel/fifa-world-cup-2026',
            urls,
        )
        self.assertIn('https://www.tiktok.com/tag/comedy', urls)
        self.assertIn(
            'https://www.tiktok.com/search/video?q=Singing%20and%20Dancing',
            urls,
        )
        self.assertIn('https://www.tiktok.com/search?q=Football', urls)
        self.assertNotIn('https://www.tiktok.com/search?q=Log+in', urls)
        self.assertNotIn('https://www.tiktok.com/search?q=4.4M', urls)

    def test_discover_category_urls_covers_top_menu_labels(self) -> None:
        labels: list[str] = [
            'All',
            'FIFA World Cup 2026',
            'Singing and Dancing',
            'Comedy',
            'Sports',
            'Anime & Comics',
            'Relationship',
            'Shows',
            'Lipsync',
            'Beaty Care',
            'Games',
            'Society',
            'Outfit',
            'Cars',
        ]

        async def run() -> list[str]:
            return await discover_category_urls(
                _FakeTopMenuPage(labels),
                explore_url='https://www.tiktok.com/explore',
            )

        import asyncio
        urls: list[str] = asyncio.run(run())

        self.assertEqual(urls[0], 'https://www.tiktok.com/explore')
        self.assertNotIn('https://www.tiktok.com/search?q=All', urls)
        self.assertIn(
            'https://www.tiktok.com/search?q=FIFA+World+Cup+2026',
            urls,
        )
        self.assertIn(
            'https://www.tiktok.com/search?q=Singing+and+Dancing',
            urls,
        )
        self.assertIn('https://www.tiktok.com/search?q=Comedy', urls)
        self.assertIn('https://www.tiktok.com/search?q=Sports', urls)
        self.assertIn(
            'https://www.tiktok.com/search?q=Anime+%26+Comics',
            urls,
        )
        self.assertIn('https://www.tiktok.com/search?q=Relationship', urls)
        self.assertIn('https://www.tiktok.com/search?q=Shows', urls)
        self.assertIn('https://www.tiktok.com/search?q=Lipsync', urls)
        self.assertIn('https://www.tiktok.com/search?q=Beaty+Care', urls)
        self.assertIn('https://www.tiktok.com/search?q=Games', urls)
        self.assertIn('https://www.tiktok.com/search?q=Society', urls)
        self.assertIn('https://www.tiktok.com/search?q=Outfit', urls)
        self.assertIn('https://www.tiktok.com/search?q=Cars', urls)

    def test_discover_category_targets_uses_default_menu_labels(self) -> None:
        async def run() -> list[TikTokCategoryTarget]:
            return await discover_category_targets(
                _FakeEmptyCategoryPage(),
                explore_url='https://www.tiktok.com/explore',
            )

        import asyncio
        targets: list[TikTokCategoryTarget] = asyncio.run(run())

        self.assertGreater(len(targets), 1)
        self.assertEqual(targets[0].url, 'https://www.tiktok.com/explore')
        self.assertIn(
            'Comedy',
            {
                target.label
                for target in targets
                if target.click_menu
            },
        )

    def test_append_search_term_targets_adds_search_urls(self) -> None:
        targets: list[TikTokCategoryTarget] = [
            TikTokCategoryTarget(url='https://www.tiktok.com/explore'),
        ]

        append_search_term_targets(
            targets,
            ['jardin', 'music', 'jardin', '  '],
            origin_url='https://www.tiktok.com/explore',
        )

        self.assertEqual(
            [target.url for target in targets],
            [
                'https://www.tiktok.com/explore',
                'https://www.tiktok.com/search?q=jardin',
                'https://www.tiktok.com/search?q=music',
            ],
        )

    def test_random_search_terms_use_offline_fallback(self) -> None:
        settings = TikTokDiscoverSearchSettings(_cli_parse_args=[])

        async def run() -> list[str]:
            with patch(
                'tools.tt_discover_search.random.choice',
                side_effect=['en', 'water', 'house'],
            ), patch(
                'tools.tt_discover_search.httpx.AsyncClient',
                _FailingAsyncClient,
            ):
                return await choose_random_search_terms(
                    settings,
                    count=2,
                )

        import asyncio
        with self.assertLogs(
            'tools.tt_discover_search',
            level='WARNING',
        ):
            terms: list[str] = asyncio.run(run())

        self.assertEqual(terms, ['water', 'house'])

    def test_scrape_category_clicks_menu_target(self) -> None:
        page = _FakeMenuScrapePage()
        limiter = _FakeLimiter()
        settings = SimpleNamespace(
            navigation_timeout_seconds=12.0,
            bot_penalty_seconds=0.0,
            max_scrolls=1,
            scroll_idle_rounds=1,
            scroll_wait_seconds=0.01,
        )
        target = TikTokCategoryTarget(
            url='https://www.tiktok.com/explore',
            label='Comedy',
            click_menu=True,
        )

        async def run(path: Path) -> TikTokCategoryScrapeResult:
            result = await scrape_category(
                page,
                target,
                limiter=limiter,
                proxy=None,
                settings=settings,
                sink=JsonlUserSink(str(path)),
            )
            return result

        import asyncio
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'users.jsonl'
            result: TikTokCategoryScrapeResult = asyncio.run(run(path))
            records = [
                json.loads(line)
                for line in path.read_text(encoding='utf-8').splitlines()
            ]

        self.assertEqual(page.clicked_labels, ['Comedy'])
        self.assertEqual(
            limiter.acquired,
            [
                (TikTokCallType.BROWSE, None),
                (TikTokCallType.BROWSE, None),
            ],
        )
        self.assertEqual(result.creators_added, 2)
        self.assertEqual(result.scrolls_scraped, 1)
        self.assertEqual(
            {record['username'] for record in records},
            {'menu.creator', 'text.creator'},
        )
        self.assertEqual(
            {record['category_url'] for record in records},
            {'https://www.tiktok.com/search?q=Comedy'},
        )

    def test_scrape_category_skips_missing_menu_target(self) -> None:
        page = _FakeMenuScrapePage(click_result=False)
        limiter = _FakeLimiter()
        settings = SimpleNamespace(
            navigation_timeout_seconds=12.0,
            bot_penalty_seconds=0.0,
            max_scrolls=1,
            scroll_idle_rounds=1,
            scroll_wait_seconds=0.01,
        )
        target = TikTokCategoryTarget(
            url='https://www.tiktok.com/explore',
            label='4.4M',
            click_menu=True,
        )

        async def run(path: Path) -> TikTokCategoryScrapeResult:
            return await scrape_category(
                page,
                target,
                limiter=limiter,
                proxy=None,
                settings=settings,
                sink=JsonlUserSink(str(path)),
            )

        import asyncio
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertLogs(
                'tools.tt_discover_search',
                level='WARNING',
            ) as logs:
                result: TikTokCategoryScrapeResult = asyncio.run(
                    run(Path(tmpdir) / 'users.jsonl')
                )

        self.assertEqual(page.clicked_labels, ['4.4M'])
        self.assertIn(
            'Could not find TikTok explore category menu item',
            logs.output[0],
        )
        self.assertEqual(result.creators_found, 0)
        self.assertEqual(result.creators_added, 0)
        self.assertEqual(result.scrolls_scraped, 0)

    def test_scrape_category_extracts_data_state_usernames(self) -> None:
        page = _FakeDataScrapePage()
        limiter = _FakeLimiter()
        settings = SimpleNamespace(
            navigation_timeout_seconds=12.0,
            bot_penalty_seconds=0.0,
            max_scrolls=1,
            scroll_idle_rounds=1,
            scroll_wait_seconds=0.01,
        )
        target = TikTokCategoryTarget(url='https://www.tiktok.com/explore')

        async def run(path: Path) -> TikTokCategoryScrapeResult:
            return await scrape_category(
                page,
                target,
                limiter=limiter,
                proxy=None,
                settings=settings,
                sink=JsonlUserSink(str(path)),
            )

        import asyncio
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'users.jsonl'
            result: TikTokCategoryScrapeResult = asyncio.run(run(path))
            records = [
                json.loads(line)
                for line in path.read_text(encoding='utf-8').splitlines()
            ]

        self.assertEqual(result.creators_found, 2)
        self.assertEqual(result.creators_added, 2)
        self.assertEqual(
            {record['username'] for record in records},
            {'json.creator', 'another_creator'},
        )

    def test_jsonl_sink_dedupes_usernames(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'users.jsonl'
            sink = JsonlUserSink(str(path))
            user = DiscoveredTikTokUser(
                username='Creator.Name',
                category_url='https://www.tiktok.com/explore',
                source_url='https://www.tiktok.com/@Creator.Name',
                discovered_at='2026-06-18T00:00:00+00:00',
            )

            self.assertTrue(sink.append(user))
            self.assertFalse(sink.append(user))

            lines = path.read_text(encoding='utf-8').splitlines()
            self.assertEqual(len(lines), 1)
            payload = json.loads(lines[0])
            self.assertEqual(payload['platform'], 'tiktok')
            self.assertEqual(payload['username'], 'Creator.Name')

    def test_new_context_uses_camoufox_firefox(self) -> None:
        playwright = _FakePlaywright()
        settings = SimpleNamespace(
            headless=True,
            user_agent='ua',
            navigation_timeout_seconds=12.0,
        )
        launch_options = {
            'executable_path': '/tmp/camoufox',
            'args': ['--x'],
            'env': {'CAMOU_CONFIG_1': 'abc'},
            'headless': True,
        }

        async def run() -> None:
            with patch(
                'tools.tt_discover_search.camoufox_launch_options',
                return_value=launch_options,
            ):
                await _new_context(playwright, settings, None)

        import asyncio
        asyncio.run(run())

        self.assertTrue(playwright.firefox.launched)
        self.assertEqual(
            playwright.firefox.launch_kwargs,
            launch_options,
        )
        self.assertEqual(
            playwright.firefox.browser.context_kwargs['user_agent'],
            'ua',
        )

    def test_goto_uses_domcontentloaded(self) -> None:
        page = _FakePage()
        limiter = _FakeLimiter()
        settings = SimpleNamespace(
            navigation_timeout_seconds=12.0,
            bot_penalty_seconds=0.0,
        )

        async def run() -> None:
            await _goto_with_limit(
                page,
                'https://www.tiktok.com/explore',
                limiter=limiter,
                proxy='proxy',
                settings=settings,
            )

        import asyncio
        asyncio.run(run())

        self.assertEqual(
            limiter.acquired,
            [(TikTokCallType.BROWSE, 'proxy')],
        )
        self.assertEqual(
            page.goto_kwargs['wait_until'],
            'domcontentloaded',
        )
        self.assertEqual(
            page.selector_kwargs['selector'],
            'body',
        )

    def test_browser_cleanup_swallows_close_errors(self) -> None:
        context = _CloseFailingResource()
        browser = _CloseFailingResource()

        async def run() -> None:
            await _close_browser_resources(context, browser)

        import asyncio
        with self.assertLogs(
            'tools.tt_discover_search',
            level='WARNING',
        ) as logs:
            asyncio.run(run())

        self.assertEqual(context.close_calls, 1)
        self.assertEqual(browser.close_calls, 1)
        self.assertEqual(
            sum(
                'TikTok discovery browser cleanup failed' in line
                for line in logs.output
            ),
            2,
        )

    def test_main_async_returns_one_for_unexpected_errors(self) -> None:
        async def fail_run(settings) -> int:
            del settings
            raise RuntimeError('boom')

        async def run() -> int:
            with patch('tools.tt_discover_search.run', fail_run):
                return await main_async([])

        import asyncio
        with self.assertLogs(
            'tools.tt_discover_search',
            level='ERROR',
        ) as logs:
            rc: int = asyncio.run(run())

        self.assertEqual(rc, 1)
        self.assertIn(
            'TikTok search discovery exited with an unhandled error',
            logs.output[0],
        )


if __name__ == '__main__':
    unittest.main()
