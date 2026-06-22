#!/usr/bin/env python3
'''Discover TikTok usernames from Explore and search result pages.'''

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict

from scrape_exchange.logging import configure_logging
from scrape_exchange.rate_limiter import RateLimiter, _BucketConfig
from scrape_exchange.settings import ScraperSettings

try:
    from camoufox import launch_options as camoufox_launch_options
except ImportError:  # pragma: no cover - exercised at runtime only.
    camoufox_launch_options = None  # type: ignore[assignment]

try:
    from playwright.async_api import (
        Browser,
        BrowserContext,
        Error as PlaywrightError,
        Page,
        Playwright,
        Response,
        TimeoutError as PlaywrightTimeoutError,
        async_playwright,
    )
except ImportError:  # pragma: no cover - exercised at runtime only.
    Browser = Any  # type: ignore[misc, assignment]
    BrowserContext = Any  # type: ignore[misc, assignment]
    PlaywrightError = Exception  # type: ignore[assignment]
    Page = Any  # type: ignore[misc, assignment]
    Playwright = Any  # type: ignore[misc, assignment]
    Response = Any  # type: ignore[misc, assignment]
    PlaywrightTimeoutError = TimeoutError  # type: ignore[assignment]
    async_playwright = None  # type: ignore[assignment]


_LOGGER: logging.Logger = logging.getLogger(__name__)

_TIKTOK_ORIGIN: str = 'https://www.tiktok.com'
_EXPLORE_PATH: str = '/explore'
_DEFAULT_OUTPUT: str = 'data/tiktok-discovered-usernames.jsonl'
_DEFAULT_RANDOM_WORD_LANGUAGES: tuple[str, ...] = (
    'en', 'es', 'it', 'de', 'fr', 'zh', 'pt-br', 'ro',
)
_OFFLINE_RANDOM_TERMS: tuple[str, ...] = (
    'water', 'house', 'music', 'historia', 'cocina',
    'viaggio', 'wissenschaft', 'jardin', 'cidade',
    'tecnologia', 'familia', 'natureza',
)
_USERNAME_RE: re.Pattern[str] = re.compile(
    r'(?<![A-Za-z0-9_.])@([A-Za-z0-9_.]{2,24})'
)
_PROFILE_PATH_RE: re.Pattern[str] = re.compile(
    r'^/@([A-Za-z0-9_.]{2,24})(?:$|[/?#])'
)
_CATEGORY_PATH_RE: re.Pattern[str] = re.compile(
    r'^/(?:explore(?:/[^?#]+)?|channel/[^?#]+|tag/[^?#]+|search'
    r'(?:/[^?#]+)?)(?:[?#].*)?$'
)
_CATEGORY_METRIC_TEXT_RE: re.Pattern[str] = re.compile(
    r'^\d+(?:[.,]\d+)?\s*[KMB]?$',
    re.IGNORECASE,
)
_BOT_TEXT_PATTERNS: tuple[str, ...] = (
    'captcha',
    'verify to continue',
    'verification',
    'too many requests',
    'access denied',
    'unusual traffic',
    'suspicious activity',
    'maximum number of attempts',
)
_IGNORED_USERNAMES: frozenset[str] = frozenset({
    'about',
    'business',
    'creators',
    'discover',
    'explore',
    'feedback',
    'following',
    'foryou',
    'live',
    'login',
    'privacy',
    'tag',
    'terms',
    'upload',
})
_IGNORED_CATEGORY_TEXTS: frozenset[str] = frozenset({
    'about',
    'all',
    'comments',
    'creators',
    'explore',
    'following',
    'for you',
    'home',
    'inbox',
    'live',
    'log in',
    'login',
    'more',
    'profile',
    'search',
    'share',
    'tiktok',
    'upload',
})
_DEFAULT_EXPLORE_CATEGORY_LABELS: tuple[str, ...] = (
    'FIFA World Cup 2026',
    'Singing and Dancing',
    'Comedy',
    'Sports',
    'Anime & Comics',
    'Relationship',
    'Shows',
    'Lipsync',
    'Beaty Care',
    'Beauty Care',
    'Games',
    'Society',
    'Outfit',
    'Cars',
)


class TikTokCallType(str, Enum):
    '''Discriminator for TikTok browser traffic.'''

    BROWSE = 'browse'


_DEFAULT_CONFIGS: dict[TikTokCallType, _BucketConfig] = {
    TikTokCallType.BROWSE: _BucketConfig(
        burst=3,
        refill_rate=12 / 60,
        jitter_min=2.0,
        jitter_max=6.0,
    ),
}
_GLOBAL_CONFIG: _BucketConfig = _BucketConfig(
    burst=3,
    refill_rate=12 / 60,
    jitter_min=0.0,
    jitter_max=0.0,
)


class TikTokBrowseRateLimiter(RateLimiter[TikTokCallType]):
    '''Shared rate limiter for TikTok browser navigation.'''

    def __init__(
        self,
        state_dir: str | None = None,
        redis_dsn: str | None = None,
    ) -> None:
        super().__init__(
            platform='tiktok',
            state_dir=state_dir,
            redis_dsn=redis_dsn,
        )

    @property
    def default_configs(self) -> dict[TikTokCallType, _BucketConfig]:
        return _DEFAULT_CONFIGS

    @property
    def global_config(self) -> _BucketConfig:
        return _GLOBAL_CONFIG


class TikTokDiscoverSearchSettings(ScraperSettings):
    '''Settings for ``tt_discover_search.py``.'''

    model_config = SettingsConfigDict(
        env_file=(
            str(Path(__file__).parent.parent / '.env'),
            '.env',
        ),
        env_file_encoding='utf-8',
        cli_parse_args=True,
        cli_kebab_case=True,
        populate_by_name=True,
        extra='ignore',
    )

    log_file: str = Field(
        default='/dev/stderr',
        validation_alias=AliasChoices('LOG_FILE', 'log_file'),
        description='Log file path',
    )
    output_file: str = Field(
        default=_DEFAULT_OUTPUT,
        validation_alias=AliasChoices(
            'TIKTOK_DISCOVER_OUTPUT_FILE',
            'output_file',
        ),
        description='Append-only JSONL file for discovered usernames.',
    )
    explore_url: str = Field(
        default=f'{_TIKTOK_ORIGIN}{_EXPLORE_PATH}',
        validation_alias=AliasChoices(
            'TIKTOK_EXPLORE_URL',
            'explore_url',
        ),
        description='TikTok explore page used to discover category links.',
    )
    category_urls: str = Field(
        default='',
        validation_alias=AliasChoices(
            'TIKTOK_CATEGORY_URLS',
            'category_urls',
        ),
        description=(
            'Comma-separated category URLs. When empty, links are '
            'discovered from explore_url.'
        ),
    )
    max_categories: int = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices(
            'TIKTOK_MAX_CATEGORIES',
            'max_categories',
        ),
        description='Maximum category pages to scrape; 0 means all.',
    )
    search_term_count: int = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices(
            'TIKTOK_SEARCH_TERM_COUNT',
            'search_term_count',
        ),
        description=(
            'Number of random search terms to scrape in addition to '
            'Explore categories; 0 disables random searches.'
        ),
    )
    random_word_url: str = Field(
        default='https://random-word-api.herokuapp.com/word',
        validation_alias=AliasChoices(
            'RANDOM_WORD_URL', 'random_word_url',
        ),
        description='Random-word API endpoint for search terms.',
    )
    random_word_languages: str = Field(
        default=','.join(_DEFAULT_RANDOM_WORD_LANGUAGES),
        validation_alias=AliasChoices(
            'RANDOM_WORD_LANGUAGES', 'random_word_languages',
        ),
        description='Comma-separated random-word API language codes.',
    )
    random_word_language: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'RANDOM_WORD_LANGUAGE', 'random_word_language',
        ),
        description='Optional random-word API language code.',
    )
    max_scrolls: int = Field(
        default=80,
        ge=1,
        validation_alias=AliasChoices(
            'TIKTOK_MAX_SCROLLS',
            'max_scrolls',
        ),
        description='Maximum scroll attempts per category page.',
    )
    scroll_idle_rounds: int = Field(
        default=4,
        ge=1,
        validation_alias=AliasChoices(
            'TIKTOK_SCROLL_IDLE_ROUNDS',
            'scroll_idle_rounds',
        ),
        description='Stop after this many scrolls with no new height/users.',
    )
    scroll_wait_seconds: float = Field(
        default=1.5,
        gt=0.0,
        validation_alias=AliasChoices(
            'TIKTOK_SCROLL_WAIT_SECONDS',
            'scroll_wait_seconds',
        ),
        description='Seconds to wait after each scroll.',
    )
    navigation_timeout_seconds: float = Field(
        default=45.0,
        gt=0.0,
        validation_alias=AliasChoices(
            'TIKTOK_NAVIGATION_TIMEOUT_SECONDS',
            'navigation_timeout_seconds',
        ),
        description='Browser navigation timeout.',
    )
    headless: bool = Field(
        default=True,
        validation_alias=AliasChoices('TIKTOK_HEADLESS', 'headless'),
        description='Run the browser in headless mode.',
    )
    user_agent: str = Field(
        default=(
            'Mozilla/5.0 (X11; Linux x86_64; rv:135.0) '
            'Gecko/20100101 Firefox/135.0'
        ),
        validation_alias=AliasChoices(
            'TIKTOK_USER_AGENT',
            'user_agent',
        ),
        description='Browser user-agent string.',
    )
    bot_penalty_seconds: float = Field(
        default=300.0,
        ge=0.0,
        validation_alias=AliasChoices(
            'TIKTOK_BOT_PENALTY_SECONDS',
            'bot_penalty_seconds',
        ),
        description='Limiter penalty when TikTok bot detection is seen.',
    )


@dataclass(frozen=True)
class DiscoveredTikTokUser:
    '''One TikTok username discovered from a browse category.'''

    username: str
    category_url: str
    source_url: str | None
    discovered_at: str


@dataclass(frozen=True)
class TikTokCategoryTarget:
    '''A TikTok category page or client-side menu tab to scrape.'''

    url: str
    label: str | None = None
    click_menu: bool = False

    @property
    def category_url(self) -> str:
        if self.label is None:
            return self.url
        return _category_search_url(self.label, origin_url=self.url)


@dataclass(frozen=True)
class TikTokCategoryScrapeResult:
    '''Counts collected while scraping one TikTok category target.'''

    creators_found: int
    creators_added: int
    scrolls_scraped: int


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalise_username(value: str) -> str | None:
    username: str = value.strip().lstrip('@').strip()
    if not username:
        return None
    if username.lower() in _IGNORED_USERNAMES:
        return None
    if not re.fullmatch(r'[A-Za-z0-9_.]{2,24}', username):
        return None
    return username


def extract_username_from_url(url: str) -> str | None:
    '''Extract a TikTok profile username from a URL, when present.'''

    parsed = urlparse(url)
    match = _PROFILE_PATH_RE.match(parsed.path)
    if not match:
        return None
    return _normalise_username(match.group(1))


def extract_usernames_from_text(text: str) -> set[str]:
    '''Extract ``@username`` references from visible page text.'''

    found: set[str] = set()
    for match in _USERNAME_RE.finditer(text):
        username: str | None = _normalise_username(match.group(1))
        if username is not None:
            found.add(username)
    return found


def is_tiktok_anti_bot_response(
    *,
    status_code: int | None,
    url: str,
    body_text: str,
) -> bool:
    '''Return True when a browser page looks blocked or challenged.'''

    if status_code in {403, 407, 418, 429, 451}:
        return True
    lowered_url: str = url.lower()
    if 'captcha' in lowered_url or 'verify' in lowered_url:
        return True
    lowered_text: str = body_text[:20000].lower()
    return any(pattern in lowered_text for pattern in _BOT_TEXT_PATTERNS)


def parse_url_list(raw: str) -> list[str]:
    '''Parse comma/newline separated URLs from a setting value.'''

    return [
        part.strip()
        for chunk in raw.splitlines()
        for part in chunk.split(',')
        if part.strip()
    ]


def _same_tiktok_origin(url: str, *, origin_url: str) -> bool:
    parsed = urlparse(urljoin(origin_url, url))
    origin = urlparse(origin_url)
    return (
        parsed.scheme in {'http', 'https'}
        and parsed.netloc == origin.netloc
    )


def normalise_explore_url(url: str, *, origin_url: str) -> str | None:
    '''Return an absolute TikTok explore/category URL, or None.'''

    absolute: str = urljoin(origin_url, url)
    if not _same_tiktok_origin(absolute, origin_url=origin_url):
        return None
    parsed = urlparse(absolute)
    path_with_query: str = parsed.path
    if parsed.query:
        path_with_query = f'{path_with_query}?{parsed.query}'
    if not _CATEGORY_PATH_RE.match(path_with_query):
        return None
    return absolute


def _normalise_category_text(text: object) -> str | None:
    if not isinstance(text, str):
        return None
    cleaned: str = re.sub(r'\s+', ' ', text).strip()
    if len(cleaned) < 2 or len(cleaned) > 64:
        return None
    if cleaned.startswith(('@', '#')):
        return None
    if cleaned.lower() in _IGNORED_CATEGORY_TEXTS:
        return None
    if _CATEGORY_METRIC_TEXT_RE.fullmatch(cleaned):
        return None
    return cleaned


def _category_search_url(text: str, *, origin_url: str) -> str:
    return urljoin(origin_url, f'/search?q={quote_plus(text)}')


def _normalise_languages(raw: str) -> tuple[str, ...]:
    languages: tuple[str, ...] = tuple(
        part.strip()
        for part in raw.split(',')
        if part.strip()
    )
    return languages or _DEFAULT_RANDOM_WORD_LANGUAGES


def _extract_words_from_random_payload(payload: Any) -> list[str]:
    if isinstance(payload, list):
        words: list[str] = []
        for item in payload:
            words.extend(_extract_words_from_random_payload(item))
        return words
    if isinstance(payload, dict):
        value: object = payload.get('word')
        return [value] if isinstance(value, str) else []
    return [payload] if isinstance(payload, str) else []


async def choose_random_search_terms(
    settings: TikTokDiscoverSearchSettings,
    *,
    count: int,
    random_word_url: str | None = None,
    random_word_language: str | None = None,
) -> list[str]:
    '''Return random search terms, falling back offline on failure.'''

    count = max(1, count)
    languages: tuple[str, ...] = _normalise_languages(
        settings.random_word_languages
    )
    language: str = random_word_language or random.choice(languages)
    url: str = random_word_url or settings.random_word_url
    params: dict[str, str | int] = {
        'number': count,
        'length': random.randint(5, 12),
    }
    if language != 'en':
        params['lang'] = language
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response: httpx.Response = await client.get(url, params=params)
            response.raise_for_status()
            words: list[str] = _extract_words_from_random_payload(
                response.json(),
            )
            usable: list[str] = [word for word in words if len(word) >= 5]
            if usable:
                if len(usable) < count:
                    usable.extend(
                        random.choice(_OFFLINE_RANDOM_TERMS)
                        for _ in range(count - len(usable))
                    )
                return usable[:count]
            raise ValueError('random-word API returned no usable word')
    except Exception as exc:
        fallback: list[str] = [
            random.choice(_OFFLINE_RANDOM_TERMS)
            for _ in range(count)
        ]
        _LOGGER.warning(
            'Random-word lookup failed; using offline fallback',
            extra={
                'error': repr(exc),
                'fallback': fallback,
                'language': language,
            },
        )
        return fallback


def _category_text_target(
    item: dict[str, str],
    *,
    origin_url: str,
) -> TikTokCategoryTarget | None:
    href: str = item.get('href', '')
    tag: str = item.get('tag', '')
    if href and tag == 'a':
        return None
    category_text: str | None = _normalise_category_text(item.get('text'))
    if category_text is None:
        return None
    return TikTokCategoryTarget(
        url=origin_url,
        label=category_text,
        click_menu=True,
    )


def _category_target_key(target: TikTokCategoryTarget) -> str:
    if target.click_menu and target.label is not None:
        return f'menu:{target.label.lower()}'
    return f'url:{target.url}'


def _append_default_category_targets(
    targets: list[TikTokCategoryTarget],
    *,
    explore_url: str,
    seen: set[str],
) -> None:
    for label in _DEFAULT_EXPLORE_CATEGORY_LABELS:
        target = TikTokCategoryTarget(
            url=explore_url,
            label=label,
            click_menu=True,
        )
        key: str = _category_target_key(target)
        if key in seen:
            continue
        seen.add(key)
        targets.append(target)


def append_search_term_targets(
    targets: list[TikTokCategoryTarget],
    terms: list[str],
    *,
    origin_url: str,
) -> None:
    '''Append TikTok search-result targets for *terms*.'''

    seen: set[str] = {
        _category_target_key(target)
        for target in targets
    }
    for term in terms:
        search_term: str = term.strip()
        if not search_term:
            continue
        target = TikTokCategoryTarget(
            url=_category_search_url(search_term, origin_url=origin_url),
        )
        key: str = _category_target_key(target)
        if key in seen:
            continue
        seen.add(key)
        targets.append(target)


def _json_record(user: DiscoveredTikTokUser) -> str:
    return json.dumps(
        {
            'platform': 'tiktok',
            'username': user.username,
            'category_url': user.category_url,
            'source_url': user.source_url,
            'discovered_at': user.discovered_at,
        },
        sort_keys=True,
    )


class JsonlUserSink:
    '''Append discovered TikTok usernames to a JSONL file.'''

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._seen: set[str] = set()
        self._load_existing()

    def _load_existing(self) -> None:
        if not self.path.exists():
            return
        with self.path.open('r', encoding='utf-8') as stream:
            for line in stream:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                username = payload.get('username')
                if isinstance(username, str):
                    normalised = _normalise_username(username)
                    if normalised is not None:
                        self._seen.add(normalised.lower())

    def append(self, user: DiscoveredTikTokUser) -> bool:
        key: str = user.username.lower()
        if key in self._seen:
            return False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open('a', encoding='utf-8') as stream:
            stream.write(f'{_json_record(user)}\n')
        self._seen.add(key)
        return True


def _playwright_proxy_config(proxy: str | None) -> dict[str, str] | None:
    if proxy is None:
        return None
    if proxy.startswith('local://'):
        raise ValueError(
            'Playwright cannot bind local:// proxy entries. Use an '
            'HTTP/SOCKS proxy entry for tt_discover_search.py.'
        )
    parsed = urlparse(proxy)
    if not parsed.scheme or not parsed.hostname:
        return {'server': proxy}
    server: str = f'{parsed.scheme}://{parsed.hostname}'
    if parsed.port is not None:
        server = f'{server}:{parsed.port}'
    config: dict[str, str] = {'server': server}
    if parsed.username is not None:
        config['username'] = parsed.username
    if parsed.password is not None:
        config['password'] = parsed.password
    return config


def _apply_camoufox_env(env: dict[str, object]) -> None:
    '''Stage Camoufox fingerprint env for the next browser launch.'''
    for key in list(os.environ):
        if key.startswith('CAMOU_CONFIG_'):
            del os.environ[key]
    for key, value in env.items():
        os.environ[key] = str(value)


def _camoufox_launch_kwargs(
    *,
    settings: TikTokDiscoverSearchSettings,
    proxy_config: dict[str, str] | None,
) -> dict[str, Any]:
    if camoufox_launch_options is None:
        raise RuntimeError(
            'Camoufox is required for tt_discover_search.py. '
            'Install dependencies with `uv sync --frozen` and run '
            '`uv run camoufox fetch`.'
        )
    opts: dict[str, Any] = camoufox_launch_options(
        headless=settings.headless,
        geoip=True,
        proxy=proxy_config,
        main_world_eval=True,
        block_webgl=False,
        humanize=True,
    )
    _apply_camoufox_env(opts.get('env') or {})
    return opts


async def _new_context(
    playwright: Playwright,
    settings: TikTokDiscoverSearchSettings,
    proxy: str | None,
) -> tuple[Browser, BrowserContext]:
    proxy_config: dict[str, str] | None = _playwright_proxy_config(proxy)
    browser: Browser = await playwright.firefox.launch(
        **_camoufox_launch_kwargs(
            settings=settings,
            proxy_config=proxy_config,
        ),
    )
    context: BrowserContext = await browser.new_context(
        user_agent=settings.user_agent,
        viewport={'width': 1280, 'height': 900},
        locale='en-US',
        timezone_id='UTC',
    )
    context.set_default_timeout(
        settings.navigation_timeout_seconds * 1000,
    )
    context.set_default_navigation_timeout(
        settings.navigation_timeout_seconds * 1000,
    )
    return browser, context


async def _close_browser_resources(
    context: BrowserContext | None,
    browser: Browser | None,
) -> None:
    '''Best-effort browser cleanup without masking scrape results.'''

    for label, resource in (
        ('browser_context', context),
        ('browser', browser),
    ):
        if resource is None:
            continue
        try:
            await resource.close()
        except PlaywrightError as exc:
            _LOGGER.warning(
                'TikTok discovery browser cleanup skipped; '
                'resource was already closed',
                extra={
                    'error': repr(exc),
                    'resource': label,
                },
            )
        except Exception as exc:
            _LOGGER.warning(
                'TikTok discovery browser cleanup failed',
                extra={
                    'error': repr(exc),
                    'resource': label,
                },
            )


async def _extract_users_from_page(
    page: Page,
    *,
    category_url: str,
) -> dict[str, DiscoveredTikTokUser]:
    users: dict[str, DiscoveredTikTokUser] = {}
    links: list[str] = await page.eval_on_selector_all(
        'a[href]',
        '''elements => elements
            .map(element => element.href)
            .filter(Boolean)''',
    )
    for href in links:
        username = extract_username_from_url(href)
        if username is None:
            continue
        users[username.lower()] = DiscoveredTikTokUser(
            username=username,
            category_url=category_url,
            source_url=href,
            discovered_at=_now_iso(),
        )

    text: str = await _body_inner_text_or_empty(
        page,
        category_url=category_url,
    )
    for username in extract_usernames_from_text(text):
        key: str = username.lower()
        users.setdefault(
            key,
            DiscoveredTikTokUser(
                username=username,
                category_url=category_url,
                source_url=None,
                discovered_at=_now_iso(),
            ),
        )

    raw_data_usernames: object = []
    try:
        raw_data_usernames = await page.evaluate(
            '''() => {
                const found = new Set();
                const usernameRe = /^[A-Za-z0-9_.]{2,24}$/;
                const keys = new Set([
                    'author',
                    'authorId',
                    'authorUniqueId',
                    'author_unique_id',
                    'nickname',
                    'uniqueId',
                    'user',
                    'username'
                ]);
                const add = value => {
                    if (typeof value !== 'string') {
                        return;
                    }
                    const cleaned = value.trim().replace(/^@+/, '');
                    if (usernameRe.test(cleaned)) {
                        found.add(cleaned);
                    }
                };
                const walk = (value, key, depth) => {
                    if (depth > 12 || value === null || value === undefined) {
                        return;
                    }
                    if (typeof value === 'string') {
                        if (keys.has(key)) {
                            add(value);
                        }
                        return;
                    }
                    if (Array.isArray(value)) {
                        value.forEach(item => walk(item, '', depth + 1));
                        return;
                    }
                    if (typeof value !== 'object') {
                        return;
                    }
                    Object.entries(value).forEach(([childKey, childValue]) => {
                        walk(childValue, childKey, depth + 1);
                    });
                };
                document.querySelectorAll('script').forEach(script => {
                    const text = script.textContent || '';
                    if (!text.includes('uniqueId')
                        && !text.includes('author')) {
                        return;
                    }
                    const texts = [text];
                    const start = text.indexOf('{');
                    const end = text.lastIndexOf('}');
                    if (start >= 0 && end > start) {
                        texts.push(text.slice(start, end + 1));
                    }
                    texts.forEach(candidate => {
                        try {
                            walk(JSON.parse(candidate), '', 0);
                        } catch {
                        }
                    });
                });
                return Array.from(found);
            }''',
        )
    except Exception as exc:
        _LOGGER.debug(
            'Could not extract TikTok usernames from page data',
            extra={'error': repr(exc)},
        )
    if isinstance(raw_data_usernames, list):
        for raw_username in raw_data_usernames:
            if not isinstance(raw_username, str):
                continue
            username: str | None = _normalise_username(raw_username)
            if username is None:
                continue
            key: str = username.lower()
            users.setdefault(
                key,
                DiscoveredTikTokUser(
                    username=username,
                    category_url=category_url,
                    source_url=None,
                    discovered_at=_now_iso(),
                ),
            )
    return users


async def _body_inner_text_or_empty(
    page: Page,
    *,
    category_url: str,
) -> str:
    try:
        return await page.locator('body').inner_text(timeout=5000)
    except PlaywrightTimeoutError:
        _LOGGER.warning(
            'Timed out reading TikTok page body text',
            extra={'category_url': category_url},
        )
        return ''


async def discover_category_targets(
    page: Page,
    *,
    explore_url: str,
) -> list[TikTokCategoryTarget]:
    '''Discover TikTok category links and menu tabs from explore.'''

    items: list[dict[str, str]] = await page.eval_on_selector_all(
        (
            'a[href], button, [role="button"], [role="link"], '
            '[role="tab"], [tabindex], div, span'
        ),
        '''elements => elements
            .map(element => ({
                href: element.getAttribute('href') || '',
                role: element.getAttribute('role') || '',
                tag: element.tagName.toLowerCase(),
                text: (element.textContent || '').trim(),
            }))
            .filter(item => item.href || item.text)''',
    )
    targets: list[TikTokCategoryTarget] = []
    seen: set[str] = set()
    for item in items:
        href: str = item.get('href', '')
        target: TikTokCategoryTarget | None = None
        if href:
            url: str | None = normalise_explore_url(
                href,
                origin_url=explore_url,
            )
            if url is not None:
                target = TikTokCategoryTarget(url=url)
        if target is None:
            target = _category_text_target(item, origin_url=explore_url)
        if target is None:
            continue
        key: str = _category_target_key(target)
        if key in seen:
            continue
        seen.add(key)
        targets.append(target)
    explore_target = TikTokCategoryTarget(url=explore_url)
    if _category_target_key(explore_target) not in seen:
        targets.insert(0, explore_target)
        seen.add(_category_target_key(explore_target))
    _append_default_category_targets(
        targets,
        explore_url=explore_url,
        seen=seen,
    )
    return targets


async def discover_category_urls(
    page: Page,
    *,
    explore_url: str,
) -> list[str]:
    '''Discover TikTok category URLs from the explore page.'''

    targets: list[TikTokCategoryTarget] = await discover_category_targets(
        page,
        explore_url=explore_url,
    )
    return [target.category_url for target in targets]


async def _check_bot_detection(
    page: Page,
    response: Response | None,
    *,
    limiter: TikTokBrowseRateLimiter,
    proxy: str | None,
    penalty_seconds: float,
) -> None:
    status_code: int | None = (
        response.status if response is not None else None
    )
    body_text: str = ''
    try:
        body_text = await page.locator('body').inner_text(timeout=5000)
    except Exception as exc:
        _LOGGER.debug(
            'Could not read TikTok page text for bot detection',
            extra={'error': repr(exc)},
        )
    if not is_tiktok_anti_bot_response(
        status_code=status_code,
        url=page.url,
        body_text=body_text,
    ):
        return
    if penalty_seconds > 0:
        await limiter.penalise(
            TikTokCallType.BROWSE,
            proxy,
            penalty_seconds,
        )
    raise RuntimeError(
        f'TikTok bot detection or rate limit seen at {page.url!r}'
    )


async def _goto_with_limit(
    page: Page,
    url: str,
    *,
    limiter: TikTokBrowseRateLimiter,
    proxy: str | None,
    settings: TikTokDiscoverSearchSettings,
) -> Response | None:
    await limiter.acquire(TikTokCallType.BROWSE, proxy=proxy)
    response: Response | None = await page.goto(
        url,
        wait_until='domcontentloaded',
        timeout=settings.navigation_timeout_seconds * 1000,
    )
    await page.wait_for_selector(
        'body',
        timeout=settings.navigation_timeout_seconds * 1000,
    )
    await _check_bot_detection(
        page,
        response,
        limiter=limiter,
        proxy=proxy,
        penalty_seconds=settings.bot_penalty_seconds,
    )
    return response


async def _click_category_menu_item(
    page: Page,
    label: str,
    *,
    limiter: TikTokBrowseRateLimiter,
    proxy: str | None,
    settings: TikTokDiscoverSearchSettings,
) -> bool:
    await limiter.acquire(TikTokCallType.BROWSE, proxy=proxy)
    clicked: bool = await page.evaluate(
        '''label => {
            const wanted = label.replace(/\\s+/g, ' ').trim().toLowerCase();
            const elements = Array.from(document.querySelectorAll('*'));
            const matches = elements.filter(element => {
                const text = (element.textContent || '')
                    .replace(/\\s+/g, ' ')
                    .trim()
                    .toLowerCase();
                return text === wanted;
            });
            matches.sort((left, right) => {
                return left.children.length - right.children.length;
            });
            const target = matches[0];
            if (!target) {
                return false;
            }
            const clickable = target.closest([
                'button',
                '[role="button"]',
                '[role="tab"]',
                'a',
                '[tabindex]'
            ].join(',')) || target;
            clickable.scrollIntoView({block: 'nearest', inline: 'center'});
            clickable.click();
            return true;
        }''',
        label,
    )
    if not clicked:
        _LOGGER.warning(
            'Could not find TikTok explore category menu item',
            extra={'category_label': label},
        )
        return False
    await page.wait_for_timeout(settings.scroll_wait_seconds * 1000)
    await _check_bot_detection(
        page,
        None,
        limiter=limiter,
        proxy=proxy,
        penalty_seconds=settings.bot_penalty_seconds,
    )
    return True


async def scrape_category(
    page: Page,
    target: TikTokCategoryTarget,
    *,
    limiter: TikTokBrowseRateLimiter,
    proxy: str | None,
    settings: TikTokDiscoverSearchSettings,
    sink: JsonlUserSink,
) -> TikTokCategoryScrapeResult:
    '''Navigate a TikTok category, scroll to the end, and emit users.'''

    await _goto_with_limit(
        page,
        target.url,
        limiter=limiter,
        proxy=proxy,
        settings=settings,
    )
    if target.click_menu and target.label is not None:
        _LOGGER.info(
            'Clicking TikTok explore category menu item',
            extra={
                'category_label': target.label,
                'category_url': target.category_url,
            },
        )
        clicked: bool = await _click_category_menu_item(
            page,
            target.label,
            limiter=limiter,
            proxy=proxy,
            settings=settings,
        )
        if not clicked:
            return TikTokCategoryScrapeResult(
                creators_found=0,
                creators_added=0,
                scrolls_scraped=0,
            )

    category_url: str = target.category_url
    found: set[str] = set()
    added: int = 0
    scrolls_scraped: int = 0
    idle_rounds: int = 0
    previous_height: int = 0
    previous_count: int = 0

    for scroll_index in range(settings.max_scrolls):
        users = await _extract_users_from_page(
            page,
            category_url=category_url,
        )
        for user in users.values():
            found.add(user.username.lower())
            if sink.append(user):
                added += 1
        scrolls_scraped = scroll_index + 1

        height: int = await page.evaluate(
            '() => document.documentElement.scrollHeight'
        )
        if height == previous_height and len(users) == previous_count:
            idle_rounds += 1
        else:
            idle_rounds = 0
        if idle_rounds >= settings.scroll_idle_rounds:
            break

        previous_height = height
        previous_count = len(users)
        await page.mouse.wheel(0, 1800)
        await page.wait_for_timeout(settings.scroll_wait_seconds * 1000)
        await _check_bot_detection(
            page,
            None,
            limiter=limiter,
            proxy=proxy,
            penalty_seconds=settings.bot_penalty_seconds,
        )
        _LOGGER.debug(
            'Scrolled TikTok category',
            extra={
                'category_url': category_url,
                'scroll_index': scroll_index,
                'height': height,
                'user_count': len(users),
            },
        )

    return TikTokCategoryScrapeResult(
        creators_found=len(found),
        creators_added=added,
        scrolls_scraped=scrolls_scraped,
    )


async def run(settings: TikTokDiscoverSearchSettings) -> int:
    '''Run the TikTok search discovery workflow.'''

    if async_playwright is None:
        raise RuntimeError(
            'Playwright is required for tt_discover_search.py. '
            'Install dependencies with `uv sync --frozen`.'
        )

    limiter = TikTokBrowseRateLimiter.get(
        state_dir=settings.rate_limiter_state_dir,
        redis_dsn=settings.redis_dsn,
    )
    limiter.set_proxies(list(settings.proxies) or None)
    proxy: str | None = await limiter.acquire(TikTokCallType.BROWSE)
    sink = JsonlUserSink(settings.output_file)

    async with async_playwright() as playwright:
        browser: Browser | None = None
        context: BrowserContext | None = None
        try:
            browser, context = await _new_context(
                playwright,
                settings,
                proxy,
            )
            page: Page = await context.new_page()
            explicit_urls: list[str] = parse_url_list(
                settings.category_urls
            )
            if explicit_urls:
                category_targets = [
                    TikTokCategoryTarget(url=url)
                    for url in explicit_urls
                ]
            else:
                await _goto_with_limit(
                    page,
                    settings.explore_url,
                    limiter=limiter,
                    proxy=proxy,
                    settings=settings,
                )
                category_targets = await discover_category_targets(
                    page,
                    explore_url=settings.explore_url,
                )
                _LOGGER.info(
                    'Discovered TikTok Explore categories',
                    extra={'categories': len(category_targets)},
                )
            if settings.max_categories:
                category_targets = category_targets[:settings.max_categories]
            if settings.search_term_count > 0:
                search_terms: list[str] = await choose_random_search_terms(
                    settings,
                    count=settings.search_term_count,
                )
                append_search_term_targets(
                    category_targets,
                    search_terms,
                    origin_url=settings.explore_url,
                )
                _LOGGER.info(
                    'Added TikTok random search targets',
                    extra={
                        'search_term_count': len(search_terms),
                        'search_terms': search_terms,
                    },
                )

            total_found: int = 0
            total_added: int = 0
            failed_targets: int = 0
            for target in category_targets:
                _LOGGER.info(
                    'Scraping TikTok discovery target',
                    extra={
                        'category_label': target.label,
                        'category_url': target.category_url,
                        'click_menu': target.click_menu,
                    },
                )
                try:
                    result: TikTokCategoryScrapeResult = (
                        await scrape_category(
                            page,
                            target,
                            limiter=limiter,
                            proxy=proxy,
                            settings=settings,
                            sink=sink,
                        )
                    )
                except Exception as exc:
                    failed_targets += 1
                    _LOGGER.exception(
                        'TikTok discovery target failed',
                        exc_info=True,
                        extra={
                            'category_label': target.label,
                            'category_url': target.category_url,
                            'click_menu': target.click_menu,
                            'error': repr(exc),
                        },
                    )
                    continue
                total_found += result.creators_found
                total_added += result.creators_added
                _LOGGER.info(
                    'Scraped TikTok discovery target creators',
                    extra={
                        'category_label': target.label,
                        'category_url': target.category_url,
                        'click_menu': target.click_menu,
                        'creators_found': result.creators_found,
                        'creators_added': result.creators_added,
                        'scrolls_scraped': result.scrolls_scraped,
                    },
                )
            _LOGGER.info(
                'TikTok search discovery finished',
                extra={
                    'categories': len(category_targets),
                    'creators_found': total_found,
                    'creators_added': total_added,
                    'failed_targets': failed_targets,
                    'output_file': settings.output_file,
                },
            )
            return 1 if failed_targets else 0
        except Exception as exc:
            _LOGGER.exception(
                'TikTok search discovery failed',
                exc_info=True,
                extra={'error': repr(exc)},
            )
            return 1
        finally:
            await _close_browser_resources(context, browser)


async def main_async(argv: list[str] | None = None) -> int:
    try:
        settings = TikTokDiscoverSearchSettings(_cli_parse_args=argv)
        configure_logging(
            level=settings.log_level,
            filename=settings.log_file,
            log_format=settings.log_format,
        )
        return await run(settings)
    except Exception as exc:
        _LOGGER.exception(
            'TikTok search discovery exited with an unhandled error',
            exc_info=True,
            extra={'error': repr(exc)},
        )
        return 1


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == '__main__':
    main()
