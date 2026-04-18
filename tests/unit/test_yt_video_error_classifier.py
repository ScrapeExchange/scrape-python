'''
Unit tests for the yt-dlp error classifier in tools/yt_video_scrape.py.

The classifier maps a yt-dlp error message string to one of the
reason buckets used by the ``yt_video_proxy_scrape_failures_total``
Prometheus counter. Six branches in the original ``_scrape``
function were silently dead because their substrings contained
uppercase characters compared against ``str(exc).lower()`` — these
tests pin every reason against representative real-world yt-dlp
messages so that case bug never returns.
'''

import importlib.util
import unittest

from pathlib import Path
from types import ModuleType


def _load_yt_video_scrape() -> ModuleType:
    '''
    Load tools/yt_video_scrape.py as a module.
    ``tools/`` is not a Python package so a normal
    ``import`` would fail; load it directly from
    the file path instead.

    The module is cached in ``sys.modules`` so that
    repeated loads (e.g. VS Code test discovery
    running all test files in one process) do not
    re-register Prometheus metrics and trigger a
    ``Duplicated timeseries`` error.
    '''

    import sys
    if 'yt_video_scrape' in sys.modules:
        return sys.modules['yt_video_scrape']

    repo_root: Path = (
        Path(__file__).resolve().parents[2]
    )
    module_path: Path = (
        repo_root / 'tools' / 'yt_video_scrape.py'
    )
    spec = importlib.util.spec_from_file_location(
        'yt_video_scrape', module_path,
    )
    assert (
        spec is not None
        and spec.loader is not None
    )
    module: ModuleType = (
        importlib.util.module_from_spec(spec)
    )
    sys.modules['yt_video_scrape'] = module
    spec.loader.exec_module(module)
    return module


yt_video_scrape: ModuleType = _load_yt_video_scrape()
classify = yt_video_scrape._classify_yt_dlp_error
next_failure_sleep = yt_video_scrape._next_failure_sleep
FAILURE_SLEEP_MIN: int = yt_video_scrape.FAILURE_SLEEP_MIN
FAILURE_SLEEP_MAX: int = yt_video_scrape.FAILURE_SLEEP_MAX


class TestRateLimitClassification(unittest.TestCase):

    def test_rate_limited_by_youtube(self) -> None:
        self.assertEqual(
            classify('ERROR: rate-limited by YouTube'),
            'rate_limit',
        )

    def test_vpn_proxy_detected_uppercase(self) -> None:
        '''Regression: this exact substring was dead before the fix.'''
        self.assertEqual(
            classify('ERROR: VPN/Proxy Detected, please try again'),
            'rate_limit',
        )

    def test_youtube_blocked_uppercase(self) -> None:
        '''Regression: was dead before the fix.'''
        self.assertEqual(
            classify('ERROR: YouTube blocked this request'),
            'rate_limit',
        )

    def test_captcha(self) -> None:
        self.assertEqual(
            classify('ERROR: Captcha challenge required'),
            'rate_limit',
        )

    def test_try_again_later(self) -> None:
        self.assertEqual(
            classify('ERROR: try again later'),
            'rate_limit',
        )

    def test_page_needs_to_be_reloaded(self) -> None:
        self.assertEqual(
            classify('ERROR: The page needs to be reloaded'),
            'rate_limit',
        )

    def test_429_response(self) -> None:
        self.assertEqual(
            classify('HTTP Error 429: Too Many Requests'),
            'rate_limit',
        )

    def test_429_with_leading_space(self) -> None:
        self.assertEqual(
            classify('failed with status 429 retrying'),
            'rate_limit',
        )

    def test_expected_string_or_bytes_like_object(self) -> None:
        '''Bot-detection signature documented in commit 9aeef12.'''
        self.assertEqual(
            classify(
                "TypeError: expected string or bytes-like object",
            ),
            'rate_limit',
        )


class TestMissingDataClassification(unittest.TestCase):

    def test_missing_microformat_data_uppercase(self) -> None:
        '''Regression: was dead before the fix.'''
        self.assertEqual(
            classify('ERROR: Missing microformat data for VIDEO_ID'),
            'missing_data',
        )

    def test_missing_microformat_data_lowercase(self) -> None:
        self.assertEqual(
            classify('error: missing microformat data'),
            'missing_data',
        )


class TestUnavailableClassification(unittest.TestCase):

    def test_video_is_private(self) -> None:
        self.assertEqual(
            classify('ERROR: This video is private'),
            'unavailable',
        )

    def test_video_has_been_removed(self) -> None:
        self.assertEqual(
            classify('ERROR: This video has been removed'),
            'unavailable',
        )

    def test_video_is_age_restricted(self) -> None:
        self.assertEqual(
            classify('ERROR: video is age restricted'),
            'unavailable',
        )

    def test_modern_age_gate(self) -> None:
        self.assertEqual(
            classify('ERROR: Sign in to confirm your age'),
            'unavailable',
        )

    def test_members_only_level(self) -> None:
        self.assertEqual(
            classify(
                "ERROR: This video is available to this channel's "
                'members on level 1 and above',
            ),
            'unavailable',
        )

    def test_members_only_content(self) -> None:
        self.assertEqual(
            classify('ERROR: members-only content'),
            'unavailable',
        )

    def test_live_event_will_begin(self) -> None:
        self.assertEqual(
            classify('ERROR: This live event will begin in 2 hours'),
            'unavailable',
        )

    def test_live_event_has_ended(self) -> None:
        self.assertEqual(
            classify('ERROR: This live event has ended'),
            'unavailable',
        )

    def test_live_stream_recording_not_available(self) -> None:
        self.assertEqual(
            classify(
                'ERROR: live stream recording is not available',
            ),
            'unavailable',
        )

    def test_music_premium_uppercase(self) -> None:
        '''Regression: was dead before the fix.'''
        self.assertEqual(
            classify('ERROR: This video requires Music Premium'),
            'unavailable',
        )

    def test_video_unavailable(self) -> None:
        self.assertEqual(
            classify('ERROR: Video unavailable'),
            'unavailable',
        )

    def test_video_is_not_available(self) -> None:
        self.assertEqual(
            classify('ERROR: This video is not available'),
            'unavailable',
        )

    def test_geo_block_uploader_variant(self) -> None:
        '''Original substring matched this variant only.'''
        self.assertEqual(
            classify(
                'ERROR: The uploader has not made this video '
                'available in your country',
            ),
            'unavailable',
        )

    def test_geo_block_is_not_available_variant(self) -> None:
        '''Second geo-block variant the original substring missed.'''
        self.assertEqual(
            classify(
                'ERROR: This video is not available in your country',
            ),
            'unavailable',
        )

    def test_copyright(self) -> None:
        self.assertEqual(
            classify('ERROR: Removed for copyright infringement'),
            'unavailable',
        )

    def test_offline_with_period_uppercase(self) -> None:
        '''Regression: ``Offline.`` was dead before the fix.'''
        self.assertEqual(
            classify('ERROR: Video unavailable. Offline.'),
            'unavailable',
        )


class TestPremiereClassification(unittest.TestCase):

    def test_premieres_uppercase(self) -> None:
        '''Regression: was dead before the fix.'''
        self.assertEqual(
            classify('ERROR: Premieres in 30 minutes'),
            'premiere',
        )

    def test_premiere_singular(self) -> None:
        self.assertEqual(
            classify('ERROR: This is a Premiere'),
            'premiere',
        )


class TestTransientClassification(unittest.TestCase):

    def test_offline_no_period(self) -> None:
        '''
        Bare ``offline`` (no trailing period) is treated as a
        transport-level transient error rather than the
        ``Video unavailable. Offline.`` content state.
        '''
        self.assertEqual(
            classify('ConnectionError: connection went offline'),
            'transient',
        )

    def test_timed_out(self) -> None:
        self.assertEqual(
            classify('ReadTimeoutError: timed out'),
            'transient',
        )

    def test_ssl_error(self) -> None:
        self.assertEqual(
            classify('SSLError: certificate verify failed'),
            'transient',
        )

    def test_ssl_colon(self) -> None:
        self.assertEqual(
            classify('ssl: handshake failed'),
            'transient',
        )

    def test_unable_to_connect_to_proxy(self) -> None:
        self.assertEqual(
            classify(
                'urllib.error.URLError: unable to connect to proxy',
            ),
            'transient',
        )


class TestOtherFallback(unittest.TestCase):

    def test_unrecognised_error_falls_through(self) -> None:
        self.assertEqual(
            classify('ERROR: completely unexpected gibberish'),
            'other',
        )

    def test_empty_string(self) -> None:
        self.assertEqual(classify(''), 'other')


class TestDeadPatternGuard(unittest.TestCase):

    def test_all_patterns_are_lowercase(self) -> None:
        '''
        Every substring in :data:`_ERROR_PATTERNS` must already be
        lowercase. The classifier compares against ``str(exc).lower()``
        so any uppercase character would silently kill the branch —
        which is exactly the bug this test exists to prevent
        regressing.
        '''
        for reason, patterns in yt_video_scrape._ERROR_PATTERNS:
            for pattern in patterns:
                self.assertEqual(
                    pattern, pattern.lower(),
                    f'{pattern!r} under reason {reason!r} contains '
                    f'uppercase characters and would never match',
                )

    def test_known_reasons_only(self) -> None:
        '''Pin the set of reason names so dashboard panels and the
        Prometheus label values stay in sync with the code.'''
        expected: set[str] = {
            'rate_limit', 'missing_data', 'unavailable',
            'premiere', 'transient',
        }
        actual: set[str] = {
            reason for reason, _ in yt_video_scrape._ERROR_PATTERNS
        }
        self.assertEqual(actual, expected)


class TestNextFailureSleep(unittest.TestCase):

    def test_zero_starts_at_failure_sleep_min(self) -> None:
        self.assertEqual(
            next_failure_sleep(0), FAILURE_SLEEP_MIN,
        )

    def test_below_min_snaps_to_min(self) -> None:
        self.assertEqual(
            next_failure_sleep(FAILURE_SLEEP_MIN - 1),
            FAILURE_SLEEP_MIN,
        )

    def test_at_min_doubles(self) -> None:
        self.assertEqual(
            next_failure_sleep(FAILURE_SLEEP_MIN),
            min(FAILURE_SLEEP_MIN * 2, FAILURE_SLEEP_MAX),
        )

    def test_doubling_sequence(self) -> None:
        # Pin the streak: 60 → 120 → 240 → 300 (capped).
        seq: list[int] = []
        s: int = 0
        for _ in range(5):
            s = next_failure_sleep(s)
            seq.append(s)
        self.assertEqual(seq, [60, 120, 240, 300, 300])

    def test_clamps_at_failure_sleep_max(self) -> None:
        self.assertEqual(
            next_failure_sleep(FAILURE_SLEEP_MAX),
            FAILURE_SLEEP_MAX,
        )
        self.assertEqual(
            next_failure_sleep(FAILURE_SLEEP_MAX * 4),
            FAILURE_SLEEP_MAX,
        )

    def test_reset_via_zero_restarts_streak(self) -> None:
        '''A success / unavailable / premiere path that sets sleep=0
        must reset the backoff — the next failure should start at
        ``FAILURE_SLEEP_MIN`` again, not the mid-streak value.'''
        mid: int = next_failure_sleep(next_failure_sleep(0))  # 120
        self.assertEqual(mid, 120)
        # Reset to 0 (simulating a success / unavailable result)
        # and assert the next failure starts clean.
        after_reset: int = next_failure_sleep(0)
        self.assertEqual(after_reset, FAILURE_SLEEP_MIN)


if __name__ == '__main__':
    unittest.main()
