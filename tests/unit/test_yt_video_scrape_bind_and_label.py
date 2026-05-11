'''
Unit tests for video-scraper bind_failed classification.

The video scraper inspects the exception cause chain and routes
OSError(EADDRNOTAVAIL) — raised by the kernel when a ``local://``
egress IP is not bound on this host — to the ``bind_failed``
reason bucket instead of letting the message-based classifier
fall through to ``other``.
'''

import errno
import unittest

from tools.yt_video_scrape import (
    _classify_scrape_error,
    _is_bind_failure,
)


class TestIsBindFailure(unittest.TestCase):

    def test_direct_oserror_eaddrnotavail(self) -> None:
        exc: OSError = OSError(
            errno.EADDRNOTAVAIL,
            'Cannot assign requested address',
        )
        self.assertTrue(_is_bind_failure(exc))

    def test_oserror_other_errno_is_not_bind(self) -> None:
        exc: OSError = OSError(
            errno.ECONNREFUSED, 'Connection refused',
        )
        self.assertFalse(_is_bind_failure(exc))

    def test_wrapped_via_cause(self) -> None:
        inner: OSError = OSError(
            errno.EADDRNOTAVAIL, 'bind',
        )
        outer: RuntimeError = RuntimeError('upstream')
        outer.__cause__ = inner
        self.assertTrue(_is_bind_failure(outer))

    def test_wrapped_via_context(self) -> None:
        inner: OSError = OSError(
            errno.EADDRNOTAVAIL, 'bind',
        )
        try:
            try:
                raise inner
            except OSError:
                raise RuntimeError('outer')
        except RuntimeError as exc:
            self.assertTrue(_is_bind_failure(exc))

    def test_unrelated_exception_is_not_bind(self) -> None:
        exc: ValueError = ValueError('not a bind failure')
        self.assertFalse(_is_bind_failure(exc))

    def test_self_referential_cause_does_not_loop(self) -> None:
        '''Defensive: a malformed cause chain that points back to
        itself must not hang the classifier.'''
        exc: RuntimeError = RuntimeError('cycle')
        exc.__cause__ = exc
        self.assertFalse(_is_bind_failure(exc))


class TestClassifyScrapeError(unittest.TestCase):

    def test_bind_failed_takes_precedence_over_message(self) -> None:
        '''Even if the message would match a yt-dlp pattern, an
        OSError(EADDRNOTAVAIL) cause routes to bind_failed.'''
        inner: OSError = OSError(
            errno.EADDRNOTAVAIL,
            'Cannot assign requested address',
        )
        outer: RuntimeError = RuntimeError(
            'rate-limited by YouTube',
        )
        outer.__cause__ = inner
        self.assertEqual(
            _classify_scrape_error(outer), 'bind_failed',
        )

    def test_falls_through_to_yt_dlp_classifier(self) -> None:
        exc: RuntimeError = RuntimeError('HTTP Error 429')
        self.assertEqual(
            _classify_scrape_error(exc), 'rate_limit',
        )

    def test_unrecognised_falls_to_other(self) -> None:
        exc: RuntimeError = RuntimeError('mystery')
        self.assertEqual(
            _classify_scrape_error(exc), 'other',
        )


if __name__ == '__main__':
    unittest.main()
