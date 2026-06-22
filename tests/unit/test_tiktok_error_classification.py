'''
Unit tests for classify_tiktok_error and the lowercase-substring
invariant.
'''

import unittest

from scrape_exchange.tiktok.tiktok_error_classification import (
    _ERROR_PATTERNS,
    classify_tiktok_error,
)


class TestClassifier(unittest.TestCase):

    def test_rate_limit_via_empty_response(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('EmptyResponseException received'),
            ),
            'rate_limit',
        )

    def test_rate_limit_via_bot_detection_message(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception(
                    "TikTok returned an empty response. They are "
                    "detecting you're a bot",
                ),
            ),
            'rate_limit',
        )

    def test_rate_limit_via_empty_user_info(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception(
                    "invalid TikTok user detail response: "
                    "status=None missing='id'",
                ),
            ),
            'rate_limit',
        )

    def test_rate_limit_via_missing_user_status_zero(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception(
                    "invalid TikTok user detail response: "
                    "status=0 missing='user'",
                ),
            ),
            'rate_limit',
        )

    def test_rate_limit_via_status_10201(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception(
                    "invalid TikTok user detail response: "
                    "status=10201 missing='user'",
                ),
            ),
            'rate_limit',
        )

    def test_rate_limit_via_429(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('Got HTTP 429 too many requests'),
            ),
            'rate_limit',
        )

    def test_rate_limit_via_captcha(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('captcha challenge presented'),
            ),
            'rate_limit',
        )

    def test_unavailable_via_user_not_found(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('User not found in response'),
            ),
            'unavailable',
        )

    def test_transient_via_timeout(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('Navigation timeout exceeded'),
            ),
            'transient',
        )

    def test_auth_via_ms_token_rejected(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('ms_token rejected by server'),
            ),
            'auth',
        )

    def test_signing_via_signer_failure(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('signer.js failure to sign request'),
            ),
            'signing',
        )

    def test_other_fallback(self) -> None:
        self.assertEqual(
            classify_tiktok_error(
                Exception('something completely different'),
            ),
            'other',
        )

    def test_all_substrings_lowercase(self) -> None:
        for reason, substrings in _ERROR_PATTERNS:
            for s in substrings:
                self.assertEqual(
                    s, s.lower(),
                    msg=f'{reason!r} pattern {s!r} not lowercase',
                )


if __name__ == '__main__':
    unittest.main()
