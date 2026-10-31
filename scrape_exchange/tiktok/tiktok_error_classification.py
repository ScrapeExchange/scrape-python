'''
Classify TikTok-stack exceptions into a small fixed taxonomy
shared with the YouTube failure metric. Mirrors
``_classify_yt_dlp_error`` in ``tools/yt_video_scrape.py``.

The classifier is a flat ordered list of
``(reason, [substring, ...])``. The first reason whose
substring list contains a substring of the lowercased error
string wins. Anything that matches no entry is ``other``.

All substrings MUST be pre-lowercased; an assertion at module
import enforces this so a regression caught in tests rather
than silently mis-classifying.

:maintainer : Boinko <boinko@scrape.exchange>
:copyright  : Copyright 2026
:license    : GPLv3
'''


_ERROR_PATTERNS: list[tuple[str, list[str]]] = [
    # TikTok user-detail statuses that belong to the account, not the
    # session: the same code comes back from any proxy and session.
    # 10221 (banned), 10202 (does not exist), 209002 (unavailable;
    # meaning unconfirmed). Checked before rate_limit, whose
    # 'invalid tiktok user detail response' would otherwise claim
    # them, open the bot-detection circuit and retry the creator.
    # The trailing space anchors the code in 'status=<code> missing='.
    ('unavailable', [
        'status=10221 ',
        'status=10202 ',
        'status=209002 ',
    ]),
    ('rate_limit', [
        'emptyresponseexception',
        'tiktok bot detection',
        "detecting you're a bot",
        'invalid tiktok user detail response',
        "status=none missing='id'",
        "status=0 missing='user'",
        'status=10201',
        "'user'",
        "'id'",
        'tiktokexception.__init__() missing',
        'html response body',
        'empty response body',
        'http 429',
        'too many requests',
        'captcha',
        'verify_security',
        'rate limit',
    ]),
    ('unavailable', [
        'user not found',
        'video not available',
        'video does not exist',
        'private account',
        'account not found',
    ]),
    ('auth', [
        'ms_token rejected',
        'invalid ms_token',
        'session expired',
    ]),
    ('signing', [
        'signer.js',
        'failed to sign',
        'failed to acquire signature',
    ]),
    ('transient', [
        'invalid tiktok profile identity response',
        'invalid tiktok video list response',
        'timeout',
        'connection reset',
        'connection refused',
        'browser has been closed',
        'navigation timeout',
        'page crash',
    ]),
]


# Defensive invariant: every substring must already be lowercase.
for _reason, _subs in _ERROR_PATTERNS:
    for _s in _subs:
        assert _s == _s.lower(), (
            f'{_reason!r}: pattern {_s!r} is not lowercase'
        )


def classify_tiktok_error(exc: BaseException) -> str:
    '''Return one of: rate_limit, unavailable, auth, signing,
    transient, other.'''
    msg: str = str(exc).lower()
    for reason, subs in _ERROR_PATTERNS:
        for sub in subs:
            if sub in msg:
                return reason
    return 'other'
