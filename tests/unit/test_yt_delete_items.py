'''
Unit tests for the bulk-delete tool's filter-body construction.

The body translation from settings to request JSON is the only
piece of pure logic in the tool worth testing in isolation; the
HTTP layer is exercised against a real server in functional
tests when needed.
'''

import unittest
from unittest.mock import patch


# Environment variables that pydantic-settings would otherwise
# pull from the developer's .env / shell when constructing
# DeleteItemsSettings. Cleared per test to keep filter-body
# behaviour deterministic across machines.
_ISOLATED_ENV: dict[str, str] = {
    'API_KEY_ID': 'test',
    'API_KEY_SECRET': 'test',
}


class TestBuildFilterBody(unittest.TestCase):

    def setUp(self) -> None:
        # ``clear=True`` wipes the inherited environment for the
        # duration of the test so .env-supplied filter defaults
        # don't bleed into the body.
        self._env_patch = patch.dict(
            'os.environ', _ISOLATED_ENV, clear=True,
        )
        self._env_patch.start()

    def tearDown(self) -> None:
        self._env_patch.stop()

    def test_omits_unset_fields(self) -> None:
        '''Unset filter fields must not appear in the body so the
        server's ``if filters.X`` checks treat them as absent.'''
        from tools.yt_delete_items import (
            DeleteItemsSettings,
            _build_filter_body,
        )

        settings: DeleteItemsSettings = DeleteItemsSettings(
            _cli_parse_args=False,
            _env_file=None,
            schema_owner='boinko',
            platform='youtube',
        )
        body: dict[str, str] = _build_filter_body(settings)
        self.assertEqual(
            body, {'schema_owner': 'boinko', 'platform': 'youtube'},
        )

    def test_includes_all_filter_fields(self) -> None:
        '''Every filter field, when set, ends up in the body.'''
        from tools.yt_delete_items import (
            DeleteItemsSettings,
            _build_filter_body,
        )

        settings: DeleteItemsSettings = DeleteItemsSettings(
            _cli_parse_args=False,
            _env_file=None,
            schema_owner='boinko',
            platform='youtube',
            entity='channel',
            schema_version='0.0.2',
            platform_content_id='UC1234567890abcdefghij',
            platform_creator_id='somehandle',
            platform_topic_id='topic-abc',
        )
        body: dict[str, str] = _build_filter_body(settings)
        self.assertEqual(
            body,
            {
                'schema_owner': 'boinko',
                'platform': 'youtube',
                'entity': 'channel',
                'version': '0.0.2',
                'platform_content_id': 'UC1234567890abcdefghij',
                'platform_creator_id': 'somehandle',
                'platform_topic_id': 'topic-abc',
            },
        )

    def test_empty_when_nothing_set(self) -> None:
        '''A bare DeleteItemsSettings emits no filter keys.'''
        from tools.yt_delete_items import (
            DeleteItemsSettings,
            _build_filter_body,
        )

        body: dict[str, str] = _build_filter_body(
            DeleteItemsSettings(
                _cli_parse_args=False,
                _env_file=None,
            ),
        )
        self.assertEqual(body, {})


if __name__ == '__main__':
    unittest.main()
