import unittest
from unittest.mock import patch

from tools.tt_creator_upload import (
    CreatorUploadSettings,
    _load_creator_record,
)
from tools.tt_video_upload import (
    VideoUploadSettings,
    _load_video_record,
)


class TestTikTokUploadSettings(unittest.TestCase):
    def test_uses_tiktok_schema_defaults_not_youtube_globals(self) -> None:
        with patch.dict(
            'os.environ',
            {
                'SCHEMA_OWNER': 'boinko',
                'SCHEMA_VERSION': '0.0.2',
            },
            clear=False,
        ):
            settings = CreatorUploadSettings(_cli_parse_args=[])
        self.assertEqual(settings.schema_owner, 'drand')
        self.assertEqual(settings.schema_version, '0.0.1')

    def test_tiktok_schema_env_overrides_defaults(self) -> None:
        with patch.dict(
            'os.environ',
            {
                'TIKTOK_SCHEMA_OWNER': 'alice',
                'TIKTOK_SCHEMA_VERSION': '9.9.9',
            },
            clear=False,
        ):
            settings = VideoUploadSettings(_cli_parse_args=[])
        self.assertEqual(settings.schema_owner, 'alice')
        self.assertEqual(settings.schema_version, '9.9.9')

    def test_upload_settings_ignore_missing_proxy_files(self) -> None:
        with patch.dict(
            'os.environ',
            {'PROXY_FILES': '/data/proxies/vpn.proxies.lst'},
            clear=False,
        ):
            settings = CreatorUploadSettings(_cli_parse_args=[])
        self.assertEqual(settings.proxies, [])


class TestTikTokUploadRecordLoaders(unittest.TestCase):
    def test_creator_loader_round_trips_valid_record(self) -> None:
        record = _load_creator_record({
            'username': 'alice',
            'sec_uid': 'sec',
            'user_id': '123',
            'verified': False,
            'private_account': False,
            'follower_count': 1,
            'following_count': 2,
            'heart_count': 3,
            'video_count': 4,
            'friend_count': 5,
            'url': 'https://www.tiktok.com/@alice',
            'scraped_timestamp': '2026-06-14T00:00:00Z',
        })
        self.assertEqual(record['username'], 'alice')
        self.assertEqual(record['videos'], [])

    def test_creator_loader_replaces_embedded_videos_with_empty_list(
        self,
    ) -> None:
        record = _load_creator_record({
            'username': 'alice',
            'sec_uid': 'sec',
            'user_id': '123',
            'verified': False,
            'private_account': False,
            'follower_count': 1,
            'following_count': 2,
            'heart_count': 3,
            'video_count': 4,
            'friend_count': 5,
            'videos': [{
                'video_id': '456',
                'username': 'alice',
                'url': 'https://www.tiktok.com/@alice/video/456',
            }],
            'url': 'https://www.tiktok.com/@alice',
            'scraped_timestamp': '2026-06-14T00:00:00Z',
        })
        self.assertEqual(record['videos'], [])

    def test_video_loader_round_trips_valid_record(self) -> None:
        record = _load_video_record({
            'video_id': '123',
            'username': 'alice',
            'sec_uid': 'sec',
            'created_timestamp': '2026-06-14T00:00:00Z',
            'url': 'https://www.tiktok.com/@alice/video/123',
            'scraped_timestamp': '2026-06-14T00:00:00Z',
        })
        self.assertEqual(record['video_id'], '123')

    def test_video_loader_accepts_author_avatar_url(self) -> None:
        record = _load_video_record({
            'video_id': '123',
            'username': 'alice',
            'sec_uid': 'sec',
            'author_avatar_url': 'https://example.com/avatar.jpeg',
            'created_timestamp': '2026-06-14T00:00:00Z',
            'url': 'https://www.tiktok.com/@alice/video/123',
            'scraped_timestamp': '2026-06-14T00:00:00Z',
        })
        self.assertEqual(
            record['author_avatar_url'],
            'https://example.com/avatar.jpeg',
        )


if __name__ == '__main__':
    unittest.main()
