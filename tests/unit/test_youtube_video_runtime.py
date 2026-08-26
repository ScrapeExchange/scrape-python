'''Tests for yt-dlp runtime setup used by YouTube video scraping.'''

import unittest
from unittest.mock import MagicMock, patch

from scrape_exchange.youtube import youtube_video


class TestResolveDenoPath(unittest.TestCase):
    def test_expands_configured_path(self) -> None:
        with patch.object(
            youtube_video.os.path,
            'expandvars',
            return_value='~/.deno/bin/deno',
        ), patch.object(
            youtube_video.os.path,
            'expanduser',
            return_value='/runtime/.deno/bin/deno',
        ), patch.object(
            youtube_video.os.path,
            'isfile',
            return_value=True,
        ), patch.object(
            youtube_video.os,
            'access',
            return_value=True,
        ):
            result: str = youtube_video._resolve_deno_path(
                '$RUNTIME_HOME/.deno/bin/deno',
            )

        self.assertEqual(result, '/runtime/.deno/bin/deno')

    def test_uses_configured_executable_first(self) -> None:
        configured: str = '../../.deno/bin/deno'
        with patch.object(
            youtube_video.os.path,
            'isfile',
            return_value=True,
        ), patch.object(
            youtube_video.os,
            'access',
            return_value=True,
        ), patch.object(
            youtube_video.shutil,
            'which',
        ) as which:
            result: str = youtube_video._resolve_deno_path(configured)

        self.assertEqual(result, configured)
        which.assert_not_called()

    def test_falls_back_to_deno_on_path(self) -> None:
        with patch.object(
            youtube_video.os.path,
            'isfile',
            return_value=False,
        ), patch.object(
            youtube_video.shutil,
            'which',
            return_value='/usr/local/deno/bin/deno',
        ):
            result: str = youtube_video._resolve_deno_path(
                '../../.deno/bin/deno',
            )

        self.assertEqual(result, '/usr/local/deno/bin/deno')

    def test_missing_deno_names_configuration_and_path(self) -> None:
        with patch.object(
            youtube_video.os.path,
            'isfile',
            return_value=False,
        ), patch.object(
            youtube_video.shutil,
            'which',
            return_value=None,
        ):
            with self.assertRaisesRegex(
                ValueError,
                'DENO_PATH.*missing-deno.*PATH',
            ):
                youtube_video._resolve_deno_path('missing-deno')


class TestDownloadClientOptions(unittest.TestCase):
    def test_uses_default_clients_and_keeps_po_token_provider(self) -> None:
        download_client: MagicMock = MagicMock()
        with patch.object(
            youtube_video,
            '_resolve_deno_path',
            return_value='/usr/local/deno/bin/deno',
        ), patch.object(
            youtube_video,
            'YoutubeDL',
            return_value=download_client,
        ) as youtube_dl:
            result: object = youtube_video.YouTubeVideo._setup_download_client(
                '../../.deno/bin/deno',
                'http://localhost:4416',
            )

        self.assertIs(result, download_client)
        options: dict = youtube_dl.call_args.args[0]
        self.assertEqual(
            options['js_runtimes']['deno']['path'],
            '/usr/local/deno/bin/deno',
        )
        youtube_args: dict = options['extractor_args']['youtube']
        self.assertNotIn('player-client', youtube_args)
        self.assertNotIn('player_client', youtube_args)
        self.assertEqual(
            youtube_args['youtubepot-bgutilhttp:base_url'],
            'http://localhost:4416',
        )


if __name__ == '__main__':
    unittest.main()
