'''Anonymous response filtering and bounded browser cleanup.'''

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scrape_exchange.twitch.settings import TwitchScraperSettings
from scrape_exchange.twitch.twitch_browser import (
    fetch_profile,
    profile_operations,
    select_profile_responses,
)
from scrape_exchange.twitch.twitch_error_classification import (
    ProfileRateLimitError,
)


class TestResponseSelection(unittest.TestCase):
    def test_panels_use_id_addressing_but_response_must_match_login(
        self,
    ) -> None:
        request: dict = {
            'operationName': 'ChannelPanels', 'variables': {'id': '123'},
        }
        response: dict = {'data': {'user': {
            'id': '123', 'login': 'example', 'panels': [],
        }}}
        self.assertEqual(select_profile_responses(
            request, response, 'example',
        ), [response])
        self.assertEqual(select_profile_responses(
            request, response, 'other',
        ), [])

    def test_filters_recommendations_and_unrelated_accounts(self) -> None:
        requests: list[dict] = [
            {'operationName': 'ChannelShell',
             'variables': {'login': 'example'}},
            {'operationName': 'SideNav', 'variables': {}},
            {'operationName': 'ChannelAvatar',
             'variables': {'channelLogin': 'other'}},
        ]
        responses: list[dict] = [
            {'data': {'userOrError': {'login': 'example', 'id': '123'}}},
            {'data': {'recommendations': ['ignored']}},
            {'data': {'user': {'id': '999'}}},
        ]
        selected: list[dict] = select_profile_responses(
            requests, responses, 'example',
        )
        self.assertEqual(selected, responses[:1])
        self.assertEqual(profile_operations(requests, 'example'), [
            'ChannelShell',
        ])

    def test_malformed_batch_is_rejected(self) -> None:
        self.assertEqual(select_profile_responses(
            [{'operationName': 'ChannelShell',
              'variables': {'login': 'example'}}], [], 'example',
        ), [])

    def test_non_string_operation_names_are_ignored(self) -> None:
        self.assertEqual(profile_operations([
            {'operationName': ['ChannelShell'],
             'variables': {'login': 'example'}},
        ], 'example'), [])


class TestBrowserCollection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        for name, value in (
            ('PROFILE_BASE_URL', 'https://localhost'),
            ('GRAPHQL_URL', 'https://localhost/gql'),
        ):
            self.enterContext(patch(
                f'scrape_exchange.twitch.twitch_browser.{name}', value,
            ))

    def settings(self) -> TwitchScraperSettings:
        return TwitchScraperSettings(
            _env_file=None, _cli_parse_args=[],
            creator_data_wait_seconds=0.01,
        )

    async def test_collects_profile_and_removes_listener(self) -> None:
        page: MagicMock = MagicMock()
        page.url = 'https://localhost/example/about'
        page.route = AsyncMock()
        page.unroute = AsyncMock()
        page.content = AsyncMock(return_value='')
        response: MagicMock = MagicMock()
        response.url = 'https://localhost/gql'
        response.status = 200
        response.headers = {}
        response.request.post_data = json.dumps({
            'operationName': 'ChannelShell',
            'variables': {'login': 'example'},
        })
        response.body = AsyncMock(return_value=json.dumps({
            'data': {'userOrError': {'id': '123', 'login': 'example'}},
        }).encode())

        async def navigate(*args, **kwargs):
            page.on.call_args.args[1](response)
            await asyncio.sleep(0)
            return MagicMock(status=200)

        page.goto = AsyncMock(side_effect=navigate)
        limiter: MagicMock = MagicMock(acquire=AsyncMock())
        creator = await fetch_profile(
            page, 'example', self.settings(), limiter, 'direct',
        )
        self.assertEqual(creator.user_id, '123')
        page.remove_listener.assert_called_once_with(
            'response', page.on.call_args.args[1],
        )
        page.unroute.assert_awaited_once()

    async def test_blocked_navigation_is_not_missing_profile(self) -> None:
        page: MagicMock = MagicMock()
        page.route = AsyncMock()
        page.unroute = AsyncMock()
        page.goto = AsyncMock(return_value=MagicMock(status=429))
        with self.assertRaises(ProfileRateLimitError):
            await fetch_profile(
                page, 'example', self.settings(),
                MagicMock(acquire=AsyncMock()), 'direct',
            )
        page.remove_listener.assert_called_once()
