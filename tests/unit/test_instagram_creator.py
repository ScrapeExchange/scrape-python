'''
Unit tests for Instagram creator parsing and modelling.
'''

import unittest

from scrape_exchange.instagram.instagram_creator import (
    InstagramCreator,
    InstagramProfileUnavailableError,
    UnknownFollowersError,
    extract_profile_data,
    parse_count,
)


class TestInstagramCountParsing(unittest.TestCase):

    def test_parse_plain_and_compact_counts(self) -> None:
        self.assertEqual(parse_count('123'), 123)
        self.assertEqual(parse_count('1,234'), 1234)
        self.assertEqual(parse_count('12.5K'), 12_500)
        self.assertEqual(parse_count('1.2M'), 1_200_000)
        self.assertEqual(parse_count('2B'), 2_000_000_000)
        self.assertIsNone(parse_count('unknown'))


class TestInstagramProfileExtraction(unittest.TestCase):

    def test_extracts_structured_profile(self) -> None:
        html: str = '''
        <html><head><script>
        {"entry_data": {"ProfilePage": [{"graphql": {"user": {
          "username": "NatGeo",
          "id": "123",
          "full_name": "National Geographic",
          "biography": "Photos",
          "profile_pic_url_hd": "https://cdn.example/avatar.jpg",
          "edge_followed_by": {"count": 282000000},
          "edge_follow": {"count": 154},
          "edge_owner_to_timeline_media": {"count": 30500},
          "is_verified": true,
          "is_private": false
        }}}]}}
        </script></head><body></body></html>
        '''
        profile = extract_profile_data(html, 'natgeo')
        self.assertEqual(profile.username, 'natgeo')
        self.assertEqual(profile.user_id, '123')
        self.assertEqual(profile.follower_count, 282_000_000)
        self.assertEqual(profile.following_count, 154)
        self.assertEqual(profile.post_count, 30_500)
        self.assertTrue(profile.verified)
        self.assertEqual(profile.detected_markers, ['structured_profile'])

    def test_extracts_meta_profile(self) -> None:
        html: str = '''
        <html><head>
        <meta property="og:description"
              content="1.2M Followers, 50 Following, 12 Posts">
        <meta property="og:title" content="Example Profile">
        <meta property="og:image" content="https://cdn.example/p.jpg">
        </head><body></body></html>
        '''
        profile = extract_profile_data(html, 'example')
        self.assertEqual(profile.username, 'example')
        self.assertEqual(profile.follower_count, 1_200_000)
        self.assertEqual(profile.following_count, 50)
        self.assertEqual(profile.post_count, 12)
        self.assertEqual(profile.detected_markers, ['meta_profile'])

    def test_extracts_lowercase_meta_profile(self) -> None:
        html: str = '''
        <html><head>
        <meta property="og:description"
              content="670M followers, 647 following, 4,101 posts">
        </head><body></body></html>
        '''
        profile = extract_profile_data(html, 'cristiano')
        self.assertEqual(profile.follower_count, 670_000_000)
        self.assertEqual(profile.following_count, 647)
        self.assertEqual(profile.post_count, 4_101)

    def test_extracts_french_meta_profile(self) -> None:
        html: str = '''
        <html><head>
        <meta property="og:description"
              content="670 M abonnés, 647 abonnements,
              4,101 publications">
        </head><body></body></html>
        '''
        profile = extract_profile_data(html, 'cristiano')
        self.assertEqual(profile.follower_count, 670_000_000)
        self.assertEqual(profile.following_count, 647)
        self.assertEqual(profile.post_count, 4_101)

    def test_login_link_does_not_mask_public_profile_metadata(
        self,
    ) -> None:
        html: str = '''
        <html><head>
        <meta property="og:description"
              content="1.2M Followers, 50 Following, 12 Posts">
        <meta property="og:title" content="Example Profile">
        </head><body>
        <a href="/accounts/login/">Log in</a>
        </body></html>
        '''
        profile = extract_profile_data(html, 'example')
        self.assertEqual(profile.username, 'example')
        self.assertEqual(profile.follower_count, 1_200_000)

    def test_incomplete_structured_profile_falls_back_to_meta(
        self,
    ) -> None:
        html: str = '''
        <html><head>
        <meta property="og:description"
              content="1.2M Followers, 50 Following, 12 Posts">
        <script type="application/ld+json">
        {"@type": "Person", "alternateName": "example",
         "name": "Example Profile"}
        </script>
        </head><body></body></html>
        '''
        profile = extract_profile_data(html, 'example')
        self.assertEqual(profile.username, 'example')
        self.assertEqual(profile.follower_count, 1_200_000)
        self.assertEqual(
            profile.detected_markers,
            ['structured_profile', 'meta_profile'],
        )

    def test_extracts_saved_page_profile_fields(self) -> None:
        html: str = '''
        <html><head>
        <meta property="og:description"
              content="670M Followers, 647 Following, 4,101 Posts">
        <script type="application/json">
        {"require":[["RelayPrefetchedStreamCache","next",[],[
          "query", {"__bbox":{"result":{"data":{
            "xig_user_by_username":{
              "pk":"173560420",
              "username":"cristiano",
              "profile_pic_url":"https://files.scrape.exchange/p.jpg",
              "is_private":false,
              "is_unpublished":false,
              "biography":"",
              "full_name":"Cristiano Ronaldo",
              "is_verified":true,
              "text_post_app_badge_label":null,
              "show_text_post_app_badge":false,
              "account_badges":[{"badge":"verified"}],
              "bio_links":[{
                "image_url":"",
                "is_pinned":false,
                "link_type":"external",
                "lynx_url":"https://scrape.exchange/?u=one",
                "media_accent_color_hex":"",
                "media_type":"none",
                "title":"Main Site",
                "url":"https://www.scrape.exchange/profile",
                "creation_source":"NONE"
              }],
              "linked_fb_info":{"page_id":"42"},
              "is_memorialized":false,
              "pronouns":["he"],
              "follower_count":670449309,
              "following_count":632,
              "all_media_count":null,
              "id":"17841401692602711",
              "is_coppa_enforced":false,
              "has_any_clips":true
            }}}}}]]]}
        </script></head><body></body></html>
        '''
        profile = extract_profile_data(html, 'cristiano')
        self.assertEqual(profile.pk, '173560420')
        self.assertEqual(profile.user_id, '17841401692602711')
        self.assertEqual(profile.biography, '')
        self.assertEqual(profile.follower_count, 670_449_309)
        self.assertEqual(profile.following_count, 632)
        self.assertEqual(profile.post_count, 4_101)
        self.assertFalse(profile.unpublished)
        self.assertFalse(profile.memorialized)
        self.assertFalse(profile.coppa_enforced)
        self.assertTrue(profile.has_any_clips)
        self.assertEqual(profile.pronouns, ['he'])
        self.assertEqual(profile.account_badges, [{'badge': 'verified'}])
        self.assertEqual(profile.linked_fb_info, {'page_id': '42'})
        self.assertEqual(len(profile.bio_links), 1)
        link = profile.bio_links[0]
        self.assertEqual(link.title, 'Main Site')
        self.assertEqual(link.url, 'https://www.scrape.exchange/profile')
        self.assertEqual(link.link_type, 'external')
        self.assertEqual(link.image_url, '')
        self.assertEqual(link.media_type, 'none')
        self.assertEqual(link.creation_source, 'NONE')
        self.assertFalse(link.is_pinned)

        creator = InstagramCreator.from_profile_data(profile)
        record: dict = creator.to_dict()
        self.assertEqual(record['pk'], '173560420')
        self.assertEqual(record['bio_links'][0]['title'], 'Main Site')
        self.assertEqual(record['post_count'], 4_101)
        self.assertEqual(record['pronouns'], ['he'])
        self.assertEqual(record['linked_fb_info'], {'page_id': '42'})

    def test_unavailable_marker_raises(self) -> None:
        with self.assertRaises(InstagramProfileUnavailableError):
            extract_profile_data(
                "Sorry, this page isn't available.",
                'missing',
            )

    def test_creator_requires_follower_count(self) -> None:
        profile = extract_profile_data(
            '<meta property="og:description" content="No counters">',
            'example',
        )
        with self.assertRaises(UnknownFollowersError):
            InstagramCreator.from_profile_data(profile)

    def test_to_dict_omits_missing_non_follower_counts(self) -> None:
        profile = extract_profile_data(
            '<meta property="og:description" '
            'content="42 Followers - Example">',
            'example',
        )
        creator = InstagramCreator.from_profile_data(profile)
        record: dict = creator.to_dict()
        self.assertEqual(record['follower_count'], 42)
        self.assertNotIn('following_count', record)
        self.assertNotIn('post_count', record)


if __name__ == '__main__':
    unittest.main()
