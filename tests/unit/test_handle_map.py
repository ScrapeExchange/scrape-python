import asyncio
import unittest

from scrape_exchange.handle_map import NullHandleMap


class TestNullHandleMap(unittest.TestCase):
    def test_get_returns_none_for_unknown(self) -> None:
        m = NullHandleMap()
        self.assertIsNone(asyncio.run(m.get('LinusTechTips')))

    def test_get_many_returns_empty_dict_for_empty_input(
        self,
    ) -> None:
        m = NullHandleMap()
        self.assertEqual(asyncio.run(m.get_many([])), {})

    def test_put_then_get_round_trip(self) -> None:
        m = NullHandleMap()
        asyncio.run(m.put('LinusTechTips', 'UCXuqSBlHAE6Xw-yeJA0Tunw'))
        self.assertEqual(
            asyncio.run(m.get('LinusTechTips')),
            'UCXuqSBlHAE6Xw-yeJA0Tunw',
        )

    def test_get_all_returns_full_dict(self) -> None:
        m = NullHandleMap()
        asyncio.run(m.put('Foo', 'UC1'))
        asyncio.run(m.put('Bar', 'UC2'))
        self.assertEqual(
            asyncio.run(m.get_all()),
            {'Foo': 'UC1', 'Bar': 'UC2'},
        )

    def test_get_many_returns_partial_hits(self) -> None:
        m = NullHandleMap()
        asyncio.run(m.put('Foo', 'UC1'))
        result: dict[str, str | None] = asyncio.run(
            m.get_many(['Foo', 'Bar'])
        )
        self.assertEqual(result['Foo'], 'UC1')
        # Bar absent from store -> None per contract
        self.assertIsNone(result.get('Bar'))

    def test_put_many_round_trip(self) -> None:
        m = NullHandleMap()
        asyncio.run(m.put_many({'Foo': 'UC1', 'Bar': 'UC2'}))
        self.assertEqual(asyncio.run(m.get('Foo')), 'UC1')
        self.assertEqual(asyncio.run(m.get('Bar')), 'UC2')


if __name__ == '__main__':
    unittest.main()
