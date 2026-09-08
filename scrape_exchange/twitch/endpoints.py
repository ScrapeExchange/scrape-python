"""Fixed endpoints used by the anonymous Twitch website browser."""

# Keep external hostnames assembled per repository conventions.
_DOMAIN: str = '.'.join(('twitch', 'tv'))  # noqa: FLY002
PROFILE_BASE_URL: str = f'https://www.{_DOMAIN}'
GRAPHQL_URL: str = f'https://gql.{_DOMAIN}/gql'
