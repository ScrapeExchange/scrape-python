'''Public profile extraction, independent of browser and file storage.'''

import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Self
from urllib.parse import SplitResult, urlsplit

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    TypeAdapter,
    field_validator,
    model_validator,
)

from scrape_exchange.onlyfans.endpoints import PROFILE_BASE_URL

Count = Annotated[int, Field(strict=True, ge=0)]
WebUrl = Annotated[str, Field(
    pattern=r'^https?://', json_schema_extra={'format': 'uri'},
)]
_HTTP_URL: TypeAdapter[HttpUrl] = TypeAdapter(HttpUrl)
_USERNAME: re.Pattern[str] = re.compile(r'[a-z0-9_.-]{1,64}')


def _annotation_schema_types(schema: dict[str, Any]) -> None:
    '''Expose nullable scalar types to the API's x-scrape validators.'''
    prop: dict[str, Any]
    for prop in schema.get('properties', {}).values():
        if not (prop.get('x-scrape-field') or prop.get('x-scrape-gauge')):
            continue
        alternatives: list[dict[str, Any]] = prop.get('anyOf', [])
        if len(alternatives) != 2 or alternatives[1] != {'type': 'null'}:
            continue
        scalar: dict[str, Any] = alternatives[0]
        if scalar.get('type') not in ('string', 'integer', 'number'):
            continue
        del prop['anyOf']
        prop.update(scalar)
        prop['type'] = [scalar['type'], 'null']


def normalize_creator(value: str) -> str:
    '''Accept a username, @handle or a public profile URL.'''
    name: str = value.strip()
    if '://' in name:
        parsed: SplitResult = urlsplit(name)
        host: str | None = urlsplit(PROFILE_BASE_URL).hostname
        if (
            parsed.scheme != 'https'
            or parsed.hostname not in (host, f'www.{host}')
            or parsed.username or parsed.password or parsed.port
        ):
            raise ValueError('Expected an OnlyFans public profile URL')
        name = parsed.path.removeprefix('/').removesuffix('/')
    name = name.removeprefix('@').lower()
    if not _USERNAME.fullmatch(name) or name in ('.', '..'):
        raise ValueError('Expected a username or @handle without spaces')
    return name


class OnlyFansCreator(BaseModel):
    '''Unknown values remain null; zero and false are observed values.'''

    model_config = ConfigDict(
        extra='forbid', allow_inf_nan=False,
        json_schema_serialization_defaults_required=True,
        json_schema_extra=_annotation_schema_types,
    )

    username: str = Field(
        pattern=r'^[a-z0-9_.-]{1,64}$',
        description='Profile username without @.',
        json_schema_extra={
            'x-scrape-field': 'platform_creator_id',
            'x-scrape-normalize': 'lowercase',
        },
    )
    handle: str = Field(pattern=r'^@[a-z0-9_.-]{1,64}$')
    user_id: str = Field(
        pattern=r'^[1-9][0-9]*$',
        json_schema_extra={'x-scrape-field': 'platform_content_id'},
    )
    url: WebUrl = Field(json_schema_extra={'x-scrape-field': 'source_url'})
    scraped_timestamp: AwareDatetime = Field(json_schema_extra={
        'pattern': r'^\d{4}(-\d{2}){2}T.+(Z|[+-]\d{2}:\d{2})$',
    })
    display_name: str | None = Field(
        default=None, description='Human-readable profile name.',
    )
    verified: Annotated[bool, Field(strict=True)] | None = None
    like_count: Count | None = Field(
        default=None, description='Likes received, not likes given.',
        json_schema_extra={'x-scrape-gauge': True},
    )
    photo_count: Count | None = Field(
        default=None, json_schema_extra={'x-scrape-gauge': True},
    )
    video_count: Count | None = Field(
        default=None, json_schema_extra={'x-scrape-gauge': True},
    )
    audio_count: Count | None = Field(
        default=None, json_schema_extra={'x-scrape-gauge': True},
    )
    banner_url: WebUrl | None = None
    avatar_url: WebUrl | None = Field(
        default=None,
        json_schema_extra={
            'x-scrape-field': 'platform_creator_thumbnail_url',
        },
    )
    fan_count: Count | None = Field(
        default=None, description='Fans, only when visibility is opted in.',
        json_schema_extra={'x-scrape-gauge': True},
    )
    fan_count_visible: Annotated[bool, Field(strict=True)] | None = None
    biography: str | None = None
    subscription_price: Annotated[
        float, Field(strict=True, ge=0),
    ] | None = Field(
        default=None, description='Regular monthly price, before promotions.',
    )
    subscription_currency: Literal['USD'] | None = None
    subscription_status: Literal['free', 'paid'] | None = None
    extractor_version: Literal['onlyfans-profile-v1'] = 'onlyfans-profile-v1'

    @field_validator('url', 'avatar_url', 'banner_url')
    @classmethod
    def validate_url(cls, value: str | None) -> str | None:
        if value is not None:
            _HTTP_URL.validate_python(value)
        return value

    @model_validator(mode='after')
    def validate_consistency(self) -> Self:
        if normalize_creator(self.username) != self.username:
            raise ValueError('Invalid profile username')
        if self.handle != f'@{self.username}':
            raise ValueError('Handle must match username')
        if self.fan_count is not None and self.fan_count_visible is not True:
            raise ValueError('Fan count requires public visibility')
        status: str | None = None
        if self.subscription_price is not None:
            status = 'free' if self.subscription_price == 0 else 'paid'
        if self.subscription_status != status:
            raise ValueError('Subscription status must match regular price')
        currency: str | None = 'USD' if status is not None else None
        if self.subscription_currency != currency:
            raise ValueError('Subscription currency requires a known price')
        return self

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode='json')


def extract_profile(payload: object, requested: str) -> OnlyFansCreator:
    '''Map the public user response, never recommendations or paid posts.'''
    username: str = normalize_creator(requested)
    if not isinstance(payload, dict) or payload.get('error'):
        raise ValueError('No public profile in response')
    observed: object = payload.get('username')
    if not isinstance(observed, str) or observed.lower() != username:
        raise ValueError('Public profile identity does not match request')
    user_id: object = payload.get('id')
    if isinstance(user_id, bool) or not isinstance(user_id, (str, int)):
        raise ValueError('Public profile has no account ID')  # noqa: TRY004
    visibility: bool | None = (
        payload['showSubscribersCount']
        if type(payload.get('showSubscribersCount')) is bool else None
    )
    media_visible: bool = payload.get('showMediaCount') is not False
    price: object = payload.get('subscribePrice')
    # Validate before deriving status, including rejecting bool and NaN.
    if price is not None:
        if isinstance(price, bool) or not isinstance(price, (int, float)):
            raise ValueError('Invalid regular subscription price')
        if not price >= 0:
            raise ValueError('Invalid regular subscription price')
    status: str | None = None
    if price is not None:
        status = 'free' if price == 0 else 'paid'
    data: dict[str, Any] = {
        'username': username, 'handle': f'@{username}',
        'user_id': str(user_id), 'url': f'{PROFILE_BASE_URL}/{username}',
        'scraped_timestamp': datetime.now(UTC),
        'display_name': payload.get('name'),
        'verified': payload.get('isVerified'),
        'like_count': payload.get('favoritedCount'),
        'photo_count': payload.get('photosCount') if media_visible else None,
        'video_count': payload.get('videosCount') if media_visible else None,
        'audio_count': payload.get('audiosCount') if media_visible else None,
        'banner_url': payload.get('header') or None,
        'avatar_url': payload.get('avatar') or None,
        'fan_count_visible': visibility,
        'fan_count': (
            payload.get('subscribersCount') if visibility is True else None
        ),
        'biography': payload.get('about'),
        'subscription_price': price,
        'subscription_currency': 'USD' if price is not None else None,
        'subscription_status': status,
    }
    return OnlyFansCreator.model_validate(data)
