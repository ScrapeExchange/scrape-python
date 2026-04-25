# JSON Schemas on Scrape Exchange

This guide explains how to create and register a JSON Schema on scrape.exchange so you can submit scraped social media data against it.

## What is a Schema?

A schema defines the structure of data you want to collect from a specific platform. Every piece of data submitted to scrape.exchange must conform to a registered schema. Schemas use the [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12/json-schema-core) specification.

Each schema is scoped to a specific combination of:

| Field | Description | Constraints |
|---|---|---|
| **username** | Your account username (set automatically) | 3-50 chars, alphanumeric + `_.-` |
| **platform** | The social media platform | One of: `youtube`, `tiktok`, `twitch`, `kick`, `facebook`, `instagram`, `x`, `rumble`, `telegram`, `threads`, `reddit`, `discord`, `steam` |
| **entity** | The type of content being scraped | 3-16 chars, alphanumeric + `_.-` (e.g. `video`, `channel`, `post`, `comment`) |
| **version** | Semantic version of the schema | Format `X.Y.Z` (e.g. `1.0.0`, `2.3.11`) |

The combination of `(username, platform, entity, version)` must be unique. You can have multiple versions of the same schema.

## Schema Naming

When you register a schema, the API automatically assigns a name in this format:

```
{username}-{platform}-{entity}-{version}
```

For example, user `cano` registering a YouTube video schema at version `1.2.13` gets the name:

```
cano-youtube-video-1.2.13
```

The `$id` field in your schema is also set automatically to:

```
https://scrape.exchange/schemas/cano-youtube-video-1.2.13
```

You do not need to set `$id` yourself -- it will be overwritten on submission.

## Rules

1. **All property names must be `snake_case`.**
2. **All `$ref` references must be local** -- they must point to types defined within the same schema (e.g. `#/$defs/thumbnail`). Cross-file and external references are not allowed.
3. **URLs in the schema must point to allowed domains only:** `scrape.exchange`, `www.scrape.exchange`, or `json-schema.org`. The `$schema` field is exempt from this check.
4. **Versioning must follow [Semantic Versioning](https://semver.org).**
5. **The schema must be valid JSON Schema Draft 2020-12.** The API validates this on submission.

## Writing a Schema

### Basic Structure

Every schema needs at minimum:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "YouTube Video Metadata",
  "description": "Schema for YouTube video metadata",
  "type": "object",
  "properties": {
    ...
  },
  "required": ["video_id", "title"]
}
```

### Property Types

Use standard JSON Schema types: `string`, `integer`, `number`, `boolean`, `array`, `object`, `null`.

For nullable fields, use a type array:

```json
"like_count": {
  "type": ["integer", "null"],
  "minimum": 0,
  "description": "The number of likes on the video"
}
```

### Formats

Use `format` for common string formats:

```json
"channel_url": { "type": "string", "format": "uri" },
"published_at": { "type": "string", "format": "date-time" },
"upload_date": { "type": "string", "format": "date" },
"email": { "type": "string", "format": "email" }
```

### Patterns

Constrain string values with regex patterns:

```json
"video_id": {
  "type": "string",
  "pattern": "^[a-zA-Z0-9_-]{11}$"
}
```

### Enums

Restrict a field to a fixed set of values:

```json
"privacy_status": {
  "type": "string",
  "enum": ["public", "unlisted", "private"]
}
```

### Arrays

Define arrays with `items` describing the element type:

```json
"tags": {
  "type": "array",
  "items": { "type": "string" }
}
```

For arrays of objects:

```json
"chapters": {
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "title": { "type": "string" },
      "start_time": { "type": "integer", "minimum": 0 }
    },
    "required": ["title", "start_time"]
  }
}
```

### Reusable Types with `$defs`

Define reusable types in the `$defs` section and reference them with `$ref`:

```json
{
  "properties": {
    "thumbnails": {
      "type": "array",
      "items": { "$ref": "#/$defs/thumbnail" }
    }
  },
  "$defs": {
    "thumbnail": {
      "type": "object",
      "properties": {
        "url": { "type": "string", "format": "uri" },
        "width": { "type": "integer", "minimum": 1 },
        "height": { "type": "integer", "minimum": 1 }
      },
      "required": ["url"]
    }
  }
}
```

All `$ref` targets must be defined within the same schema. The API validates that every `$ref` resolves to an existing `$defs`, `definitions`, or `$anchor` entry.

## Scrape-Specific Extensions

### `x-scrape-field` -- Field Mapping

Use the `x-scrape-field` annotation to map a property in your schema to a well-known field in the scrape.exchange data model. This allows the platform to index, filter, and display your data correctly.

Available targets:

| Target | Purpose | Required Shape |
|---|---|---|
| `platform_content_id` | Unique ID of the content on the platform (e.g. video ID) | string |
| `platform_creator_id` | ID of the content creator (e.g. channel ID) | string |
| `platform_topic_id` | ID of the topic/category | string |
| `platform_content_thumbnail_url` | Thumbnail for the content | See below |
| `platform_creator_thumbnail_url` | Thumbnail for the creator | See below |
| `platform_topic_thumbnail_url` | Thumbnail for the topic | See below |

Each target can only be used once per schema. Duplicates will be rejected.

Example:

```json
"video_id": {
  "type": "string",
  "x-scrape-field": "platform_content_id"
},
"channel_id": {
  "type": "string",
  "x-scrape-field": "platform_creator_id"
}
```

#### Thumbnail Field Shapes

Thumbnail targets (`platform_content_thumbnail_url`, `platform_creator_thumbnail_url`, `platform_topic_thumbnail_url`) must use one of two accepted shapes:

**Shape A -- Simple URI string:**

```json
"thumbnail_url": {
  "type": "string",
  "format": "uri",
  "x-scrape-field": "platform_content_thumbnail_url"
}
```

**Shape B -- Array of thumbnail objects:**

```json
"thumbnails": {
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "url": { "type": "string" },
      "height": { "type": "integer" },
      "width": { "type": "integer" }
    }
  },
  "x-scrape-field": "platform_content_thumbnail_url"
}
```

### Gauge Fields (Metrics Tracking)

Any property whose name ends in `_count` and has a numeric type (`integer` or `number`) is automatically treated as a gauge metric. These fields are tracked as time-series data for rankings and statistics.

Examples of fields that qualify:

```json
"view_count": { "type": "integer", "minimum": 0 },
"subscriber_count": { "type": "integer" },
"like_count": { "type": ["integer", "null"], "minimum": 0 }
```

You can query which fields are gauges for a schema via:

```
GET /api/v1/schema/counters/{username}/{platform}/{entity}/{version}
```

## Registering a Schema

### Via the API

Send a `POST` to `/api/v1/schema` with your authentication token:

```bash
curl -X POST https://scrape.exchange/api/v1/schema \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "youtube",
    "entity": "video",
    "version": "1.0.0",
    "json_schema": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "YouTube Video",
      "type": "object",
      "properties": {
        "video_id": {
          "type": "string",
          "x-scrape-field": "platform_content_id"
        },
        "title": { "type": "string" },
        "view_count": { "type": "integer", "minimum": 0 }
      },
      "required": ["video_id", "title"]
    }
  }'
```

The API returns the generated schema name (e.g. `youruser-youtube-video-1.0.0`) on success.

### Auto-Generate from Data

If you already have scraped data, you can generate a schema from it:

```bash
curl -X POST https://scrape.exchange/api/v1/schema/generate \
  -H "Authorization: Bearer <your-token>" \
  -F "file=@my_data.json"
```

This accepts `.json`, `.jsonl`, and `.csv` files (up to 50 MB). It will infer a Draft 2020-12 schema from the data. If the data already matches an existing schema, the API returns a `409` response with the matching schema name(s).

Note: the generated schema won't include `x-scrape-field` annotations -- you'll need to add those yourself before registering.

## Retrieving Schemas

| Endpoint | Description |
|---|---|
| `GET /api/v1/schema` | List all schemas (paginated) |
| `GET /api/v1/schema/{schema_name}` | Get schema by name |
| `GET /api/v1/schema/newest/{username}/{platform}/{entity}` | Get the latest version of a schema |
| `GET /api/v1/schema/param/{username}/{platform}/{entity}/{version}` | Get a specific version |
| `GET /api/v1/schema/param/{username}/{platform}/{entity}` | List all versions for an entity |
| `GET /api/v1/schema/param/{username}/{platform}` | List all schemas for a platform |
| `GET /api/v1/schema/param/{username}` | List all schemas for a user |

## Deleting a Schema

```
DELETE /api/v1/schema/param/{platform}/{entity}/{version}
```

Requires authentication. You can only delete your own schemas.

## Complete Example

Below is a complete schema for YouTube channel metadata:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "YouTube Channel",
  "description": "Schema for YouTube channel metadata",
  "type": "object",
  "required": ["channel_id", "title"],
  "properties": {
    "channel_id": {
      "type": "string",
      "description": "The YouTube channel ID",
      "x-scrape-field": "platform_content_id"
    },
    "channel": {
      "type": "string",
      "description": "The channel handle (e.g. @channelname)"m
      "x-scrape-field": "platform_creator_id"
    },
    "title": {
      "type": "string",
      "description": "The channel name"
    },
    "description": {
      "type": ["string", "null"],
      "description": "The channel description"
    },
    "channel_url": {
      "type": "string",
      "format": "uri",
      "description": "URL to the channel page",
      "x-scrape-field": "platform_creator_thumbnail_url"
    },
    "subscriber_count": {
      "type": "integer",
      "minimum": 0,
      "description": "Number of subscribers"
    },
    "video_count": {
      "type": "integer",
      "minimum": 0,
      "description": "Number of videos"
    },
    "view_count": {
      "type": "integer",
      "minimum": 0,
      "description": "Total channel views"
    },
    "verified": {
      "type": "boolean",
      "description": "Whether the channel is verified"
    },
    "country_code": {
      "type": "string",
      "description": "ISO 3166-2 country code"
    },
    "thumbnails": {
      "type": "array",
      "items": { "$ref": "#/$defs/thumbnail" },
      "x-scrape-field": "platform_content_thumbnail_url"
    },
    "keywords": {
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "$defs": {
    "thumbnail": {
      "type": "object",
      "properties": {
        "url": { "type": "string", "format": "uri" },
        "width": { "type": "integer", "minimum": 1 },
        "height": { "type": "integer", "minimum": 1 }
      },
      "required": ["url", "width", "height"]
    }
  }
}
```

Register it with:

```bash
curl -X POST https://scrape.exchange/api/v1/schema \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "platform": "youtube",
    "entity": "channel",
    "version": "1.0.0",
    "json_schema": <the schema above>
  }'
```
