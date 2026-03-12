# Webhooks & Notifications

Shoebox sends HTTP notifications when objects are created, deleted, or copied. Configure a webhook endpoint and Shoebox will POST event payloads whenever matching operations occur.

## Configuring Webhooks

Webhooks are configured per-bucket via the S3 notification API. Use `aws s3api` or `curl`:

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api put-bucket-notification-configuration --bucket photos \
  --notification-configuration '{
    "CloudFunctionConfigurations": [{
      "Id": "upload-notify",
      "CloudFunction": "https://example.com/webhook",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {"Name": "prefix", "Value": "uploads/"},
            {"Name": "suffix", "Value": ".jpg"}
          ]
        }
      }
    }]
  }'
```

Or with curl (Shoebox's native JSON format):

```bash
curl -X PUT "http://localhost:9000/photos?notification" \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "id": "upload-notify",
      "url": "https://example.com/webhook",
      "events": ["s3:ObjectCreated:*"],
      "filter": {
        "prefix": "uploads/",
        "suffix": ".jpg"
      }
    }
  ]'
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique identifier for this webhook |
| `url` | Yes | HTTP or HTTPS endpoint to receive events |
| `events` | Yes | Array of event patterns to match |
| `filter` | No | Key prefix/suffix filter |
| `filter.prefix` | No | Only trigger for keys starting with this prefix |
| `filter.suffix` | No | Only trigger for keys ending with this suffix |

## Supported Events

| Event | Triggered By |
|-------|-------------|
| `s3:ObjectCreated:Put` | Object uploaded via PUT |
| `s3:ObjectCreated:Copy` | Object copied (server-side) |
| `s3:ObjectCreated:CompleteMultipartUpload` | Multipart upload completed |
| `s3:ObjectCreated:*` | Any object creation event |
| `s3:ObjectRemoved:Delete` | Object deleted |
| `s3:ObjectRemoved:*` | Any object deletion event |

### Wildcard Matching

Use `*` at the end of an event pattern to match all sub-events:
- `s3:ObjectCreated:*` matches Put, Copy, and CompleteMultipartUpload
- `s3:ObjectRemoved:*` matches Delete

## Event Payload

Shoebox POSTs a JSON payload to your endpoint:

```json
{
  "event_name": "s3:ObjectCreated:Put",
  "event_time": "2026-03-11T14:30:00Z",
  "bucket": "photos",
  "object_id": "550e8400-e29b-41d4-a716-446655440000",
  "object_key": "uploads/photo.jpg",
  "size": 4194304,
  "etag": "\"d41d8cd98f00b204e9800998ecf8427e\""
}
```

## Filtering

Filters narrow which events trigger the webhook. Both prefix and suffix are optional — omit the filter entirely to receive all matching events.

```json
{
  "id": "images-only",
  "url": "https://example.com/webhook",
  "events": ["s3:ObjectCreated:*"],
  "filter": {
    "prefix": "uploads/",
    "suffix": ".png"
  }
}
```

This webhook only fires for PNG files created under the `uploads/` prefix.

## Multiple Webhooks

You can configure multiple webhooks per bucket. Each has its own event filter:

```json
[
  {
    "id": "all-uploads",
    "url": "https://api.example.com/on-upload",
    "events": ["s3:ObjectCreated:*"]
  },
  {
    "id": "deletions",
    "url": "https://api.example.com/on-delete",
    "events": ["s3:ObjectRemoved:*"]
  }
]
```

## Reading Webhook Config

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api get-bucket-notification-configuration --bucket photos
```

## Delivery

- Webhooks are delivered asynchronously — the S3 operation completes before the webhook fires.
- Failed deliveries are retried with exponential backoff.
- Delivery attempts are logged in the per-bucket SQLite database for debugging.
- Both HTTP and HTTPS endpoints are supported. HTTPS uses system-trusted certificates.

## HTTPS Endpoints

For production use, prefer HTTPS endpoints. Shoebox uses `hyper-rustls` for TLS, which trusts the standard webpki root certificates.

The `validate` command warns about HTTP (non-HTTPS) webhook URLs:

```bash
shoebox validate ~/Photos
```

```
  [WARN] Webhook "upload-notify" uses HTTP (not HTTPS): http://example.com/webhook
```

## Example: Simple Webhook Receiver

A minimal webhook receiver in Python:

```python
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers["Content-Length"])
        body = json.loads(self.rfile.read(length))
        print(f"{body['event_name']}: {body['bucket']}/{body['object_key']}")
        self.send_response(200)
        self.end_headers()

HTTPServer(("0.0.0.0", 8080), WebhookHandler).serve_forever()
```

## See Also

- [CORS](cors.md) — Browser access configuration (separate from webhooks)
- [CLI Reference](cli-reference.md) — The `validate` command checks webhook configuration
- [S3 Compatibility](s3-compatibility.md) — Full list of supported operations
