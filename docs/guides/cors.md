# CORS Configuration

Browsers block cross-origin requests by default. If you're building a web app that talks to Shoebox directly from the browser, you need to configure CORS (Cross-Origin Resource Sharing) rules.

## When You Need CORS

You need CORS if your web app:
- Runs on a different origin than Shoebox (e.g., `http://localhost:3000` talking to `http://localhost:9000`)
- Uploads or downloads files directly from the browser using the S3 API
- Uses pre-signed URLs for browser-based uploads

You don't need CORS if:
- Your backend proxies all S3 requests (the browser never talks to Shoebox directly)
- You're only using CLI tools, SDKs, or server-side code

## Quick Setup

Configure CORS using the standard `aws s3api put-bucket-cors` command:

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api put-bucket-cors --bucket photos --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
      "AllowedHeaders": ["*"],
      "ExposeHeaders": ["ETag", "x-amz-request-id"],
      "MaxAgeSeconds": 3600
    }]
  }'
```

This allows all origins. For production, restrict `AllowedOrigins` to your app's domain.

## Common Patterns

### Development (Allow Everything)

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api put-bucket-cors --bucket photos --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
      "AllowedHeaders": ["*"],
      "ExposeHeaders": ["ETag", "x-amz-request-id"],
      "MaxAgeSeconds": 3600
    }]
  }'
```

### Single App

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api put-bucket-cors --bucket photos --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["https://myapp.example.com"],
      "AllowedMethods": ["GET", "PUT", "HEAD"],
      "AllowedHeaders": ["Content-Type", "Authorization", "x-amz-content-sha256", "x-amz-date"],
      "ExposeHeaders": ["ETag"],
      "MaxAgeSeconds": 86400
    }]
  }'
```

### Read-Only Public Access

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api put-bucket-cors --bucket photos --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "HEAD"],
      "AllowedHeaders": ["*"],
      "ExposeHeaders": ["Content-Length", "Content-Type", "ETag"],
      "MaxAgeSeconds": 86400
    }]
  }'
```

## Managing CORS Rules

### Set CORS Rules

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api put-bucket-cors --bucket photos --cors-configuration '{
    "CORSRules": [{
      "AllowedOrigins": ["http://localhost:3000"],
      "AllowedMethods": ["GET", "PUT", "HEAD"],
      "AllowedHeaders": ["*"],
      "ExposeHeaders": ["ETag"],
      "MaxAgeSeconds": 3600
    }]
  }'
```

### Read Current Rules

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api get-bucket-cors --bucket photos
```

### Delete CORS Rules

```bash
aws --profile shoebox --endpoint-url http://localhost:9000 \
  s3api delete-bucket-cors --bucket photos
```

## How Preflight Works

When a browser makes a cross-origin request, it first sends an `OPTIONS` request (the "preflight"). Shoebox automatically handles preflight requests using the configured CORS rules — no extra setup needed.

The flow:
1. Browser sends `OPTIONS` with `Origin` and `Access-Control-Request-Method` headers.
2. Shoebox checks the request against CORS rules for that bucket.
3. If a rule matches, Shoebox responds with the appropriate `Access-Control-Allow-*` headers.
4. Browser makes the actual request.

## Debugging CORS Issues

If your browser shows CORS errors:

1. **Check that CORS rules exist** for the bucket:
   ```bash
   aws --profile shoebox --endpoint-url http://localhost:9000 \
     s3api get-bucket-cors --bucket photos
   ```

2. **Check the origin matches**: The `AllowedOrigins` must include your app's exact origin (protocol + host + port), or use `*`.

3. **Check the method is allowed**: The HTTP method must be in `AllowedMethods`.

4. **Check the headers are allowed**: Any custom headers your app sends must be listed in `AllowedHeaders`, or use `*`.

5. **Validate with the Shoebox CLI**:
   ```bash
   shoebox validate ~/Photos
   ```
   This reports whether CORS rules are configured.

## See Also

- [Pre-signed URLs](presigned-urls.md) — Browser-friendly temporary access links
- [S3 Compatibility](s3-compatibility.md) — Using Shoebox with JavaScript SDKs
- [Troubleshooting](troubleshooting.md) — Common CORS debugging tips
