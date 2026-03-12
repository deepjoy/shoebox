# Pre-signed URLs

Pre-signed URLs let you share temporary download or upload links without exposing your credentials. Generate a URL, share it, and anyone with the link can access the file until it expires.

## Generating Download URLs

```bash
shoebox presign get photos vacation/sunset.jpg \
  --bucket-path ~/Photos \
  --expires 7d
```

Output:
```
http://localhost:9000/photos/vacation/sunset.jpg?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Expires=604800&X-Amz-Signature=...
```

Anyone with this URL can download the file for 7 days. No credentials needed.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--expires <DURATION>` | `1h` | How long the URL is valid |
| `--endpoint <URL>` | `http://localhost:9000` | The server's public URL |
| `--bucket-path <PATH>` | — | Path to the bucket directory (required, reads credentials from config) |

### Duration Format

| Example | Duration |
|---------|----------|
| `30m` | 30 minutes |
| `1h` | 1 hour |
| `7d` | 7 days |
| `24h` | 24 hours |

## Generating Upload URLs

```bash
shoebox presign put photos uploads/new-photo.jpg \
  --bucket-path ~/Photos \
  --expires 1h \
  --content-type image/jpeg
```

The `--content-type` flag restricts what content type the uploader must use. Omit it to allow any content type.

## Using in Web Apps

Pre-signed URLs are commonly used for browser-based uploads and downloads without proxying through your backend.

### Download Link

```html
<a href="http://localhost:9000/photos/vacation/sunset.jpg?X-Amz-Algorithm=...">
  Download Photo
</a>
```

### Browser Upload (fetch)

```javascript
// 1. Your backend generates the pre-signed URL
const uploadUrl = await fetch("/api/get-upload-url").then(r => r.json());

// 2. Browser uploads directly to Shoebox
await fetch(uploadUrl, {
  method: "PUT",
  headers: { "Content-Type": "image/jpeg" },
  body: fileBlob,
});
```

### Browser Upload (HTML Form)

```html
<input type="file" id="file-input" />
<script>
  document.getElementById("file-input").addEventListener("change", async (e) => {
    const file = e.target.files[0];
    const uploadUrl = await fetch(`/api/upload-url?name=${file.name}`).then(r => r.json());
    await fetch(uploadUrl, {
      method: "PUT",
      headers: { "Content-Type": file.type },
      body: file,
    });
  });
</script>
```

## Custom Endpoint

If Shoebox is behind a reverse proxy or accessible at a different URL, set the endpoint:

```bash
shoebox presign get photos sunset.jpg \
  --bucket-path ~/Photos \
  --endpoint https://s3.example.com
```

The generated URL will use `https://s3.example.com` as the base.

## How It Works

Pre-signed URLs use AWS Signature V4 query string authentication. The URL embeds:
- The access key ID (not the secret)
- An expiration timestamp
- A cryptographic signature computed from the secret key

Shoebox verifies the signature when the URL is used. If the signature is valid and the URL hasn't expired, the request is allowed — even without `Authorization` headers.

## Security Considerations

- **URLs are bearer tokens**: Anyone with the URL can access the file. Share them carefully.
- **Set short expirations**: Use the shortest expiration that makes sense for your use case.
- **HTTPS in production**: Use `--endpoint https://...` for production URLs. HTTP URLs expose the signed parameters in transit.
- **Upload URLs respect content-type**: If you specify `--content-type`, the uploader must use that exact content type or the request fails.

## See Also

- [CORS](cors.md) — Required for browser-based uploads to work
- [Credentials](credentials.md) — Managing the access keys used to sign URLs
- [CLI Reference](cli-reference.md) — Full `presign` command options
