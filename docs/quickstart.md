# Quickstart

## 1. Start the server

```bash
# Serve your Documents folder
shoebox ~/Documents

# Output:
# Serving 1 bucket on http://localhost:9000
#   documents → /home/user/Documents
#     Access Key: AKIA...
#     Secret Key: ...
```

Or with Docker:

```bash
docker run -v ~/Documents:/data -p 9000:9000 deeppjoymajumdar/shoebox /data
```

## 2. Configure AWS CLI

```bash
aws configure --profile shoebox
# AWS Access Key ID: (paste from output)
# AWS Secret Access Key: (paste from output)
# Default region: us-east-1
# Default output format: json
```

## 3. Use it

```bash
# List files
aws --profile shoebox --endpoint-url http://localhost:9000 s3 ls s3://documents/

# Upload a file
aws --profile shoebox --endpoint-url http://localhost:9000 s3 cp file.txt s3://documents/

# Download a file
aws --profile shoebox --endpoint-url http://localhost:9000 s3 cp s3://documents/file.txt ./
```

## Multiple Buckets

```bash
# Serve multiple directories as separate buckets
shoebox ~/Photos ~/Documents ~/Backups
```

## Custom Host/Port

```bash
shoebox --host 127.0.0.1 --port 8080 ~/Documents
```

## Validate Configuration

```bash
shoebox validate ~/Documents
```

## Next Steps

- [CLI Reference](guides/cli-reference.md) — All commands, flags, and options
- [Configuration](guides/configuration.md) — Global config, data directory, environment variables
- [S3 Compatibility](guides/s3-compatibility.md) — AWS CLI, rclone, SDK setup
- [Installation](installation.md) — Docker Compose examples and deployment options
- [All Guides](README.md) — Full documentation index
