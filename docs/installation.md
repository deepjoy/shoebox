# Installation

## Docker (Recommended)

```bash
# From Docker Hub
docker pull deeppjoymajumdar/shoebox:latest
docker run -v /path/to/data:/data -p 9000:9000 deeppjoymajumdar/shoebox /data

# Or from GitHub Container Registry
docker pull ghcr.io/deepjoy/shoebox:latest
docker run -v /path/to/data:/data -p 9000:9000 ghcr.io/deepjoy/shoebox /data
```

### Docker Compose

A `docker-compose.yml` is included at the repo root for single-bucket usage:

```bash
docker compose up
```

#### Multiple Buckets

```yaml
services:
  shoebox:
    image: deeppjoymajumdar/shoebox:latest
    ports:
      - "9000:9000"
    volumes:
      - ./photos:/photos
      - ./documents:/documents
      - ./backups:/backups
    environment:
      SHOEBOX_LOG: info
    command: ["/photos", "/documents", "/backups"]
```

#### With Reverse Proxy (Traefik)

```yaml
services:
  shoebox:
    image: deeppjoymajumdar/shoebox:latest
    volumes:
      - ./data:/data
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.shoebox.rule=Host(`s3.example.com`)"
      - "traefik.http.routers.shoebox.tls.certresolver=letsencrypt"
    command: ["/data"]

  traefik:
    image: traefik:v2.10
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./traefik:/etc/traefik
```

## From crates.io

```bash
cargo install shoebox
```

## From Source

```bash
cargo install --git https://github.com/deepjoy/shoebox
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SHOEBOX_HOST` | Listen address (default: `0.0.0.0`) |
| `SHOEBOX_PORT` | Listen port (default: `9000`) |
| `SHOEBOX_DATA_DIR` | Directory for per-bucket state (config, metadata DB) |
| `SHOEBOX_CONFIG` | Path to global config file |
| `SHOEBOX_LOG` | Log level: `trace`, `debug`, `info`, `warn`, `error` (default: `info`) |
| `RUST_LOG` | Alternative log level (used if `SHOEBOX_LOG` not set) |

## Next Steps

- [Quickstart](quickstart.md) — Get running in 5 minutes
- [All Guides](README.md) — Full documentation index
