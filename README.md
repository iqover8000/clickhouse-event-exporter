# clickhouse-event-exporter

Flask-based service that tails ClickHouse system logs and prints them to stdout in `raw` or `json` format.
Streaming offset is persisted in ClickHouse.

## Repository Layout

- `application/app.py` - process entrypoint (runtime orchestration)
- `application/settings.py` - env parsing and validation
- `application/serialization.py` - datetime/json helpers
- `application/streamer.py` - ClickHouse streaming logic and offset management
- `application/web.py` - health/config HTTP endpoints
- `helm/clickhouse-event-exporter/` - Helm chart
- `Dockerfile` - image build
- `docker-compose.yaml` - local run setup

## CI/CD

- Push to any branch: runs `yamllint` and `flake8`
- Push a tag: builds and pushes a multi-arch image to GHCR
  - `linux/amd64`
  - `linux/arm64`

## Run with Docker Compose

1. Fill required values in `.env`:
   - `CH_HOST`
   - `CH_PASSWORD`
2. Start:

```bash
docker compose up --build
```

3. Start in background:

```bash
docker compose up -d --build
```

4. Stop:

```bash
docker compose down
```

## Environment Variables

- `CH_HOST` (required)
- `CH_PORT` (default: `8443`)
- `CH_USER` (default: `default`)
- `CH_PASSWORD` (required)
- `CH_DATABASE` (default: `default`)
- `CH_SECURE` (default: `true`)
- `CH_VERIFY` (default: `true`)
- `CH_LOG_TABLE` (default: `system.text_log`)
- `CH_LOG_TIMESTAMP_COLUMN` (default: `event_time_microseconds`)
- `CH_LOG_SEVERITY_COLUMN` (default: `level`)
- `CH_LOG_MESSAGE_COLUMN` (default: `message`)
- `LOG_SEVERITY` (default: `error`)
  - values: `fatal`, `critical`, `error`, `warning`, `notice`, `information`, `debug`, `trace`, `test`
  - threshold behavior: `error` streams `fatal`, `critical`, `error`
- `LOG_OUTPUT_FORMAT` (default: `raw`)
  - values: `raw`, `json`
- `CH_OFFSET_TABLE` (default: `default.logs_streamer_offsets`)
- `STREAMER_ID` (default: `default-streamer`)
- `POLL_INTERVAL_SECONDS` (default: `10`)
- `BATCH_SIZE` (default: `1000`)
- `HEALTH_STALE_SECONDS` (default: `max(30, POLL_INTERVAL_SECONDS * 3)`)
- `LOOKBACK_MAX_SECONDS` (default: `3600`)
  - limits max scan window during idle periods
- `FLASK_HOST` (default: `0.0.0.0`)
- `FLASK_PORT` (default: `8080`)

## Streaming Model

1. Ensures offset table exists in ClickHouse.
2. Loads latest offset for `STREAMER_ID`.
3. Reads logs by `(timestamp_microseconds, row_hash)` cursor.
4. Prints logs to stdout.
5. Saves new cursor into offset table.
6. Sleeps and repeats.

Offset table engine:
- `ReplacingMergeTree(updated_at)`
- key: `streamer_id`

## HTTP Endpoints

- `GET /actuator/health`
  - `200/UP` when stream thread is alive and not stale
  - `500/DOWN` otherwise
- `GET /actuator/config`

## Helm

Chart path: `helm/clickhouse-event-exporter`

Install example:

```bash
helm upgrade --install clickhouse-event-exporter ./helm/clickhouse-event-exporter \
  --set env.CH_HOST=<host> \
  --set env.CH_PASSWORD=<password>
```
