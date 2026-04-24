# clickhouse-logs-streamer

Flask service that continuously reads logs from a ClickHouse system log table,
prints them to stdout, and stores a streaming offset in ClickHouse.

## Repository Layout

- `application/` - Python application source
- `helm/clickhouse-logs-streamer/` - Helm chart
- `Dockerfile` - Docker image build (uses `application/`)
- `docker-compose.yaml` - local run configuration

## Run with Docker Compose

1. Fill required variables in `.env`:
   - `CH_HOST`
   - `CH_PASSWORD`
2. Start the service:

```bash
docker compose up --build
```

3. Run in background:

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
- `LOG_SEVERITY` (default: `information`)
  - one of: `fatal`, `critical`, `error`, `warning`, `notice`, `information`, `debug`, `trace`, `test`
  - threshold behavior: for example, `error` streams `fatal`, `critical`, and `error`
- `LOG_OUTPUT_FORMAT` (default: `raw`)
  - `raw` or `json`
- `CH_OFFSET_TABLE` (default: `default.logs_streamer_offsets`)
- `STREAMER_ID` (default: `default-streamer`)
- `POLL_INTERVAL_SECONDS` (default: `10`)
- `BATCH_SIZE` (default: `1000`)
- `HEALTH_STALE_SECONDS` (default: `30`)
- `LOOKBACK_MAX_SECONDS` (default: `3600`)
  - limits query scan to at most the last N seconds to avoid growing scan windows during idle periods
- `FLASK_HOST` (default: `0.0.0.0`)
- `FLASK_PORT` (default: `8080`)

## How It Works

1. On startup, loads offset for `STREAMER_ID` from `CH_OFFSET_TABLE`.
2. If no offset exists, starts from current startup time.
3. Runs `SELECT` from the log table with:
   - severity threshold
   - offset cursor
   - lookback cap (`LOOKBACK_MAX_SECONDS`)
4. Prints logs to stdout in `raw` or `json` format.
5. Persists new offset to `CH_OFFSET_TABLE` as `timestamp + row_hash`.
6. Sleeps for `POLL_INTERVAL_SECONDS` and repeats.

## Health Endpoints

- `GET /actuator/health`
  - returns `500/DOWN` if the streaming thread is dead or stale
  - supervisor thread auto-restarts streamer thread
- `GET /actuator/config`

## Helm Chart

Chart path: `helm/clickhouse-logs-streamer`

Example install:

```bash
helm upgrade --install clickhouse-logs-streamer ./helm/clickhouse-logs-streamer \
  --set env.CH_HOST=<host> \
  --set env.CH_PASSWORD=<password>
```
