import json
import logging
import os
import re
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

import clickhouse_connect
from flask import Flask, jsonify


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("clickhouse-logs-streamer")

app = Flask(__name__)


SEVERITY_TO_RANK = {
    "fatal": 1,
    "critical": 2,
    "error": 3,
    "warning": 4,
    "notice": 5,
    "information": 6,
    "debug": 7,
    "trace": 8,
    "test": 9,
}


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


def env_str(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def validate_identifier(name: str, value: str) -> str:
    if not _IDENTIFIER.match(value):
        raise ValueError(f"{name} has invalid identifier value: {value}")
    return value


def to_utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raise TypeError(f"Expected datetime, got {type(value)}")


def as_json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return to_utc_datetime(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


@dataclass
class OffsetState:
    offset_ts: datetime
    offset_hash: int


@dataclass
class Settings:
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    ch_database: str
    ch_secure: bool
    ch_verify: bool

    log_table: str
    log_ts_column: str
    log_severity_column: str
    log_message_column: str

    severity: str
    severity_rank: int
    output_format: str

    offset_table: str
    streamer_id: str
    poll_interval_seconds: int
    batch_size: int
    health_stale_seconds: int
    lookback_max_seconds: int

    flask_host: str
    flask_port: int

    @staticmethod
    def from_env() -> "Settings":
        severity = env_str("LOG_SEVERITY", "Information").strip().lower()
        if severity not in SEVERITY_TO_RANK:
            raise ValueError(
                "LOG_SEVERITY must be one of: "
                + ", ".join(sorted(SEVERITY_TO_RANK.keys()))
            )

        output_format = env_str("LOG_OUTPUT_FORMAT", "raw").strip().lower()
        if output_format not in {"raw", "json"}:
            raise ValueError("LOG_OUTPUT_FORMAT must be raw or json")

        poll_interval_seconds = env_int("POLL_INTERVAL_SECONDS", 10)

        return Settings(
            ch_host=env_str("CH_HOST"),
            ch_port=env_int("CH_PORT", 8443),
            ch_user=env_str("CH_USER", "default"),
            ch_password=env_str("CH_PASSWORD"),
            ch_database=env_str("CH_DATABASE", "default"),
            ch_secure=env_bool("CH_SECURE", True),
            ch_verify=env_bool("CH_VERIFY", True),
            log_table=validate_identifier(
                "CH_LOG_TABLE", env_str("CH_LOG_TABLE", "system.text_log")
            ),
            log_ts_column=validate_identifier(
                "CH_LOG_TIMESTAMP_COLUMN",
                env_str("CH_LOG_TIMESTAMP_COLUMN", "event_time_microseconds"),
            ),
            log_severity_column=validate_identifier(
                "CH_LOG_SEVERITY_COLUMN", env_str("CH_LOG_SEVERITY_COLUMN", "level")
            ),
            log_message_column=validate_identifier(
                "CH_LOG_MESSAGE_COLUMN", env_str("CH_LOG_MESSAGE_COLUMN", "message")
            ),
            severity=severity,
            severity_rank=SEVERITY_TO_RANK[severity],
            output_format=output_format,
            offset_table=validate_identifier(
                "CH_OFFSET_TABLE",
                env_str("CH_OFFSET_TABLE", "default.logs_streamer_offsets"),
            ),
            streamer_id=env_str("STREAMER_ID", "default-streamer"),
            poll_interval_seconds=poll_interval_seconds,
            batch_size=env_int("BATCH_SIZE", 1000),
            health_stale_seconds=env_int("HEALTH_STALE_SECONDS", max(30, poll_interval_seconds * 3)),
            lookback_max_seconds=env_int("LOOKBACK_MAX_SECONDS", 3600),
            flask_host=env_str("FLASK_HOST", "0.0.0.0"),
            flask_port=env_int("FLASK_PORT", 8080),
        )


SETTINGS = Settings.from_env()


class ClickHouseLogStreamer:
    ROW_HASH_ALIAS = "_streamer_row_hash"
    ROW_HASH_EXPR = "cityHash64(toString(tuple(*)))"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.stop_event = threading.Event()
        self.last_loop_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.client = clickhouse_connect.get_client(
            host=settings.ch_host,
            port=settings.ch_port,
            username=settings.ch_user,
            password=settings.ch_password,
            database=settings.ch_database,
            secure=settings.ch_secure,
            verify=settings.ch_verify,
        )

    def ensure_offset_table(self) -> None:
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {self.settings.offset_table}
        (
            streamer_id String,
            offset_ts DateTime64(6, 'UTC'),
            offset_hash UInt64 DEFAULT 0,
            updated_at DateTime64(6, 'UTC') DEFAULT now64(6)
        )
        ENGINE = MergeTree
        ORDER BY (streamer_id, updated_at)
        """
        self.client.command(create_query)

        alter_query = f"""
        ALTER TABLE {self.settings.offset_table}
        ADD COLUMN IF NOT EXISTS offset_hash UInt64 DEFAULT 0
        """
        self.client.command(alter_query)

    def load_offset(self) -> OffsetState:
        query = f"""
        SELECT offset_ts, offset_hash
        FROM {self.settings.offset_table}
        WHERE streamer_id = {{streamer_id:String}}
        ORDER BY updated_at DESC
        LIMIT 1
        """
        result = self.client.query(query, parameters={"streamer_id": self.settings.streamer_id})
        if result.result_rows:
            row = result.result_rows[0]
            offset = OffsetState(
                offset_ts=to_utc_datetime(row[0]),
                offset_hash=int(row[1] or 0),
            )
            logger.info(
                "Loaded offset ts=%s hash=%s for streamer_id=%s",
                offset.offset_ts.isoformat(),
                offset.offset_hash,
                self.settings.streamer_id,
            )
            return offset

        startup_ts = datetime.now(timezone.utc)
        offset = OffsetState(offset_ts=startup_ts, offset_hash=0)
        logger.info(
            "Offset not found for streamer_id=%s. Starting from startup time %s",
            self.settings.streamer_id,
            offset.offset_ts.isoformat(),
        )
        return offset

    def save_offset(self, offset: OffsetState) -> None:
        query = f"""
        INSERT INTO {self.settings.offset_table} (streamer_id, offset_ts, offset_hash)
        VALUES ({{streamer_id:String}}, {{offset_ts:DateTime64(6)}}, {{offset_hash:UInt64}})
        """
        self.client.command(
            query,
            parameters={
                "streamer_id": self.settings.streamer_id,
                "offset_ts": offset.offset_ts,
                "offset_hash": offset.offset_hash,
            },
        )

    def fetch_logs(self, offset: OffsetState) -> List[Dict[str, Any]]:
        query_start = offset.offset_ts
        query_start_hash = offset.offset_hash
        if self.settings.lookback_max_seconds > 0:
            lookback_start = datetime.now(timezone.utc) - timedelta(
                seconds=self.settings.lookback_max_seconds
            )
            if lookback_start > query_start:
                query_start = lookback_start
                query_start_hash = 0

        query = f"""
        SELECT *, {self.ROW_HASH_EXPR} AS {self.ROW_HASH_ALIAS}
        FROM {self.settings.log_table}
        WHERE (
            {self.settings.log_ts_column} > {{query_start_ts:DateTime64(6)}}
            OR (
                {self.settings.log_ts_column} = {{query_start_ts:DateTime64(6)}}
                AND {self.ROW_HASH_EXPR} > {{offset_hash:UInt64}}
            )
        )
          AND CAST({self.settings.log_severity_column}, 'Int32') <= {{severity_rank:Int32}}
        ORDER BY {self.settings.log_ts_column} ASC, {self.ROW_HASH_ALIAS} ASC
        LIMIT {{batch_size:Int32}}
        """
        result = self.client.query(
            query,
            parameters={
                "query_start_ts": query_start,
                "offset_hash": query_start_hash,
                "severity_rank": self.settings.severity_rank,
                "batch_size": self.settings.batch_size,
            },
        )

        if not result.result_rows:
            return []

        rows: List[Dict[str, Any]] = []
        for row in result.named_results():
            rows.append(dict(row))
        return rows

    def emit_row(self, row: Dict[str, Any]) -> None:
        ts_val = row.get(self.settings.log_ts_column)
        severity_val = row.get(self.settings.log_severity_column)
        message_val = row.get(self.settings.log_message_column)

        if self.settings.output_format == "json":
            payload = {key: as_json_value(value) for key, value in row.items()}
            payload["streamer_id"] = self.settings.streamer_id
            print(json.dumps(payload, ensure_ascii=False), flush=True)
            return

        ts_text = ""
        if isinstance(ts_val, datetime):
            ts_text = to_utc_datetime(ts_val).isoformat()
        elif ts_val is not None:
            ts_text = str(ts_val)
        severity_text = str(severity_val) if severity_val is not None else "UNKNOWN"
        message_text = str(message_val) if message_val is not None else ""
        print(f"{ts_text} [{severity_text}] {message_text}", flush=True)

    def _max_offset(self, current: OffsetState, row_ts: datetime, row_hash: int) -> OffsetState:
        if row_ts > current.offset_ts:
            return OffsetState(offset_ts=row_ts, offset_hash=row_hash)
        if row_ts == current.offset_ts and row_hash > current.offset_hash:
            return OffsetState(offset_ts=row_ts, offset_hash=row_hash)
        return current

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.ensure_offset_table()
                offset = self.load_offset()

                while not self.stop_event.is_set():
                    rows = self.fetch_logs(offset)
                    max_offset = offset

                    for row in rows:
                        row_hash = int(row.pop(self.ROW_HASH_ALIAS, 0))
                        self.emit_row(row)

                        row_offset_raw = row.get(self.settings.log_ts_column)
                        if isinstance(row_offset_raw, datetime):
                            row_offset = to_utc_datetime(row_offset_raw)
                            max_offset = self._max_offset(max_offset, row_offset, row_hash)

                    if (
                        max_offset.offset_ts != offset.offset_ts
                        or max_offset.offset_hash != offset.offset_hash
                    ):
                        self.save_offset(max_offset)
                        offset = max_offset

                    self.last_loop_at = datetime.now(timezone.utc)
                    self.last_error = None
                    time.sleep(self.settings.poll_interval_seconds)
            except Exception:
                self.last_error = "streamer_failed"
                logger.exception("Streaming loop failed, restarting")
                time.sleep(self.settings.poll_interval_seconds)

    def stop(self) -> None:
        self.stop_event.set()


streamer = ClickHouseLogStreamer(SETTINGS)
stream_thread: Optional[threading.Thread] = None
stream_thread_lock = threading.Lock()
supervisor_stop_event = threading.Event()
supervisor_thread: Optional[threading.Thread] = None


@app.route("/actuator/health", methods=["GET"])
def health() -> Any:
    now = datetime.now(timezone.utc)
    is_alive = stream_thread is not None and stream_thread.is_alive()

    stale = False
    if streamer.last_loop_at is None:
        stale = True
    else:
        age = (now - streamer.last_loop_at).total_seconds()
        stale = age > SETTINGS.health_stale_seconds

    if not is_alive or stale:
        return (
            jsonify(
                {
                    "status": "DOWN",
                    "stream_thread_alive": is_alive,
                    "last_loop_at": streamer.last_loop_at.isoformat() if streamer.last_loop_at else None,
                    "last_error": streamer.last_error,
                }
            ),
            500,
        )

    return (
        jsonify(
            {
                "status": "UP",
                "stream_thread_alive": True,
                "last_loop_at": streamer.last_loop_at.isoformat(),
                "last_error": streamer.last_error,
            }
        ),
        200,
    )


@app.route("/actuator/config", methods=["GET"])
def config() -> Any:
    return (
        jsonify(
            {
                "streamer_id": SETTINGS.streamer_id,
                "log_table": SETTINGS.log_table,
                "severity": SETTINGS.severity,
                "output_format": SETTINGS.output_format,
                "poll_interval_seconds": SETTINGS.poll_interval_seconds,
                "batch_size": SETTINGS.batch_size,
                "offset_table": SETTINGS.offset_table,
                "health_stale_seconds": SETTINGS.health_stale_seconds,
                "lookback_max_seconds": SETTINGS.lookback_max_seconds,
            }
        ),
        200,
    )


def _handle_shutdown(*_: Iterable[Any]) -> None:
    streamer.stop()


def start_streamer_thread() -> threading.Thread:
    global stream_thread
    with stream_thread_lock:
        if stream_thread is None or not stream_thread.is_alive():
            stream_thread = threading.Thread(target=streamer.run_forever, daemon=True)
            stream_thread.start()
        return stream_thread


def _supervisor_loop() -> None:
    while not supervisor_stop_event.is_set():
        if stream_thread is None or not stream_thread.is_alive():
            logger.warning("Streamer thread is not alive, restarting")
            start_streamer_thread()
        time.sleep(2)


def start_supervisor_thread() -> threading.Thread:
    global supervisor_thread
    if supervisor_thread is None or not supervisor_thread.is_alive():
        supervisor_thread = threading.Thread(target=_supervisor_loop, daemon=True)
        supervisor_thread.start()
    return supervisor_thread


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


if __name__ == "__main__":
    start_streamer_thread()
    start_supervisor_thread()

    try:
        app.run(host=SETTINGS.flask_host, port=SETTINGS.flask_port)
    except Exception:
        logger.exception("Flask server stopped with an error")
        streamer.stop()
        raise
    finally:
        streamer.stop()
        supervisor_stop_event.set()
        if stream_thread is not None:
            stream_thread.join(timeout=5)
        if supervisor_thread is not None:
            supervisor_thread.join(timeout=5)
        sys.exit(0)
