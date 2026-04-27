import os
import re
from dataclasses import dataclass
from typing import Optional


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


@dataclass(frozen=True)
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
        severity = env_str("LOG_SEVERITY", "error").strip().lower()
        if severity not in SEVERITY_TO_RANK:
            raise ValueError(
                "LOG_SEVERITY must be one of: " + ", ".join(sorted(SEVERITY_TO_RANK.keys()))
            )

        output_format = env_str("LOG_OUTPUT_FORMAT", "raw").strip().lower()
        if output_format not in {"raw", "json"}:
            raise ValueError("LOG_OUTPUT_FORMAT must be raw or json")

        poll_interval_seconds = env_int("POLL_INTERVAL_SECONDS", 10)
        if poll_interval_seconds <= 0:
            raise ValueError("POLL_INTERVAL_SECONDS must be > 0")

        batch_size = env_int("BATCH_SIZE", 1000)
        if batch_size <= 0:
            raise ValueError("BATCH_SIZE must be > 0")

        lookback_max_seconds = env_int("LOOKBACK_MAX_SECONDS", 3600)
        if lookback_max_seconds < 0:
            raise ValueError("LOOKBACK_MAX_SECONDS must be >= 0")

        return Settings(
            ch_host=env_str("CH_HOST"),
            ch_port=env_int("CH_PORT", 8443),
            ch_user=env_str("CH_USER", "default"),
            ch_password=env_str("CH_PASSWORD"),
            ch_database=env_str("CH_DATABASE", "default"),
            ch_secure=env_bool("CH_SECURE", True),
            ch_verify=env_bool("CH_VERIFY", True),
            log_table=validate_identifier("CH_LOG_TABLE", env_str("CH_LOG_TABLE", "system.text_log")),
            log_ts_column=validate_identifier(
                "CH_LOG_TIMESTAMP_COLUMN", env_str("CH_LOG_TIMESTAMP_COLUMN", "event_time_microseconds")
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
                "CH_OFFSET_TABLE", env_str("CH_OFFSET_TABLE", "default.logs_streamer_offsets")
            ),
            streamer_id=env_str("STREAMER_ID", "default-streamer"),
            poll_interval_seconds=poll_interval_seconds,
            batch_size=batch_size,
            health_stale_seconds=env_int("HEALTH_STALE_SECONDS", max(30, poll_interval_seconds * 3)),
            lookback_max_seconds=lookback_max_seconds,
            flask_host=env_str("FLASK_HOST", "0.0.0.0"),
            flask_port=env_int("FLASK_PORT", 8080),
        )
