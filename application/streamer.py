import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import clickhouse_connect

from serialization import as_json_value, coerce_utc_datetime, to_utc_datetime
from settings import Settings


logger = logging.getLogger("clickhouse-event-exporter")


@dataclass(frozen=True)
class OffsetState:
    offset_ts: datetime
    offset_ts_us: int
    offset_hash: int


class ClickHouseLogStreamer:
    ROW_HASH_ALIAS = "_streamer_row_hash"
    ROW_HASH_EXPR = "cityHash64(toString(tuple(*)))"
    ROW_TS_ALIAS = "_streamer_row_ts"
    ROW_TS_US_ALIAS = "_streamer_row_ts_us"

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
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.settings.offset_table}
        (
            streamer_id String,
            offset_ts DateTime64(6, 'UTC'),
            offset_ts_us UInt64 DEFAULT 0,
            offset_hash UInt64 DEFAULT 0,
            updated_at DateTime64(6, 'UTC') DEFAULT now64(6)
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (streamer_id)
        """
        self.client.command(query)

    def load_offset(self) -> OffsetState:
        query = f"""
        SELECT offset_ts, offset_ts_us, offset_hash
        FROM {self.settings.offset_table} FINAL
        WHERE streamer_id = {{streamer_id:String}}
        LIMIT 1
        SETTINGS final = 1
        """
        result = self.client.query(query, parameters={"streamer_id": self.settings.streamer_id})
        if result.result_rows:
            row = result.result_rows[0]
            offset_ts = to_utc_datetime(row[0])
            offset_ts_us = int(row[1] or 0)
            if offset_ts_us == 0:
                offset_ts_us = int(offset_ts.timestamp() * 1_000_000)
            loaded = OffsetState(
                offset_ts=offset_ts,
                offset_ts_us=offset_ts_us,
                offset_hash=int(row[2] or 0),
            )
            logger.info(
                "Loaded offset ts=%s us=%s hash=%s for streamer_id=%s",
                loaded.offset_ts.isoformat(),
                loaded.offset_ts_us,
                loaded.offset_hash,
                self.settings.streamer_id,
            )
            return loaded

        startup_ts = datetime.now(timezone.utc)
        return OffsetState(
            offset_ts=startup_ts,
            offset_ts_us=int(startup_ts.timestamp() * 1_000_000),
            offset_hash=0,
        )

    def save_offset(self, offset: OffsetState) -> None:
        query = f"""
        INSERT INTO {self.settings.offset_table}
            (streamer_id, offset_ts, offset_ts_us, offset_hash)
        VALUES
            ({{streamer_id:String}}, {{offset_ts:DateTime64(6)}}, {{offset_ts_us:UInt64}}, {{offset_hash:UInt64}})
        """
        self.client.command(
            query,
            parameters={
                "streamer_id": self.settings.streamer_id,
                "offset_ts": offset.offset_ts,
                "offset_ts_us": offset.offset_ts_us,
                "offset_hash": offset.offset_hash,
            },
        )

    def fetch_logs(self, offset: OffsetState) -> List[Dict[str, Any]]:
        query_start_us = offset.offset_ts_us
        query_start_hash = offset.offset_hash

        if self.settings.lookback_max_seconds > 0:
            lookback_start = datetime.now(timezone.utc) - timedelta(
                seconds=self.settings.lookback_max_seconds
            )
            lookback_us = int(lookback_start.timestamp() * 1_000_000)
            if lookback_us > query_start_us:
                query_start_us = lookback_us
                query_start_hash = 0

        query = f"""
        SELECT *
        FROM
        (
          SELECT
            *,
            {self.ROW_HASH_EXPR} AS {self.ROW_HASH_ALIAS},
            toDateTime64({self.settings.log_ts_column}, 6, 'UTC') AS {self.ROW_TS_ALIAS},
            toUnixTimestamp64Micro({self.settings.log_ts_column}) AS {self.ROW_TS_US_ALIAS}
          FROM {self.settings.log_table}
          WHERE CAST({self.settings.log_severity_column}, 'Int32') <= {{severity_rank:Int32}}
        ) AS rows
        WHERE (
            rows.{self.ROW_TS_US_ALIAS} > {{query_start_ts_us:UInt64}}
            OR (
                rows.{self.ROW_TS_US_ALIAS} = {{query_start_ts_us:UInt64}}
                AND rows.{self.ROW_HASH_ALIAS} > {{offset_hash:UInt64}}
            )
        )
        ORDER BY rows.{self.ROW_TS_US_ALIAS} ASC, rows.{self.ROW_HASH_ALIAS} ASC
        LIMIT {{batch_size:Int32}}
        SETTINGS final = 1
        """
        result = self.client.query(
            query,
            parameters={
                "query_start_ts_us": query_start_us,
                "offset_hash": query_start_hash,
                "severity_rank": self.settings.severity_rank,
                "batch_size": self.settings.batch_size,
            },
        )

        return [dict(row) for row in result.named_results()]

    def emit_row(self, row: Dict[str, Any]) -> None:
        ts_val = row.get(self.settings.log_ts_column)
        severity_val = row.get(self.settings.log_severity_column)
        message_val = row.get(self.settings.log_message_column)

        if self.settings.output_format == "json":
            payload = {key: as_json_value(value) for key, value in row.items()}
            payload["streamer_id"] = self.settings.streamer_id
            print(json.dumps(payload, ensure_ascii=False), flush=True)
            return

        if isinstance(ts_val, datetime):
            ts_text = to_utc_datetime(ts_val).isoformat()
        elif ts_val is None:
            ts_text = ""
        else:
            ts_text = str(ts_val)

        severity_text = str(severity_val) if severity_val is not None else "UNKNOWN"
        message_text = str(message_val) if message_val is not None else ""
        print(f"{ts_text} [{severity_text}] {message_text}", flush=True)

    @staticmethod
    def _next_offset(current: OffsetState, row_ts: datetime, row_ts_us: int, row_hash: int) -> OffsetState:
        if row_ts_us > current.offset_ts_us:
            return OffsetState(offset_ts=row_ts, offset_ts_us=row_ts_us, offset_hash=row_hash)
        if row_ts_us == current.offset_ts_us and row_hash > current.offset_hash:
            return OffsetState(offset_ts=row_ts, offset_ts_us=row_ts_us, offset_hash=row_hash)
        return current

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.ensure_offset_table()
                offset = self.load_offset()

                while not self.stop_event.is_set():
                    rows = self.fetch_logs(offset)
                    next_offset = offset

                    for row in rows:
                        row_hash = int(row.pop(self.ROW_HASH_ALIAS, 0))
                        row_ts_raw = row.pop(self.ROW_TS_ALIAS, None)
                        row_ts_us = int(row.pop(self.ROW_TS_US_ALIAS, 0) or 0)
                        self.emit_row(row)

                        row_ts = coerce_utc_datetime(row_ts_raw)
                        if row_ts is None:
                            row_ts = coerce_utc_datetime(row.get(self.settings.log_ts_column))

                        if row_ts is not None and row_ts_us > 0:
                            next_offset = self._next_offset(next_offset, row_ts, row_ts_us, row_hash)

                    if next_offset != offset:
                        self.save_offset(next_offset)
                        offset = next_offset

                    self.last_error = None
                    self.last_loop_at = datetime.now(timezone.utc)
                    time.sleep(self.settings.poll_interval_seconds)
            except Exception:
                self.last_error = "streamer_failed"
                logger.exception("Streaming loop failed, restarting")
                time.sleep(self.settings.poll_interval_seconds)

    def stop(self) -> None:
        self.stop_event.set()
