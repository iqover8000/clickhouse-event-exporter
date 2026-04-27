from datetime import datetime, timezone
from typing import Any, Callable

from flask import Flask, jsonify

from settings import Settings
from streamer import ClickHouseLogStreamer


def create_app(
    streamer: ClickHouseLogStreamer,
    settings: Settings,
    stream_alive: Callable[[], bool],
) -> Flask:
    app = Flask(__name__)

    @app.route("/actuator/health", methods=["GET"])
    def health() -> Any:
        now = datetime.now(timezone.utc)
        alive = stream_alive()

        if streamer.last_loop_at is None:
            stale = True
        else:
            age = (now - streamer.last_loop_at).total_seconds()
            stale = age > settings.health_stale_seconds

        if not alive or stale:
            return (
                jsonify(
                    {
                        "status": "DOWN",
                        "stream_thread_alive": alive,
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
                    "streamer_id": settings.streamer_id,
                    "log_table": settings.log_table,
                    "severity": settings.severity,
                    "output_format": settings.output_format,
                    "poll_interval_seconds": settings.poll_interval_seconds,
                    "batch_size": settings.batch_size,
                    "offset_table": settings.offset_table,
                    "health_stale_seconds": settings.health_stale_seconds,
                    "lookback_max_seconds": settings.lookback_max_seconds,
                }
            ),
            200,
        )

    return app
