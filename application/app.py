import logging
import signal
import sys
import threading
import time
from typing import Any, Optional

from settings import Settings
from streamer import ClickHouseLogStreamer
from web import create_app


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("clickhouse-event-exporter")


class Runtime:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.streamer = ClickHouseLogStreamer(settings)

        self.stream_thread: Optional[threading.Thread] = None
        self.stream_thread_lock = threading.Lock()

        self.supervisor_thread: Optional[threading.Thread] = None
        self.supervisor_stop_event = threading.Event()

        self.app = create_app(self.streamer, self.settings, self.stream_thread_alive)

    def stream_thread_alive(self) -> bool:
        return self.stream_thread is not None and self.stream_thread.is_alive()

    def start_streamer_thread(self) -> threading.Thread:
        with self.stream_thread_lock:
            if self.stream_thread is None or not self.stream_thread.is_alive():
                self.stream_thread = threading.Thread(target=self.streamer.run_forever, daemon=True)
                self.stream_thread.start()
            return self.stream_thread

    def supervisor_loop(self) -> None:
        while not self.supervisor_stop_event.is_set():
            if not self.stream_thread_alive():
                logger.warning("Streamer thread is not alive, restarting")
                self.start_streamer_thread()
            time.sleep(2)

    def start_supervisor_thread(self) -> threading.Thread:
        if self.supervisor_thread is None or not self.supervisor_thread.is_alive():
            self.supervisor_thread = threading.Thread(target=self.supervisor_loop, daemon=True)
            self.supervisor_thread.start()
        return self.supervisor_thread

    def stop(self) -> None:
        self.streamer.stop()
        self.supervisor_stop_event.set()

        if self.stream_thread is not None:
            self.stream_thread.join(timeout=5)
        if self.supervisor_thread is not None:
            self.supervisor_thread.join(timeout=5)


def main() -> int:
    runtime = Runtime(Settings.from_env())

    def handle_shutdown(*_: Any) -> None:
        runtime.stop()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    runtime.start_streamer_thread()
    runtime.start_supervisor_thread()

    try:
        runtime.app.run(host=runtime.settings.flask_host, port=runtime.settings.flask_port)
    except Exception:
        logger.exception("Flask server stopped with an error")
        runtime.stop()
        raise
    finally:
        runtime.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
