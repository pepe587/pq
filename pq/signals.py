"""Signal handling for the worker process."""
from __future__ import annotations

import signal
from dataclasses import dataclass


@dataclass
class WorkerStop:
    should_stop: bool = False


def install_handlers(ws: WorkerStop) -> None:
    """Install SIGINT and SIGTERM handlers that set ws.should_stop = True."""

    def _handler(signum, frame):
        ws.should_stop = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
