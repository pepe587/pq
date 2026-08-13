import signal
import os
import time
from pq.signals import WorkerStop, install_handlers


def test_worker_stop_starts_false():
    ws = WorkerStop()
    assert ws.should_stop is False


def test_sigint_sets_flag():
    ws = WorkerStop()
    install_handlers(ws)
    os.kill(os.getpid(), signal.SIGINT)
    time.sleep(0.05)
    assert ws.should_stop is True
    # Reset for other tests
    ws.should_stop = False
