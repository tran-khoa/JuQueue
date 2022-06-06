from __future__ import annotations

import asyncio
from enum import Enum
from typing import Literal

from loguru import logger

ALL_EXPERIMENTS = "@ALL_EXPERIMENTS"
ALL_RUNS = "@ALL_RUNS"


class CancellationReason(str, Enum):
    SERVER_SHUTDOWN = "server_shutdown"
    WORKER_SHUTDOWN = "worker_shutdown"
    USER_CANCEL = "user"


RunStatus = Literal['running', 'ready', 'failed', 'inactive', 'finished']


def generic_error_handler(future: asyncio.Future):
    exc = future.exception()
    if exc:
        if isinstance(exc, asyncio.TimeoutError):
            return

        from juqueue.backend.backend import Backend

        logger.opt(exception=exc).exception(f"An unexpected exception occured ({exc}), stopping the backend... "
                                            "Please report this issue!")
        asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(
            Backend.instance().stop()
        ))


class RunEvent(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    CANCELLED_RUN_CHANGED = "cancelled_run_changed"
    CANCELLED_WORKER_DEATH = "cancelled_worker_death"
    CANCELLED_WORKER_SHUTDOWN = "cancelled_worker_shutdown"
    CANCELLED_SERVER_SHUTDOWN = "cancelled_server_shutdown"
    CANCELLED_USER = "cancelled_user"
    FAILED = "failed"
