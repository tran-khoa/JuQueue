import asyncio
import contextlib
import sys

from loguru import logger
import pytest
import pytest_asyncio
import unittest


from juqueue.backend.cluster.worker_manager import CentralWorkerManager

from .configs import DEFAULT_CONFIG


logger.remove()
logger.add(sys.stderr, level="DEBUG")


@pytest.mark.asyncio
async def test_address(event_loop):
    central = CentralWorkerManager(DEFAULT_CONFIG)
    await central.start()

    assert isinstance(central.server.address, str)

    with contextlib.suppress(asyncio.CancelledError):
        await central.stop()
