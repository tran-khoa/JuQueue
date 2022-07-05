import asyncio
import concurrent.futures
import contextlib
import multiprocessing
import threading
import time

import pytest
import pytest_asyncio
import socketio
import zmq
import uuid

from aiohttp import web

from juqueue.backend.cluster.messages import BaseMessage, InitResponse, RequestMessage
from juqueue.backend.cluster.worker import Worker, worker
from juqueue.backend.cluster.worker_manager import WorkerManagerServer

WORKER_CONF = {
    "cluster_name": "cluster-name",
    "worker_id": str(uuid.uuid4()),
    "num_slots": 4,
    "report_interval": 30,
    "timeout": 1,
    "num_retries": 0
}


async def stop(sio, runner):
    await runner.cleanup()

    # Ugly workaround...
    for task in asyncio.all_tasks():
        if task.get_coro().cr_code == sio.eio._service_task().cr_code:  # noqa
            with contextlib.suppress(asyncio.CancelledError):
                task.cancel()
                await task
            break


@pytest_asyncio.fixture
async def mock_server():
    sio = socketio.AsyncServer(
        async_mode="aiohttp",
        logger=True, engineio_logger=True
    )

    app = web.Application()
    sio.attach(app)

    @sio.event
    def worker_message(sid, data):
        print(f"sid:{sid},data:{data}")

    runner = web.AppRunner(app)
    await runner.setup()

    address, socket = WorkerManagerServer._get_socket()  # noqa
    site = web.SockSite(runner, socket, shutdown_timeout=1)
    await site.start()

    yield address, sio, runner

    await stop(sio, runner)


def test_worker_no_connection():
    with pytest.raises(ConnectionError):
        asyncio.run(worker(
            manager_addr="localhost",
            **WORKER_CONF)
        )


@pytest.mark.asyncio
async def test_worker_connect(mock_server):
    addr, _, _ = mock_server
    w = Worker(manager_addr=addr, **WORKER_CONF)
    await w.start(1, wait=False)


@pytest.mark.asyncio
async def test_message_send(mock_server):
    addr, sio, _ = mock_server

    w = Worker(manager_addr=addr, **WORKER_CONF)
    main_task = asyncio.create_task(w.start(1))

    msg_received = asyncio.Event()

    @sio.event
    def worker_message(sid, data):
        msg_received.set()

    await w.ready.wait()
    await w.send_message(RequestMessage(cluster_name="cluster", worker_id="test"))

    await asyncio.wait_for(msg_received.wait(), 1)

    with contextlib.suppress(asyncio.CancelledError):
        main_task.cancel()
        await main_task


@pytest.mark.asyncio
async def test_server_failure(mock_server):
    addr, sio, runner = mock_server

    w = Worker(manager_addr=addr, **WORKER_CONF)
    main_task = asyncio.create_task(w.start(1))

    await w.ready.wait()
    await stop(sio, runner)

    # TODO handle?
    #await w.send_message(RequestMessage(cluster_name="cluster", worker_id="test"))

    # Should detect death of server and cancel
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(15)
        print(main_task)
        assert main_task.done()
