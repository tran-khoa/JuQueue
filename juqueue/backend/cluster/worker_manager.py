from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import multiprocessing
import signal
import socket
import subprocess
from typing import Dict, Optional, Tuple, Union, List, Callable

import aiohttp.web
import aioprocessing
from loguru import logger

from aiohttp import web
import socketio


from juqueue.backend.utils import AsyncMultiprocessingQueue, standard_error_handler
from juqueue.config import Config
from juqueue.backend.cluster.messages import AckResponse, InternalMessage, InitResponse, BaseMessage, AckStatusRequest, \
    AckReportRequest, \
    RequestMessage, ResponseMessage


class WorkerManagerServer:
    def __init__(self):
        self.loop_task = None
        self._incoming_queue = AsyncMultiprocessingQueue[BaseMessage]()  # from workers
        self._outgoing_queue = AsyncMultiprocessingQueue[BaseMessage]()  # to workers
        self.address = None

        self.ready_event = asyncio.Event()

    @property
    def incoming_messages(self) -> AsyncMultiprocessingQueue:
        return self._incoming_queue

    def send_message(self, message: BaseMessage):
        self._outgoing_queue.put(message)

    async def start(self):
        self.loop_task = asyncio.create_task(self._start_loop())
        await self.ready_event.wait()

    async def stop(self):
        if self.loop_task is None:
            return

        self.loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.loop_task

        self._incoming_queue.close()
        self._outgoing_queue.close()

        logger.debug("WorkerManagerServer stopped!")

    @staticmethod
    async def _handle_inputs(sio: socketio.AsyncServer, queue: AsyncMultiprocessingQueue):
        while True:
            message = await queue.aget()
            if message is None:
                return

            assert isinstance(message, (RequestMessage, ResponseMessage))
            await sio.emit(message, to=message.worker_id)

    async def _start_loop(self):
        loop = asyncio.get_running_loop()
        process_task = None

        try:
            process_task = asyncio.ensure_future(
                loop.run_in_executor(None, self._server, self._outgoing_queue, self._incoming_queue)
            )
            message = await asyncio.wait_for(self._incoming_queue.aget(), 5)
            assert isinstance(message, InitResponse)
            self.address = message.address
            self.ready_event.set()
            await process_task
        except asyncio.CancelledError:
            if process_task is not None:
                process_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await process_task
        finally:
            self._outgoing_queue.close()
            self._incoming_queue.close()

    @staticmethod
    def _server(outgoing_queue: AsyncMultiprocessingQueue,
                incoming_queue: AsyncMultiprocessingQueue,
                bind_interface: Optional[str] = None):
        """
        Should be run in a separate process
        """
        try:
            loop = asyncio.new_event_loop()
            address, sock = WorkerManagerServer._get_socket(bind_interface)
            incoming_queue.put(InitResponse(address))

            sio = socketio.AsyncServer(
                async_mode="aiohttp"
            )
            app = web.Application()
            sio.attach(app)
            input_handler = loop.create_task(WorkerManagerServer._handle_inputs(sio, outgoing_queue))

            web.run_app(app, sock=sock, shutdown_timeout=1)
            logger.debug("CentralWorkerManager loop process closing...")
            input_handler.cancel()

            incoming_queue.put(None)

            loop.stop()
            loop.close()
        except:
            logger.exception("Exception occured in CentralWorkerManager loop process.")
            raise

    @staticmethod
    def _get_socket(bind_interface: Optional[str] = None) -> Tuple[str, socket.socket]:
        def get_ip_interface(ifname):
            """
            Adapted from dask.distributed

            Get the local IPv4 address of a network interface.

            KeyError is raised if the interface doesn't exist.
            ValueError is raised if the interface does no have an IPv4 address
            associated with it.
            """
            import psutil

            net_if_addrs = psutil.net_if_addrs()

            if ifname not in net_if_addrs:
                allowed_ifnames = list(net_if_addrs.keys())
                raise ValueError(
                    "{!r} is not a valid network interface. "
                    "Valid network interfaces are: {}".format(ifname, allowed_ifnames)
                )

            for info in net_if_addrs[ifname]:
                if info.family == socket.AF_INET:
                    return info.address
            raise ValueError(f"interface {ifname!r} doesn't have an IPv4 address")

        sock = socket.socket()
        if bind_interface is None:
            sock.bind((socket.gethostname(), 0))
        else:
            sock.bind((get_ip_interface(bind_interface), 0))
        sock.listen(128)

        return "tcp://{}:{}".format(*sock.getsockname()), sock


class CentralWorkerManager:
    def __init__(self, config: Config):
        self.config = config
        self.address = None
        self.managers = {}

        self.server = WorkerManagerServer()

        self.handler = asyncio.create_task(self.message_handler())  # Use main thread for message handling for now

    async def start(self):
        await self.server.start()

    async def message_handler(self):
        while True:
            await asyncio.sleep(3600)
            """
            message = await self.server.incoming_messages.coro_get()
            assert isinstance(message, BaseMessage)

            await self.handle_message(message)
            """

    async def handle_message(self, message: BaseMessage):
        if isinstance(message, AckStatusRequest):
            pass
        else:
            raise ValueError(f"Unknown message type {type(message)}!")

    def build_worker_manager(self, name: str, **kwargs) -> WorkerManager:
        self.managers[name] = WorkerManager(self, cluster_name=name, **kwargs)
        return self.managers[name]

    async def stop(self):
        self.handler.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.handler

        await self.server.stop()


class WorkerManager:
    """
    Manages JuQueue workers, belongs to exactly one cluster manager.
    """

    def __init__(self,
                 central_worker_manager: CentralWorkerManager,
                 *,
                 cluster_name: str,
                 max_jobs: int):
        self.central_worker_manager = central_worker_manager
        self.cluster_name = cluster_name
        self.max_jobs = max_jobs
