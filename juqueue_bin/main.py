#!/usr/bin/env python

import argparse
import asyncio
import signal
from importlib.resources import files
from pathlib import Path
import socket

import nest_asyncio
import uvloop
from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute
from filelock import FileLock, Timeout
from hypercorn.asyncio import Config as HypercornConfig, serve
from loguru import logger
from starlette.staticfiles import StaticFiles
from tornado.ioloop import IOLoop

import juqueue
import juqueue.assets.juqueue_web
from juqueue.api import API_ROUTERS
from juqueue.backend.backend import Backend
from juqueue.backend.utils import standard_error_handler
from juqueue.config import Config, HasConfigField
from juqueue.logger import setup_logger

MAIN_ROUTER = APIRouter()


class Server(HasConfigField):
    def __init__(self, config: Config):
        self.config = config

        if not config.debug:
            uvloop.install()
        self._event_loop = asyncio.new_event_loop()
        self._event_loop.set_debug(config.debug)
        asyncio.set_event_loop(self._event_loop)

        if config.debug:
            # Nested async loops for IPython debug
            nest_asyncio.apply(self._event_loop)

        self._event_loop.set_exception_handler(standard_error_handler)

        self._tornado_loop = IOLoop.current()

        self._ip = socket.gethostbyname(socket.gethostname())

        assert self.config.port > 0 or self.config.unix_socket is not None

        self.address = f"{self._ip}:{self.config.port}"
        binds = []
        if self.config.port > 0:
            binds.extend([self.address, f"localhost:{self.config.port}"])
        if self.config.unix_socket:
            binds.append(f"unix:{self.config.unix_socket.as_posix()}")
        self._hypercorn_config = HypercornConfig.from_mapping({"bind": binds})
        self._api = FastAPI(
            title="JuQueue",
            version=juqueue.__version__
        )
        for router in API_ROUTERS:
            self._api.include_router(router)

        self._api.include_router(MAIN_ROUTER)
        self._api.mount("/",
                        StaticFiles(directory=files(juqueue.assets.juqueue_web), html=True),  # noqa
                        name="app")
        for route in self._api.routes:
            if isinstance(route, APIRoute):
                route.operation_id = route.name  # in this case, 'read_items'

        self._backend = Backend(config, self.on_backend_shutdown())
        self._shutdown_event = asyncio.Event()

        for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            self._event_loop.add_signal_handler(
                s, lambda: asyncio.create_task(self._backend.stop())
            )

    async def _initialize(self):
        await self._backend.initialize()

        # noinspection PyTypeChecker
        await serve(self._api, self._hypercorn_config, shutdown_trigger=self._shutdown_event.wait)

        self._tornado_loop.stop()

    def start(self):
        self._event_loop.create_task(self._initialize(), name="main")

        try:
            self._tornado_loop.start()
        finally:
            logger.info("JuQueue shut down. Goodbye.")
            self._tornado_loop.close()

    async def on_backend_shutdown(self):
        self._shutdown_event.set()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--def-dir",
                        help="Path to definitions, see example_defs/ for reference, available on master node.",
                        type=Path, required=True)
    parser.add_argument("--work-dir",
                        help="Path to work and metadata files, available on master and worker nodes.",
                        type=Path, required=True)
    parser.add_argument("--port",
                        help="Optional port used for api/web, defaults to 51234. Set to -1 to disable.",
                        default=51234,
                        type=int)
    parser.add_argument("--unix-socket",
                        help="Optional path to unix socket as an alternative to port-based access.",
                        default=None,
                        required=False,
                        type=Path)
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, type=bool)
    args = parser.parse_args()

    if not args.def_dir.exists():
        raise FileNotFoundError(f"Definitions path {args.def_dir} does not exist.")

    args.def_dir = args.def_dir.expanduser().resolve()
    args.work_dir = args.work_dir.expanduser().resolve()
    args.work_dir.mkdir(parents=True, exist_ok=True)

    lock = FileLock(args.work_dir / ".lock", timeout=2)
    try:
        with lock:
            # Setting up logging
            log_path = args.work_dir / "logs"
            setup_logger(log_path, args.debug)

            if args.debug:
                logger.debug("Running in debug mode.")

            # Start the server
            server = Server(Config(**vars(args)))
            server.start()
    except Timeout:
        raise RuntimeError(f"JuQueue is already running, {args.work_dir / '.lock'} still locked!")
    finally:
        lock.release()


if __name__ == '__main__':
    main()
