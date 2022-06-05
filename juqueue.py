#!/usr/bin/env python

import argparse
import asyncio
from pathlib import Path

import nest_asyncio
import uvloop
from fastapi import FastAPI
from filelock import FileLock, Timeout
from hypercorn.asyncio import Config as HypercornConfig, serve
from loguru import logger
from tornado.ioloop import IOLoop

from juqueue.api import API_ROUTERS
from juqueue.backend.backend import Backend
from juqueue.config import Config, HasConfigField
from juqueue.logger import setup_logger

# Assumes the lock file is accessible over all nodes
LOCK_FILE = Path(__file__).parent / ".lock"


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

        self._event_loop.set_exception_handler(self.handle_exception)

        self._tornado_loop = IOLoop.current()

        self._hypercorn_config = HypercornConfig.from_mapping({"bind": "0.0.0.0:51234"})  # TODO random port
        self._api = FastAPI(
            title="JuQueue",
            version="0.0.1"
        )
        for router in API_ROUTERS:
            self._api.include_router(router)

        self._backend = Backend(config, self.on_backend_shutdown())
        self._shutdown_event = asyncio.Event()

    def handle_exception(self, _, context):
        msg = context.get("exception", context["message"])
        logger.error(f"Caught exception: {msg}")

    async def _initialize(self):
        await self._backend.initialize()

        # noinspection PyTypeChecker
        await serve(self._api, self._hypercorn_config, shutdown_trigger=self._shutdown_event.wait)

    def start(self):
        self._event_loop.create_task(self._initialize(), name="main")

        try:
            self._tornado_loop.start()
        finally:
            logger.info("JuQueue shut down. Goodbye.")
            self._tornado_loop.close()

    async def on_backend_shutdown(self):
        self._shutdown_event.set()
        self._tornado_loop.call_later(delay=1, callback=self._tornado_loop.stop)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--def-dir", type=Path, default=Path(__file__).parent / "defs")
    parser.add_argument("--work-dir", type=Path, default=Path(__file__).parent / "work")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, type=bool)
    args = parser.parse_args()

    if not args.def_dir.exists():
        raise FileNotFoundError(f"Definitions path {args.def_dir} does not exist.")
    args.work_dir.mkdir(parents=True, exist_ok=True)

    lock = FileLock(LOCK_FILE, timeout=2)
    try:
        with lock:
            # Setting up logging
            log_path = args.work_dir / "logs"
            setup_logger(log_path, args.debug)

            if args.debug:
                logger.debug("Running in debug mode.")

                import faulthandler
                faulthandler.enable()

            # Start the server
            server = Server(Config(**vars(args)))
            server.start()
    except Timeout:
        raise RuntimeError(f"JuQueue is already running, {LOCK_FILE} still locked!")
    finally:
        lock.release()
