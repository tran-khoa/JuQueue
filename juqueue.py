#!/usr/bin/env python

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import nest_asyncio
from loguru import logger
from tornado.ioloop import IOLoop

from juqueue.api import API_ROUTERS
from juqueue.backend.backend import Backend

from filelock import FileLock, Timeout
from fastapi import FastAPI
from hypercorn.asyncio import serve
from hypercorn.asyncio import Config

# Assumes the lock file is accessible over all nodes
LOCK_FILE = Path(__file__).parent / ".lock"


class Server:
    def __init__(self, def_path: Path, work_path: Path, debug: bool = False):
        self._event_loop = asyncio.new_event_loop()
        self._event_loop.set_debug(debug)
        asyncio.set_event_loop(self._event_loop)
        nest_asyncio.apply(self._event_loop)
        self._event_loop.set_exception_handler(self.handle_exception)

        self._tornado_loop = IOLoop.current()

        self._hypercorn_config = Config.from_mapping({"bind": "0.0.0.0:51234"})  # TODO random port
        self._api = FastAPI()
        for router in API_ROUTERS:
            self._api.include_router(router)

        self.def_path = def_path
        self.work_path = work_path
        self.debug = debug

    def handle_exception(self, _, context):
        msg = context.get("exception", context["message"])
        logger.error(f"Caught exception: {msg}")

    async def _initialize(self):
        backend = Backend.create(definitions_path=self.def_path,
                                 work_path=self.work_path,
                                 debug=self.debug)
        await backend.initialize()

        # noinspection PyTypeChecker
        await serve(self._api, self._hypercorn_config)

    def start(self):
        self._event_loop.create_task(self._initialize(), name="main")

        try:
            self._tornado_loop.start()
        finally:
            self._tornado_loop.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--def-path", type=Path, default=Path(__file__).parent / "defs")
    parser.add_argument("--work-path", type=Path, default=Path(__file__).parent / "work")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, type=bool)
    args = parser.parse_args()

    if not args.def_path.exists():
        raise FileNotFoundError(f"Definitions path {args.def_path} does not exist.")
    args.work_path.mkdir(parents=True, exist_ok=True)

    lock = FileLock(LOCK_FILE, timeout=2)
    try:
        with lock:
            # Setting up logging
            log_path = args.work_path / "logs"
            log_path.mkdir(exist_ok=True, parents=True)

            log_level = logging.INFO
            if args.debug:
                log_level = logging.DEBUG

            logging.basicConfig(level=log_level)

            logger.remove()
            logger.add(sys.stderr, level=log_level)
            logger.add((log_path / "server.log").as_posix(),
                       format="{time} {level} {message}", rotation="1 day", compression="gz", level=log_level)

            if args.debug:
                logger.debug("Running in debug mode.")

            # Start the server
            server = Server(**vars(args))
            server.start()
    except Timeout:
        raise RuntimeError(f"JuQueue is already running, {LOCK_FILE} still locked!")
    finally:
        lock.release()
