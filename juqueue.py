import argparse
import asyncio
import logging
import os
import sys

import nest_asyncio
from loguru import logger
from tornado.ioloop import IOLoop

from juqueue.api import API_ROUTERS
from juqueue.backend.backend import Backend
from juqueue.utils import ROOT_DIR, WORK_DIR

from fastapi import FastAPI
from hypercorn.asyncio import serve
from hypercorn.asyncio import Config

PIDFILE = ROOT_DIR / "server.pid"


class Server:
    def __init__(self, debug: bool = False):
        self._event_loop = asyncio.new_event_loop()
        self._event_loop.set_debug(debug)
        asyncio.set_event_loop(self._event_loop)
        nest_asyncio.apply(self._event_loop)
        self._event_loop.set_exception_handler(self.handle_exception)

        self._tornado_loop = IOLoop.current()

        self._hypercorn_config = Config.from_mapping({"bind": "0.0.0.0:51234"})
        self._api = FastAPI()
        for router in API_ROUTERS:
            self._api.include_router(router)

        self.debug = debug

    def handle_exception(self, _, context):
        msg = context.get("exception", context["message"])
        logger.error(f"Caught exception: {msg}")

    async def _initialize(self):
        await Backend.instance(debug=self.debug).initialize()

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
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, type=bool)
    args = parser.parse_args()

    WORK_DIR.mkdir(parents=True, exist_ok=True)

    if PIDFILE.exists():
        with open(PIDFILE, 'r') as f:
            pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
            except OSError:
                pass
            else:
                print(f"Server is already running (pid {pid}), exiting...")
                sys.exit(1)

    with open(PIDFILE, 'w') as f:
        f.write(str(os.getpid()))

    # Setting up logging
    log_path = WORK_DIR / "logs"
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
    server = Server(args.debug)
    server.start()
