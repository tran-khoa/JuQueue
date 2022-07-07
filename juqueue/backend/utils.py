from __future__ import annotations

import asyncio
import concurrent.futures
import multiprocessing
from functools import partial
from typing import Generic, TypeVar

from loguru import logger

ALL_EXPERIMENTS = "@ALL_EXPERIMENTS"
ALL_RUNS = "@ALL_RUNS"


def standard_error_handler(_, context):
    if 'exception' in context and context['exception'] is not None:
        logger.opt(exception=context['exception']).error(f"Encountered an unhandled exception "
                                                         f"({type(context['exception'])}: "
                                                         f"{str(context['exception'])}.")
    else:
        logger.error(f"Encountered an exception with error message {context['message']}")


def create_process_task(func, *args, **kwargs):
    async def _process_handler():
        loop = asyncio.get_running_loop()
        executor = concurrent.futures.ProcessPoolExecutor()
        result = await loop.run_in_executor(executor, partial(func, **kwargs), *args)
        executor.shutdown()
        return result
    return asyncio.create_task(_process_handler())


T = TypeVar("T")


class AsyncMultiprocessingQueue(Generic[T]):
    def __init__(self):
        self.queue = multiprocessing.SimpleQueue()

    async def _async_op(self, sync_op, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(sync_op, **kwargs), *args)

    def empty(self) -> bool:
        return self.queue.empty()

    def put(self, item: T):
        self.queue.put(item)

    def get(self) -> T:
        return self.queue.get()

    async def aput(self, item: T):
        await self._async_op(self.put, item)

    async def aget(self) -> T:
        return await self._async_op(self.get)

    def close(self):
        self.queue.close()
