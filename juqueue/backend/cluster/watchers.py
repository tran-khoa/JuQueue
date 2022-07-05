import asyncio

import psutil


async def child_watcher(pid: int):
    """
    Attempts to track long-running child processes.
    """

    child_processes = set()

    while True:
        try:
            try:
                process = psutil.Process(pid)
            except psutil.NoSuchProcess:
                return child_processes

            child_processes = set()

            for p in process.children():
                child_processes.add(p.pid)

            await asyncio.sleep(30)
        except asyncio.CancelledError:
            # One last attempt to get current children
            try:
                process = psutil.Process(pid)
                children = process.children()
                if children:
                    return [p.pid for p in children]

            except psutil.NoSuchProcess:
                return child_processes

            return child_processes
