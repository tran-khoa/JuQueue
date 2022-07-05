import argparse
import asyncio
import pathlib
import threading
from typing import List, Dict, Any

import Pyro5
from Pyro5.server import expose

from loguru import logger

from juqueue import RunDef
from juqueue.backend.nodes import Executor
from juqueue.backend.nodes.slot import Slot
from juqueue.exceptions import NoSlotsError


class Worker:
    def __init__(self,
                 backend_addr: str,
                 num_slots: int,
                 log_path: pathlib.Path,
                 work_path: pathlib.Path):
        self.backend_addr = backend_addr
        self.num_slots = num_slots
        self.log_path = log_path
        self.work_path = work_path

        logger.add(
            log_path,
            rotation="1 day", compression="gz",
            format="{time} {level} {message}"
        )

        self.slots = [Slot(idx) for idx in range(num_slots)]

        self.aio_loop = None

        def outgoing_thread():
            self.aio_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.aio_loop)

        threading.Thread(target=outgoing_thread).start()

    @expose
    def get_slots_info(self) -> List[Dict[str, Any]]:
        return [slot.info() for slot in self.slots]

    @expose
    def available_slots(self) -> int:
        return sum(1 for slot in self.slots if not slot.is_occupied)

    @expose
    def get_occupancy(self) -> float:
        return 1 - (self.available_slots() / self.num_slots)

    @expose
    def queue_run(self, run_def: RunDef, num_slots: int = 1) -> str:
        try:
            if num_slots != 1:
                raise NotImplementedError()

            key = None
            for slot in self.slots:
                if not slot.is_occupied:
                    executor = Executor.from_def(run_def.executor, work_dir=self.work_path)
                    key = slot.assign(run_def, executor)

                    logger.info(f"Succesfully queued task: {slot}")
                    break

            if not key:
                raise NoSlotsError()

            return key
        except:
            logger.exception("Exception in queue_run")
            raise

    @expose
    def stop_run(self, run_id: str) -> bool:
        try:
            success = False
            for slot in self.slots:
                if slot.occupant == run_id:
                    slot.cancel()
                    success = True
            return success
        except:
            logger.exception("Exception in queue_run")
            raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend-addr", required=True, type=str)
    parser.add_argument("--num-slots", required=True, type=int)
    parser.add_argument("--log-path", required=True, type=pathlib.Path)
    parser.add_argument("--work-path", required=True, type=pathlib.Path)
    args = parser.parse_args()

    worker = Worker(**vars(args))

    daemon = Pyro5.server.Daemon()
    uri = daemon.register(worker)

    daemon.requestLoop()
