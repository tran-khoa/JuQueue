import asyncio
import logging
import os
import pickle
import sys
import traceback
import warnings
from typing import Dict, List, Literal, Optional, Set, Tuple, Union

from loguru import logger
import zmq.asyncio

from backend.backend import Backend
from backend.utils import ALL_EXPERIMENTS, ALL_RUNS
from config import Config
from entities.run import Run
from utils import Response

PIDFILE = Config.WORK_DIR / "server.pid"
SERVER_ACTIONS = []


def server_action(action):
    SERVER_ACTIONS.append(action.__name__)

    def f(*args, **kwargs):
        try:
            logger.info(f"Client requested calling {action.__name__}"
                         f"({', '.join(f'{k}={v}' for k, v in kwargs.items())})")
            return action(*args, **kwargs)
        except Exception as ex:
            logger.error("Fatal error", exc_info=True)
            traceback.print_exc()
            return Response(success=False, reason=str(ex))

    return f


class Server:
    def __init__(self):
        context = zmq.asyncio.Context()
        self._socket: zmq.asyncio.Socket = context.socket(zmq.REP)
        self._socket.bind(Config.SOCKET_ADDRESS)

        self._event_loop = asyncio.get_event_loop()
        self._backend = Backend(Config.ROOT_DIR / "experiments", self._event_loop)

    @server_action
    async def get_experiments(self) -> Response[List[str]]:
        return Response(success=True,
                        result=self._backend.experiment_names)

    @server_action
    async def get_runs(self, experiment_name: str) -> Response[List[Run]]:
        if experiment_name != ALL_EXPERIMENTS and not self._backend.has_experiment(experiment_name):
            return Response(success=False, reason=f"Experiment '{experiment_name}' not found.")

        if experiment_name == ALL_EXPERIMENTS:
            experiments = self._backend.experiment_names
        else:
            experiments = [experiment_name]

        runs = [run for ex in experiments for run in self._backend.managers[ex].runs]

        return Response(
            success=True,
            result=runs
        )

    @server_action
    async def resume_runs(self, experiment_name: str,
                          run_ids: Union[str, List[str]],
                          states: Optional[List[Literal["failed", "cancelled", "finished"]]]) -> Response[List[Run]]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reset runs of all experiments at once (yet).")
        if experiment_name not in self._backend.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        if run_ids == ALL_RUNS:
            runs = self._backend.managers[experiment_name].runs
        elif not isinstance(run_ids, list):
            runs = [self._backend.managers[experiment_name].run_by_id(run_ids)]
        else:
            runs = [self._backend.managers[experiment_name].run_by_id(rid) for rid in run_ids]

        resumed_runs = await self._backend.managers[experiment_name].resume_runs(runs, states=states)
        return Response(success=True,
                        result=resumed_runs)

    @server_action
    async def cancel_runs(self, experiment_name: str, run_ids: Union[str, List[str]]) -> Response[List[Run]]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reset runs of all experiments at once (yet).")
        if experiment_name not in self._backend.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        if run_ids == ALL_RUNS:
            runs = self._backend.managers[experiment_name].runs
        else:
            if not isinstance(run_ids, list):
                run_ids = [run_ids]
            runs = [self._backend.managers[experiment_name].run_by_id(run_id) for run_id in run_ids]

        res = await self._backend.managers[experiment_name].cancel_runs(runs)
        return Response(success=True, result=res)

    @server_action
    async def reset_experiment(self, experiment_name: str) -> Response[None]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reset runs of all experiments at once (yet).")
        if experiment_name not in self._backend.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        await self._backend.managers[experiment_name].reset()
        return Response(success=True)

    @server_action
    async def reload_cluster(self, experiment_name: str) -> Response[None]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reload cluster of all experiments at once (yet).")
        if experiment_name not in self._backend.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        await self._backend.managers[experiment_name].init_clusters(force_reload=True)
        return Response(success=True)

    @server_action
    async def rescale_cluster(self, experiment_name: str) -> Response[Dict[str, Tuple[int, int]]]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot rescale clusters of all experiments at once (yet).")
        if experiment_name not in self._backend.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        res = await self._backend.managers[experiment_name].rescale_clusters()
        return Response(success=True, result=res)

    @server_action
    async def reload(self) -> Response[Dict[str, Dict[str, Set[str]]]]:
        results = await self._backend.load_experiments()
        return Response(success=True, result=results)

    @server_action
    async def heartbeat(self) -> Response[None]:
        return Response(success=True)

    @server_action
    async def quit(self):
        print("Stopping server...")
        await self._backend.stop()

        self._event_loop.stop()

        PIDFILE.unlink(missing_ok=True)
        sys.exit(0)

    def start(self):
        # Start backend loop in main thread
        async def _init():
            await self._backend.load_experiments()
            await self._request_loop()

        self._event_loop.create_task(_init(), name="request_loop")

        if os.environ.get("DEBUG", False):
            logger.info("Event loop debugging activated.")
            self._event_loop.set_debug(True)

        try:
            self._event_loop.run_forever()
        finally:
            self._event_loop.close()

    async def _handle_request(self, req) -> Response:
        req_dict = pickle.loads(req)

        if not isinstance(req_dict, dict) or "_meth" not in req_dict:
            raise ValueError("Request is not a dictionary or does not contain a method.")

        meth = req_dict["_meth"]
        del req_dict["_meth"]

        if meth not in SERVER_ACTIONS:
            raise NotImplementedError(f"Method {meth} does not exist.")

        return await getattr(self, meth)(**req_dict)

    async def _request_loop(self):
        logger.info(f"Server running on {Config.SOCKET_ADDRESS}...")
        while True:
            request = await self._socket.recv()

            try:
                response = await self._handle_request(request)
            except Exception as ex:
                logger.error(f"Error {ex} occured during request handling!", exc_info=True)
                response = Response(success=False, reason=str(ex))

            await self._socket.send(pickle.dumps(response))


if __name__ == '__main__':
    warnings.simplefilter(action='ignore', category=FutureWarning)

    Config.WORK_DIR.mkdir(parents=True, exist_ok=True)

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

    log_path = Config.WORK_DIR / "logs"
    log_path.mkdir(exist_ok=True, parents=True)

    logging.basicConfig(level=logging.DEBUG)

    logger.add((log_path / "server.log").as_posix(), format="{time} {level} {message}", rotation="1 day", compression="gz")

    server = Server()
    server.start()
