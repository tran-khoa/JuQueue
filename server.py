import argparse
import logging
import os
import pickle
import sys
import threading
import traceback
import warnings
from typing import Dict, List, Literal, Optional, Set, Tuple, Union

import zmq
from managers.main import Manager
from managers.experiment import ALL_EXPERIMENTS, ALL_RUNS

from config import Config
from entities.run import Run
from utils import Response

warnings.simplefilter(action='ignore', category=FutureWarning)


PIDFILE = Config.WORK_DIR / "server.pid"
SERVER_ACTIONS = []


def server_action(callable):
    SERVER_ACTIONS.append(callable.__name__)

    def f(*args, **kwargs):
        try:
            return callable(*args, **kwargs)
        except Exception as ex:
            logging.exception("Fatal error")
            traceback.print_exc()
            return Response(success=False, reason=str(ex))
    return f


class Server:
    def __init__(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(Config.SOCKET_ADDRESS)

        self.manager = Manager(Config.ROOT_DIR / "experiments")
        self.manager.load_experiments()

    @server_action
    def get_experiments(self) -> Response[List[str]]:
        return Response(success=True,
                        result=self.manager.experiment_names)

    @server_action
    def get_runs(self, experiment_name: str) -> Response[List[Run]]:
        runs = self.manager.get_runs(experiment_name)
        return Response(
            success=True,
            result=runs
        )

    @server_action
    def resume_runs(self, experiment_name: str,
                    run_id: str,
                    states: Optional[List[Literal["failed", "cancelled", "finished"]]]) -> Response[List[Run]]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reset runs of all experiments at once (yet).")
        if experiment_name not in self.manager.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        if run_id == ALL_RUNS:
            runs = self.manager.get_runs(experiment_name)
        else:
            runs = [self.manager.managers[experiment_name].run_by_id(run_id)]

        resumed_runs = self.manager.managers[experiment_name].resume_runs(runs, states=states)
        return Response(success=True,
                        result=resumed_runs)

    @server_action
    def cancel_runs(self, experiment_name: str, run_ids: Union[str, List[str]]) -> Response[List[Run]]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reset runs of all experiments at once (yet).")
        if experiment_name not in self.manager.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        if run_ids == ALL_RUNS:
            runs = self.manager.get_runs(experiment_name)
        else:
            if not isinstance(run_ids, list):
                run_ids = [run_ids]
            runs = [self.manager.managers[experiment_name].run_by_id(run_id) for run_id in run_ids]

        res = self.manager.managers[experiment_name].cancel_run(runs)
        return Response(success=True, result=res)

    @server_action
    def reset_experiment(self, experiment_name: str) -> Response[None]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reset runs of all experiments at once (yet).")
        if experiment_name not in self.manager.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        self.manager.managers[experiment_name].reset()
        return Response(success=True)

    @server_action
    def reload_cluster(self, experiment_name: str) -> Response[None]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reload cluster of all experiments at once (yet).")
        if experiment_name not in self.manager.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        self.manager.managers[experiment_name].init_clusters(force_reload=True)
        return Response(success=True)

    @server_action
    def rescale_cluster(self, experiment_name: str) -> Response[Dict[str, Tuple[int, int]]]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot rescale clusters of all experiments at once (yet).")
        if experiment_name not in self.manager.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        res = self.manager.managers[experiment_name].rescale_clusters()
        return Response(success=True, result=res)

    @server_action
    def reload(self) -> Response[Dict[str, Dict[str, Set[str]]]]:
        results = self.manager.load_experiments()
        return Response(success=True, result=results)

    @server_action
    def heartbeat(self) -> Response[None]:
        return Response(success=True)

    @server_action
    def quit(self):
        print("Exiting server...")
        self.manager.stop()

        PIDFILE.unlink(missing_ok=True)
        sys.exit(0)

    def loop(self):
        logging.info(f"Server running on {Config.SOCKET_ADDRESS}...")
        while True:
            request = self.socket.recv()

            error = None
            req_dict = None

            try:
                # noinspection PyTypeChecker
                req_dict = pickle.loads(request)
            except pickle.PickleError:
                error = f"Received invalid request {request}"

            if not error:
                if not isinstance(req_dict, dict) or "_meth" not in req_dict:
                    error = f"Received invalid request {req_dict}"

            if not error:
                meth = req_dict["_meth"]
                del req_dict["_meth"]

                if meth not in SERVER_ACTIONS:
                    error = f"Unknown action {meth}..."

            if not error:
                response = getattr(self, meth)(**req_dict)
            else:
                response = Response(success=False, reason=error)

            self.socket.send(pickle.dumps(response))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

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

    logging.basicConfig(filename=Config.WORK_DIR / "server.log",
                        filemode="w",
                        level=logging.DEBUG)

    server = Server()
    server.loop()
