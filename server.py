import logging
import os
import pickle
import sys
import traceback
import warnings
from typing import Callable, Dict, List, Set

import zmq

from config import Config
from engine.manager import ALL_EXPERIMENTS, ALL_RUNS, Manager
from entities.run import Run
from utils import Response

warnings.simplefilter(action='ignore', category=FutureWarning)


PIDFILE = Config.WORK_DIR / "server.pid"


class Server:

    ACTIONS = ["get_experiments",
               "get_runs",
               "reload",
               "resume_runs",
               "heartbeat",
               "quit"]

    def __init__(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(Config.SOCKET_ADDRESS)

        self.manager = Manager(Config.ROOT_DIR / "experiments")
        self.manager.load_experiments()

    def get_experiments(self) -> Response[List[str]]:
        return Response(success=True,
                        result=self.manager.experiment_names)

    def get_runs(self, experiment_name: str) -> Response[List[Run]]:
        runs = self.manager.get_runs(experiment_name)
        if runs is None:
            return Response(
                success=False,
                reason=f"Experiment {experiment_name} not found"
            )
        else:
            return Response(
                success=True,
                result=runs
            )

    def resume_runs(self, experiment_name: str, run_id: str) -> Response[List[Run]]:
        if experiment_name == ALL_EXPERIMENTS:
            return Response(success=False, reason=f"Cannot reset runs of all experiments at once (yet).")
        if experiment_name not in self.manager.experiment_names:
            return Response(success=False, reason=f"Unknown experiment {experiment_name}...")

        if run_id == ALL_RUNS:
            runs = self.manager.get_runs(experiment_name)
        else:
            runs = self.manager.managers[experiment_name].run_by_id(run_id)

        resumed_runs = self.manager.managers[experiment_name].resume_runs(runs)

        return Response(success=True,
                        result=resumed_runs)

    def reload(self) -> Response[Dict[str, Dict[str, Set[str]]]]:
        try:
            results = self.manager.load_experiments()
        except Exception as err:
            return Response(success=False,
                            reason=str(err))

        return Response(success=True, result=results)

    def heartbeat(self) -> Response[None]:
        return Response(success=True)

    def quit(self):
        print("Exiting server...")

        PIDFILE.unlink(missing_ok=True)
        sys.exit(0)

    def loop(self):
        logging.info(f"Server running on {Config.SOCKET_ADDRESS}...")
        while True:
            request = self.socket.recv()

            error = None

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

                if meth not in Server.ACTIONS:
                    error = f"Unknown action {meth}..."

            if not error:
                try:
                    response = getattr(self, meth)(**req_dict)
                except Exception as err:
                    error = f"Exception {err} caught running {meth} with args {req_dict}"
                    traceback.print_exc()
                    response = Response(success=False, reason=error)
            else:
                response = Response(success=False, reason=error)

            if error:
                logging.error(error)

            self.socket.send(pickle.dumps(response))


if __name__ == '__main__':
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
                        filemode="a",
                        level=logging.INFO)
    server = Server()
    server.loop()
