import logging
import os
import pickle
import sys
import traceback
import warnings
from typing import Callable, Dict

import zmq

from config import Config
from engine.manager import Manager

warnings.simplefilter(action='ignore', category=FutureWarning)


PIDFILE = Config.WORK_DIR / "server.pid"


class Server:

    def __init__(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(Config.SOCKET_ADDRESS)

        self.manager = Manager(Config.ROOT_DIR / "experiments")
        self.manager.load_experiments()

    @property
    def actions(self) -> Dict[str, Callable]:
        return {"quit": self.quit,
                "heartbeat": self.heartbeat,
                "get_experiments": lambda: self.manager.experiment_names,
                "get_runs": self.manager.get_runs,
                "reload": self.manager.load_experiments}

    def heartbeat(self):
        return 1

    def quit(self):
        print("Exiting server...")

        PIDFILE.unlink(missing_ok=True)
        sys.exit(0)

    def loop(self):
        logging.info(f"Server running on {Config.SOCKET_ADDRESS}...")
        while True:
            request = self.socket.recv()
            logging.info(f"REQ: {request}")

            try:
                req_dict = pickle.loads(request)
            except pickle.PickleError:
                logging.error(f"Received invalid request {request}")
                continue

            if not isinstance(req_dict, dict) or "_meth" not in req_dict:
                logging.error(f"Received invalid request {req_dict}")

            meth = req_dict["_meth"]
            del req_dict["_meth"]

            try:
                result = self.actions[meth](**req_dict)
            except Exception as err:
                logging.error(f"Exception {err} caught running {meth} with args {req_dict}")
                traceback.print_exc()

            self.socket.send(pickle.dumps(result))


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
