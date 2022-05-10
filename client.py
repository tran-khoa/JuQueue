import os
import sys
import pickle
import warnings
from collections import Counter
from typing import Any, Dict, List, Set

import questionary
import tableprint as tp
import zmq
from questionary import Choice

from config import Config
from engine.manager import ALL_EXPERIMENTS, ALL_RUNS
from entities.run import Run
from utils import Response

warnings.simplefilter(action='ignore', category=FutureWarning)


class ServerSocket:
    def __init__(self):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(Config.SOCKET_ADDRESS)

    def execute_no_response(self, meth: str, **kwargs):
        req = pickle.dumps({"_meth": meth, **kwargs})
        self._socket.send(req)

    def execute(self, meth: str, **kwargs) -> Any:
        self.execute_no_response(meth, **kwargs)

        if self._socket.poll(timeout=3000):
            # noinspection PyTypeChecker
            return pickle.loads(self._socket.recv())
        else:
            raise TimeoutError()


class ExperimentClient:
    def __init__(self, experiment_name: str, socket: ServerSocket):
        self.experiment_name = experiment_name
        self.socket = socket

    def _get_runs(self) -> Response[List[Run]]:
        return self.socket.execute("get_runs", experiment_name=self.experiment_name)

    def list_runs(self):
        _runs = self._get_runs()
        if not _runs.success:
            print(f"Error: {_runs.reason}")
            return

        tp.table(headers=["UID", "Status", "Last run"],
                 data=[(run.uid, run.status, run.last_run.isoformat()) for run in _runs.result])

    def resume_runs(self):
        confirm = questionary.confirm("This will all resume runs that are cancelled or in error state. Continue?")
        if confirm:
            response = self.socket.execute("resume_runs", experiment_name=self.experiment_name, run_id=ALL_RUNS)

            if response.success:
                if response.result:
                    print("Resumed the following runs: " + ", ".join(run.uid for run in response.result))
                else:
                    print("No runs resumed")
            else:
                print(f"An error occured: {response.reason}")

    def loop(self):
        while True:
            _runs = self._get_runs()
            if not _runs.success:
                print(f"Error: {_runs.reason}")
                break

            counter = Counter((run.status for run in _runs.result))

            questionary.print(f"Selected experiment: {self.experiment_name}", style="bold underline")
            print(f"Runs: {len(_runs.result)} [{', '.join(f'{k}={v}' for k, v in counter.items())}]")

            cmd = questionary.select("Select an action",
                                     choices=[Choice("List runs", self.list_runs),
                                              Choice("Resume run", self.resume_runs),
                                              Choice("Go back", False)]).ask()
            os.system("clear")
            if not cmd:
                break
            else:
                cmd()


class Client:
    def __init__(self):
        self.socket = ServerSocket()

        try:
            self.socket.execute("heartbeat")
        except TimeoutError:
            print("Could not receive heartbeat from server, is it running?")
            print("Exiting.")
            sys.exit(1)

    def experiment_menu(self):
        response: Response[List[str]] = self.socket.execute("get_experiments")
        if not response.success:
            print(f"Could not get experiments: {response.reason}")
            return

        tp.table(headers=["id", "name"],
                 data=list(enumerate(response.result)))
        xp = questionary.select("Select an experiment",
                                 choices=response.result).ask()
        os.system("clear")
        xp_client = ExperimentClient(xp, self.socket)
        xp_client.loop()

    def list_all_runs(self):
        response: Response[List[Run]] = self.socket.execute("get_runs", experiment_name=ALL_EXPERIMENTS)
        if not response.success:
            print("Error:")
            print(response.reason)

        runs = response.result
        tp.table(headers=["Experiment", "UID", "Status"],
                 data=[(run.experiment_name, run.uid, run.status) for run in runs])

    def stop_server(self):
        confirm = questionary.confirm("Are you sure?")
        if confirm:
            self.socket.execute_no_response("quit")
            self.quit()

    def reload(self):
        print("Reloading experiments...")
        response: Response[Dict[str, Dict[str, Set[str]]]] = self.socket.execute("reload")
        if not response.success:
            print("Server reported a problem:")
            print(response.reason)

        for xp, result in response.result.items():
            print(f"Loaded experiment {xp}...")

            if result["new"]:
                print("Added the following runs:")
                for run in result["new"]:
                    print("\t" + run)
            if result["deleted"]:
                print("Removed the following runs:")
                for run in result["deleted"]:
                    print("\t" + run)
            if result["updated"]:
                print("Modified the following runs:")
                for run in result["updated"]:
                    print("\t" + run)

    def quit(self):
        sys.exit(0)

    def loop(self):
        os.system("clear")

        while True:
            cmd = questionary.select("Select an action",
                                     choices=[Choice("Manage experiment", self.experiment_menu),
                                              Choice("List all runs", self.list_all_runs),
                                              Choice("Reload experiments", self.reload),
                                              Choice("Stop server", self.stop_server),
                                              Choice("Quit", self.quit)]).ask()
            os.system("clear")

            try:
                cmd()
            except TimeoutError:
                print("Waiting too long for server response, is the server still running?")


if __name__ == '__main__':
    client = Client()
    client.loop()

