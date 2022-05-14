import os
import pickle
import sys
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

        tp.table(headers=["ID", "Status", "Last run", "Last heartbeat"],
                 data=[(run.run_id,
                        run.status,
                        run.last_run.isoformat() if run.last_run else "-",
                        run.last_heartbeat.isoformat() if run.last_heartbeat else "-") for run in _runs.result])

    def resume_runs(self):
        states = questionary.checkbox('Resume runs with the following states:',
                                      choices=[Choice('failed', checked=True),
                                               Choice('cancelled', checked=True),
                                               Choice('finished', checked=False)]).ask()

        if not states:
            print("No states selected.")
        else:
            response = self.socket.execute("resume_runs", experiment_name=self.experiment_name, run_id=ALL_RUNS,
                                           states=states)

            if response.success:
                if response.result:
                    print("Resumed the following runs: " + ", ".join(run.run_id for run in response.result))
                else:
                    print("No runs resumed")
            else:
                print(f"An error occured: {response.reason}")

    def cancel_runs(self):
        all_runs = questionary.select("", choices=[Choice('Cancel all runs', True),
                                                   Choice('Cancel runs by selection', False)]).ask()
        if all_runs:
            confirm = questionary.confirm(f"Cancelling ALL runs of experiment {self.experiment_name}! Are you sure?").ask()
            if confirm:
                r = self.socket.execute("cancel_runs", experiment_name=self.experiment_name, run_ids=ALL_RUNS)
            else:
                return
        else:
            _runs = self._get_runs()
            if not _runs.success:
                print(f"Error: {_runs.reason}")
                return

            sel_runs = questionary.checkbox("Select runs to cancel", choices=[run.run_id for run in _runs.result]).ask()
            confirm = questionary.confirm(f"Cancelling the following runs: {', '.join(sel_runs)}! Are you sure?").ask()
            if confirm:
                r = self.socket.execute("cancel_runs", experiment_name=self.experiment_name, run_ids=sel_runs)
            else:
                return

        if r.success:
            if r.result:
                print("Cancelled the following runs: " + ", ".join(run.run_id for run in r.result))
            else:
                print("No runs cancelled")
        else:
            print(f"An error occured: {r.reason}")

    def reset(self):
        confirm = questionary.confirm("This will reset ALL runs and delete all associated files. Continue?").ask()
        if confirm:
            r = self.socket.execute("reset_experiment", experiment_name=self.experiment_name)
            if r.success:
                print(f"Successfully reset experiment {self.experiment_name}!")
            else:
                print(f"An error occured: {r.reason}")

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
                                              Choice("Cancel run", self.cancel_runs),
                                              Choice("Reset experiment", self.reset),
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
        tp.table(headers=["Experiment", "Run-ID", "Status", "Last heartbeat"],
                 data=[(run.experiment_name,
                        run.run_id,
                        run.status,
                        run.last_heartbeat.isoformat() if run.last_heartbeat else "-") for run in runs])

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
