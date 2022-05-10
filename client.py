import os
import sys
import pickle
import warnings
from typing import Any, Dict, List, Set

import questionary
import tableprint as tp
import zmq
from questionary import Choice

from config import Config
from engine.manager import ALL_EXPERIMENTS
from entities.run import Run
from utils import Response

warnings.simplefilter(action='ignore', category=FutureWarning)


class ExperimentClient():
    pass


class Client:
    def __init__(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(Config.SOCKET_ADDRESS)

        try:
            self.__heartbeat()
        except TimeoutError:
            print("Could not receive heartbeat from server, is it running?")
            print("Exiting.")
            sys.exit(1)

    def __execute_no_response(self, meth: str, **kwargs):
        req = pickle.dumps({"_meth": meth, **kwargs})
        self.socket.send(req)

    def __execute(self, meth: str, **kwargs) -> Any:
        self.__execute_no_response(meth, **kwargs)

        if self.socket.poll(timeout=3000):
            # noinspection PyTypeChecker
            return pickle.loads(self.socket.recv())
        else:
            raise TimeoutError()

    def __heartbeat(self):
        self.__execute("heartbeat")
        print("Server is still running.")

    def list_experiments(self):
        response: Response[List[str]] = self.__execute("get_experiments")

        tp.table(headers=["id", "name"],
                 data=list(enumerate(response.result)))

    def list_runs(self):
        response: Response[List[str]] = self.__execute("get_experiments")
        experiments = response.result

        experiment_name = questionary.select("Select an experiment",
                                choices=[Choice(xp, xp) for xp in experiments]
                                        + [Choice("All experiments", ALL_EXPERIMENTS)]).ask()

        response: Response[List[Run]] = self.__execute("get_runs", experiment_name=experiment_name).response
        if not response.success:
            print("Error:")
            print(response.reason)

        runs = response.result
        if experiment_name == ALL_EXPERIMENTS:
            tp.table(headers=["Experiment", "UID", "Status"],
                     data=[(run.experiment_name, run.uid, run.status) for run in runs])
        else:
            tp.table(headers=["UID", "Status"],
                     data=[(run.uid, run.status) for run in runs])

    def stop_server(self):
        confirm = questionary.confirm("Are you sure?")
        if confirm:
            self.__execute_no_response("quit")
            self.quit()

    def not_implemented(self):
        print("Not implemented yet.")

    def reload(self):
        print("Reloading experiments...")
        response: Response[Dict[str, Dict[str, Set[str]]]] = self.__execute("reload")
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
                                     choices=[Choice("List experiments", self.list_experiments),
                                              Choice("List runs", self.list_runs),
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

