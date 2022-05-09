import os
import sys
import pickle
import warnings
from typing import Any

import questionary
import tableprint as tp
import zmq
from questionary import Choice

from config import Config

warnings.simplefilter(action='ignore', category=FutureWarning)


class Client:
    def __init__(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(Config.SOCKET_ADDRESS)

        try:
            self.heartbeat()
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
            return pickle.loads(self.socket.recv())
        else:
            raise TimeoutError()

    def heartbeat(self):
        self.__execute("heartbeat")
        print("Server is still running.")

    def list_experiments(self):
        experiments = self.__execute("get_experiments")

        tp.table(headers=["id", "name"],
                 data=list(enumerate(experiments)))

    def list_runs(self):
        experiments = self.__execute("get_experiments")

        experiment_name = questionary.select("Select an experiment",
                                choices=list(experiments) + ["All"]).ask()
        if experiment_name == "All":
            tp.table(headers=["Experiment", "UID", "Status"],
                     data=[(name, run.uid, run.status) for name in
                           experiments
                           for run in self.__execute("get_runs", experiment_name=name)])
        else:
            runs = self.__execute("get_runs", experiment_name=experiment_name)
            tp.table(headers=["UID", "Status"],
                     data=[(run.uid, run.status) for run in
                           runs])

    def stop_server(self):
        confirm = questionary.confirm("Are you sure?")
        if confirm:
            self.__execute_no_response("quit")
            self.quit()

    def not_implemented(self):
        print("Not implemented yet.")

    def reload(self):
        print("Reloading experiments...")
        results = self.__execute("reload")

        for xp, result in results.items():
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
                                              Choice("Heartbeat", self.heartbeat),
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

