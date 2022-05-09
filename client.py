import argparse
import os
import sys
import pickle
from typing import Any

import questionary
from questionary import Choice
import tableprint as tp

import zmq

from config import Config


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
                 data=list(enumerate(experiments.keys())))

    def list_runs(self):
        experiments = self.__execute("get_experiments")

        experiment_name = questionary.select("Select an experiment",
                                choices=list(experiments.keys()) + ["All"]).ask()
        if experiment_name == "All":
            tp.table(headers=["Experiment", "UID", "Status", "Command", "Parameters"],
                     data=[(name, run.uid, run.status, str(run.cmd), str(run.parameters)) for name in
                           experiments.keys()
                           for run in self.__execute("get_runs", experiment_name=name)])
        else:
            runs = self.__execute("get_runs", experiment_name=experiment_name)
            tp.table(headers=["UID", "Status", "Command", "Parameters"],
                     data=[(run.uid, run.status, str(run.cmd), str(run.parameters)) for run in
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
        self.__execute("reload")

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

