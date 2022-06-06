from dataclasses import dataclass
from typing import List

from juqueue import ExperimentDef, RunDef, ExecutorDef


@dataclass
class Experiment(ExperimentDef):
    @property
    def name(self) -> str:
        return "helloworld"

    @property
    def runs(self) -> List[RunDef]:
        runs = [
            RunDef(id="helloworld", experiment_name=self.name, cmd=["echo", "Hello world!"], cluster="example_local"),
            RunDef(id="python", experiment_name=self.name,
                   cmd=["python", "-V"],
                   cluster="example_local",
                   executor=ExecutorDef(
                       prepend_script=["module load Python/3.9.6"]
                   )),
        ]

        for delay in (1000, 2000, 3000, 4000, 5000):
            runs.append(
                RunDef(id=f"sleep_{delay}", experiment_name=self.name, cmd=["sleep", delay], cluster="example_local")
            )

        return runs
