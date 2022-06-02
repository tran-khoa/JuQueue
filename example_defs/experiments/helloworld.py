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
        return [
            RunDef("helloworld", self.name, cmd=["echo", "Hello world!"], cluster="example_local"),
            RunDef("helloworld2", self.name, cmd=["sleep", "6000"], cluster="example_local"),
            RunDef("python", self.name,
                   cmd=["python", "-V"],
                   cluster="example_local",
                   executor=ExecutorDef(
                       prepend_script=["module load Python/3.9.6"]
                   )),
        ]
