from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Literal, List, Optional, Union
from config import Config
import subprocess

from typing import TYPE_CHECKING

from entities.parameter import Parameter, ParameterSet

if TYPE_CHECKING:
    from experiments import BaseExperiment


@dataclass
class Run:
    # Run identifier, unique inside the respective experiment, equal for runs with the same hyperparameters
    uid: str

    #
    experiment: BaseExperiment

    #
    cmd: List[str]

    #
    env: Dict[str, str] = field(default_factory=dict)

    #
    status: Literal['active', 'failed', 'finished'] = field(default='active', init=False)

    #
    last_run: Optional[datetime] = field(default=None, init=False)

    def __post_init__(self):
        self.path.mkdir(parents=True, exist_ok=True)

    @property
    def states(self) -> Dict[str, Any]:
        return {"status": self.status, "last_run": self.last_run.isoformat()}

    @property
    def path(self) -> Path:
        return Config.WORK_DIR / Path(f"{self.experiment.name}/{self.uid}")

    def __repr__(self):
        return f"Run(uid={self.uid}, experiment={self.experiment.name}, status={self.status})"

    @property
    def __metadata_path(self) -> Path:
        return self.path / "juqueue-run.json"

    def save_to_disk(self):
        with open(self.__metadata_path, 'wt') as f:
            json.dump({"states": self.states}, f)

    def load_from_disk(self) -> bool:
        if not self.__metadata_path.exists():
            return False

        with open(self.__metadata_path, 'rt') as f:
            d = json.load(f)

        self.status = d["states"]["status"]
        self.last_run = datetime.fromisoformat(d["states"]["last_run"])
        return True

    def __eq__(self, other):
        if not isinstance(other, Run):
            return False
        return (self.uid == other.uid) \
               and (self.cmd == other.cmd) \
               and (self.env == other.env)


@dataclass
class SweepRun(Run):
    parameters: List[Parameter] = field(default_factory=list)
    parameter_format: Literal['argparse', 'eq'] = 'argparse'

    def __post_init__(self):
        for param in self.parameters:
            if self.parameter_format == "argparse":
                self.cmd.extend([f"--{param.key}", param.value])
            elif self.parameter_format == "eq":
                self.cmd.append(f"{param.key}={param.value}")
            else:
                raise NotImplementedError()

    def __eq__(self, other):
        return super().__eq__(other) \
               and (self.parameters == other.parameters) \
               and (self.parameter_format == other.parameter_format)


