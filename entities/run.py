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
    status: Literal['finished', 'running', 'paused'] = field(default='paused', init=False)

    #
    last_run: Optional[datetime] = field(default=None, init=False)

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "uid": self.uid,
            "experiment": self.experiment.name,
            "cmd": self.cmd,
            "env": self.env
        }

    @property
    def states(self) -> Dict[str, Any]:
        return {"status": self.status, "last_run": self.last_run}

    @property
    def path(self) -> Path:
        return Config.WORK_DIR / Path(f"{self.experiment.name}/{self.uid}")

    def __repr__(self):
        return f"Run(uid={self.uid}, experiment={self.experiment.name}, status={self.status})"

    def save(self):
        with open(self.path / "juqueue-run.json", 'rt') as f:
            json.dump({"metadata": self.metadata, "states": self.states}, f)


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

    @property
    def metadata(self) -> Dict[str, Any]:
        metadata = super().metadata
        metadata.update({
            "parameters": self.parameters,
            "parameter_format": self.parameter_format
        })
        return metadata

