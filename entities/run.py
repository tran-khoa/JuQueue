from __future__ import annotations

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

    uid: str
    """Run identifier, unique inside the respective experiment, equal for runs with the same hyperparameters"""

    experiment: BaseExperiment
    cmd: List[str]
    env: Dict[str, str] = field(default_factory=dict)
    status: Literal['finished', 'running', 'paused'] = field(default='paused', init=False)
    last_run: Optional[datetime] = field(default=None, init=False)

    @property
    def states(self) -> Dict[str, Any]:
        return {"status": self.status, "last_run": self.last_run}

    def load_states(self, states: Dict[str, Any]):
        self.status = states["status"]
        self.last_run = states["last_run"]

    @property
    def path(self) -> Path:
        return Config.WORK_DIR / Path(f"{self.experiment.name}/{self.uid}")

    def __repr__(self):
        return f"Run(uid={self.uid}, experiment={self.experiment.name}, status={self.status})"


@dataclass
class SweepRun(Run):
    parameters: List[Union[Parameter, ParameterSet]] = field(default_factory=list)
    parameter_format: Literal['argparse', 'k=v'] = 'argparse'

    def __post_init__(self):
        self.cmd =