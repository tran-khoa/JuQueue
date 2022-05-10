from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TYPE_CHECKING

from config import Config

if TYPE_CHECKING:
    from entities.experiment import BaseExperiment


@dataclass
class Run:
    # Run identifier, unique inside the respective experiment, equal for runs with the same hyperparameters
    uid: str

    #
    experiment_name: str

    #
    cluster: str

    #
    cmd: List[str]

    #
    env: Dict[str, str] = field(default_factory=dict)

    #
    status: Literal['pending', 'failed', 'cancelled', 'finished'] = field(default='pending', init=False)

    #
    last_run: Optional[datetime] = field(default=None, init=False)

    #
    last_error: Optional[str] = field(default=None, init=False)

    #
    parameters: Dict[str, str] = field(default_factory=dict)

    #
    parameter_format: Literal['argparse', 'eq'] = 'argparse'

    def __post_init__(self):
        self.path.mkdir(parents=True, exist_ok=True)
        self.log_path.mkdir(parents=True, exist_ok=True)

    def fork(self, uid: str) -> "Run":
        return Run(
            uid=uid,
            experiment_name=self.experiment_name,
            cluster=self.cluster,
            cmd=list(self.cmd),
            env=dict(self.env),
            parameters=dict(self.parameters),
            parameter_format=self.parameter_format
        )

    @property
    def parsed_cmd(self) -> List[str]:
        cmd = list(self.cmd)
        for key, value in self.parameters.items():
            if self.parameter_format == "argparse":
                cmd.extend([f"--{key}", value])
            elif self.parameter_format == "eq":
                cmd.append(f"{key}={value}")
            else:
                raise NotImplementedError()
        return cmd

    @property
    def _states(self) -> Dict[str, Any]:
        return {"status": self.status, "last_run": self.last_run.isoformat(), "last_error": self.last_error}

    def _restore_states(self, states: Dict[str, Any]):
        self.status = states["status"]
        self.last_run = datetime.fromisoformat(states["last_run"])
        self.last_error = states["last_error"]

    @property
    def path(self) -> Path:
        return Config.WORK_DIR / Path(f"{self.experiment_name}/{self.uid}")

    @property
    def log_path(self) -> Path:
        return self.path / "logs"

    def __repr__(self):
        return f"Run(uid={self.uid}, experiment={self.experiment_name}, status={self.status})"

    @property
    def __metadata_path(self) -> Path:
        return self.path / "juqueue-run.json"

    def save_to_disk(self):
        with open(self.__metadata_path, 'wt') as f:
            json.dump(self._states, f)

    def load_from_disk(self) -> bool:
        if not self.__metadata_path.exists():
            return False

        with open(self.__metadata_path, 'rt') as f:
            d = json.load(f)

        self._restore_states(d)
        return True

    def __eq__(self, other):
        if not isinstance(other, Run):
            return False
        return (self.uid == other.uid) \
               and (self.cmd == other.cmd) \
               and (self.env == other.env) \
               and (self.parameters == other.parameters) \
               and (self.parameter_format == other.parameter_format)

    def __hash__(self) -> int:
        return hash(self.experiment_name + self.uid)
