from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from config import Config


@dataclass
class Run:
    # Run identifier, unique inside the respective experiment, equal for runs with the same hyperparameters
    run_id: str

    #
    experiment_name: str

    #
    cluster: str

    #
    cmd: List[str]

    #
    env: Dict[str, str] = field(default_factory=dict)

    #
    is_abstract: bool = False

    #
    status: Literal['running', 'pending', 'failed', 'cancelled', 'finished'] = field(default='pending', init=False)

    #
    last_run: Optional[datetime] = field(default=None, init=False)

    #
    last_error: Optional[str] = field(default=None, init=False)

    #
    last_heartbeat: Optional[datetime] = field(default=None, init=False)

    #
    python_search_path: List[str] = field(default_factory=list)

    #
    parameters: Dict[str, str] = field(default_factory=dict)

    #
    parameter_format: Literal['argparse', 'eq'] = 'argparse'

    #
    depends_on: Optional[Run] = field(default=None)

    def __post_init__(self):
        if not self.is_abstract:
            self.path.mkdir(parents=True, exist_ok=True)
            self.log_path.mkdir(parents=True, exist_ok=True)

    def fork(self, run_id: str) -> "Run":
        return Run(
            run_id=run_id,
            experiment_name=self.experiment_name,
            cluster=self.cluster,
            cmd=list(self.cmd),
            env=dict(self.env),
            parameters=dict(self.parameters),
            parameter_format=self.parameter_format,
            python_search_path=self.python_search_path,
            depends_on=self.depends_on
        )

    @property
    def global_id(self) -> str:
        if self.is_abstract:
            return "@abstract_run"
        return f"{self.experiment_name}@{self.run_id}"

    @property
    def parsed_cmd(self) -> List[str]:
        cmd = list(self.cmd)
        for key, value in self.parameters.items():
            if self.parameter_format == "argparse":
                cmd.extend([f"--{key}", str(value)])
            elif self.parameter_format == "eq":
                cmd.append(f"{key}={value}")
            else:
                raise NotImplementedError()
        return cmd

    @property
    def _states(self) -> Dict[str, Any]:
        return {"status": self.status,
                "last_run": self.last_run.isoformat(),
                "last_error": self.last_error}

    def _restore_states(self, states: Dict[str, Any]):
        self.status = states["status"]
        self.last_run = datetime.fromisoformat(states["last_run"])
        self.last_error = states["last_error"]

    @property
    def path(self) -> Path:
        return Config.WORK_DIR / self.experiment_name / self.run_id

    @property
    def log_path(self) -> Path:
        return self.path / "logs"

    def __repr__(self):
        return f"Run(uid={self.run_id}, experiment={self.experiment_name}, status={self.status})"

    @property
    def __metadata_path(self) -> Path:
        return self.path / "juqueue-run.json"

    def save_to_disk(self):
        self.path.mkdir(exist_ok=True, parents=True)
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

        fields = [
            "run_id",
            "experiment_name",
            "cluster",
            "cmd",
            "env",
            "is_abstract",
            "parameters",
            "parameter_format",
            "python_search_path"
        ]

        return all(getattr(self, f) == getattr(other, f) for f in fields)

    def __hash__(self) -> int:
        return hash(self.global_id)
