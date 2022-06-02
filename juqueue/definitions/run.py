from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from juqueue.utils import WORK_DIR
from juqueue.definitions.executor import ExecutorDef


@dataclass(unsafe_hash=True)
class RunDef:
    # Run identifier, unique inside the respective experiment, equal for runs with the same hyperparameters
    id: str

    # Experiment name
    experiment_name: str

    # Name of cluster to run on
    cluster: str

    # List of commands
    cmd: List[str]

    # Abstract runs cannot be run, but forked
    is_abstract: bool = False

    # Dictionary of parameters appended to cmd
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Format of the appended parameter (argparse: --key value, eq: k=v)
    parameter_format: Literal['argparse', 'eq'] = 'argparse'

    # Execute this run only if the specified runs are finished
    # TODO implement
    depends_on: Optional[List[RunDef]] = field(default_factory=list)

    # Executor
    executor: ExecutorDef = field(default_factory=lambda: ExecutorDef())

    def __post_init__(self):
        if not self.is_abstract:
            # TODO move to instance
            self.path.mkdir(parents=True, exist_ok=True)
            self.log_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def create_abstract(cls, **kwargs) -> RunDef:
        if any(x in kwargs for x in ("id", "is_abstract")):
            raise ValueError("Cannot define id or is_abstract.")
        return RunDef(
            id="@abstract",
            is_abstract=True,
            **kwargs
        )

    def fork(self, run_id: str) -> RunDef:
        kwargs = dataclasses.asdict(self)
        kwargs["id"] = run_id
        kwargs["is_abstract"] = False

        return RunDef(**kwargs)

    @property
    def global_id(self) -> str:
        if self.is_abstract:
            return "@abstract_run"
        return f"{self.experiment_name}@{self.id}"

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
    def path(self) -> Path:
        return WORK_DIR / self.experiment_name / self.id

    @property
    def log_path(self) -> Path:
        return self.path / "logs"

    def __repr__(self):
        return f"RunDef(uid={self.id}, experiment={self.experiment_name})"

    @property
    def metadata_path(self) -> Path:
        return self.path / "juqueue-run.json"
