from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from juqueue.utils import WORK_DIR
from juqueue.backend.nodes import Executor


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

    # Additional environment variables
    env: Dict[str, str] = field(default_factory=dict)

    # Abstract runs cannot be run, but forked
    is_abstract: bool = False

    # Paths appended to PYTHONPATH
    python_search_path: List[str] = field(default_factory=list)

    # Dictionary of parameters appended to cmd
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Format of the appended parameter (argparse: --key value, eq: k=v)
    parameter_format: Literal['argparse', 'eq'] = 'argparse'

    # Execute this run only if the specified runs are finished
    # TODO implement
    depends_on: Optional[List[RunDef]] = field(default_factory=list)

    # Executor
    executor: Executor = field(default_factory=lambda: Executor(), metadata={"serializable": False})

    def __post_init__(self):
        if not self.is_abstract:
            # TODO move to instance
            self.path.mkdir(parents=True, exist_ok=True)
            self.log_path.mkdir(parents=True, exist_ok=True)

    def fork(self, run_id: str) -> "RunDef":
        return RunDef(
            id=run_id,
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

    def __eq__(self, other):
        if not isinstance(other, RunDef):
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

