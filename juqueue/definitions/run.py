from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Literal, Union

from pydantic import BaseModel, Field

from juqueue.definitions.executor import ExecutorDef
from juqueue.definitions.path import PathDef, PathVars


class RunDef(BaseModel):
    """
    Defines the execution of a run.
    """

    id: str = Field(
        description="Uniquely identifies a run inside an experiment. Use global_id outside the scope of an experiment."
    )

    experiment_name: str = Field(
        description="The experiment this run belongs to."
    )

    cluster: str = Field(
        description="The cluster this run should run on, as defined in clusters.yaml."
    )

    cmd: List[str] = Field(
        description="The command to be executed, as a list."
    )

    parameters: Dict[str, Union[str, int, float, bool, PathDef, Path, bytes]] = Field(
        default_factory=dict,
        description="Parameters appended to the specified command. Formatting determined by parameter_format."
    )

    parameter_format: Literal['argparse', 'eq'] = Field(
        default='argparse',
        description="'argparse' formats the parameters as --key value, 'eq' as key=value."
    )

    depends_on: List[RunDef] = Field(
        default_factory=list,
        description="Not implemented yet."
    )

    executor: ExecutorDef = Field(
        default_factory=lambda: ExecutorDef(),
        description="Specifies the execution environment the command is run in. Refer to ExecutorDef."
    )

    check_heartbeat: bool = Field(
        default=False,
        description="Not implemented yet."
    )

    is_abstract: bool = Field(
        default=False,
        description="Abstract runs are created as a template for other runs, but cannot be executed themselves."
    )

    @classmethod
    def create_abstract(cls, **kwargs) -> RunDef:
        """
        Shortcut to creating an abstract run,
        equivalent to RunDef(name="@abstract", is_abstract=True, **kwargs)
        """
        if any(x in kwargs for x in ("id", "is_abstract")):
            raise ValueError("Cannot define id or is_abstract.")
        return RunDef(
            id="@abstract",
            is_abstract=True,
            **kwargs
        )

    def fork(self, run_id: str) -> RunDef:
        """
        Creates a deep copy of the current run definition.
        """
        copy = self.copy(deep=True, update={
            "id": run_id,
            "is_abstract": False
        })

        return copy

    @property
    def global_id(self) -> str:
        """
        Used by the scheduler to identify runs across experiments.
        Should not be modified!
        """
        if self.is_abstract:
            return "@abstract_run"
        return f"{self.experiment_name}@{self.id}"

    def resolve_parameters(self, obj: Any, work_dir: Path, **kwargs):
        """
        Returns a parameter dictionary with resolved path definitions.
        """
        if isinstance(obj, PathDef):
            return obj.contextualize(work_dir=work_dir, **kwargs)
        if isinstance(obj, dict):
            return {k: self.resolve_parameters(v, work_dir, **kwargs) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.resolve_parameters(v, work_dir, **kwargs) for v in obj]
        if isinstance(obj, tuple):
            return tuple(self.resolve_parameters(v, work_dir, **kwargs) for v in obj)
        if isinstance(obj, set):
            return {self.resolve_parameters(v, work_dir, **kwargs) for v in obj}
        return obj

    def parsed_cmd(self, work_dir: Path, **kwargs) -> List[str]:
        """
        Generates the command that will be executed from the definition.
        Resolves PathDefs and appends arguments in the requested format.
        """
        cmd = self.resolve_parameters(deepcopy(self.cmd), work_dir)
        for key, value in self.resolve_parameters(self.parameters, work_dir).items():
            if self.parameter_format == "argparse":
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])
            elif self.parameter_format == "eq":
                cmd.append(f"{key}={value}")
            else:
                raise NotImplementedError()
        return cmd

    @property
    def path(self) -> PathDef:
        """
        Run-specific work directory, will be resolved later by JuQueue.

        Do NOT try to resolve (str(), as_posix()) in the definition.
        """
        return PathVars.WORK_DIR / self.experiment_name / self.id

    @property
    def log_path(self) -> PathDef:
        """
        Run-specific log directory, will be resolved later by JuQueue.

        Do NOT try to resolve (str(), as_posix()) in the definition.
        """
        return self.path / "logs"

    def __str__(self):
        return f"RunDef(uid={self.id}, experiment={self.experiment_name})"

    @property
    def metadata_path(self) -> PathDef:
        """
        Path to metadata file, holding e.g. the current status and the date of first creation.
        """
        return self.path / "juqueue-run.json"
