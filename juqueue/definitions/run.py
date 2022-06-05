from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Literal, Union

from pydantic import BaseModel, Field

from juqueue.definitions.executor import ExecutorDef
from juqueue.definitions.path import PathDef, PathVars


class PathVar:
    pass


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
        if any(x in kwargs for x in ("id", "is_abstract")):
            raise ValueError("Cannot define id or is_abstract.")
        return RunDef(
            id="@abstract",
            is_abstract=True,
            **kwargs
        )

    def fork(self, run_id: str) -> RunDef:
        copy = self.copy(deep=True, update={
            "id": run_id,
            "is_abstract": False
        })

        return copy

    @property
    def global_id(self) -> str:
        if self.is_abstract:
            return "@abstract_run"
        return f"{self.experiment_name}@{self.id}"

    def parsed_cmd(self, work_dir: Path, **kwargs) -> List[str]:
        cmd = list(self.cmd)
        for key, value in self.parameters.items():
            if isinstance(value, PathDef):
                value = value.contextualize(work_dir=work_dir, **kwargs)

            if self.parameter_format == "argparse":
                cmd.extend([f"--{key}", str(value)])
            elif self.parameter_format == "eq":
                cmd.append(f"{key}={value}")
            else:
                raise NotImplementedError()
        return cmd

    @property
    def path(self) -> PathDef:
        return PathVars.WORK_DIR / self.experiment_name / self.id

    @property
    def log_path(self) -> PathDef:
        return self.path / "logs"

    def __str__(self):
        return f"RunDef(uid={self.id}, experiment={self.experiment_name})"

    @property
    def metadata_path(self) -> PathDef:
        return self.path / "juqueue-run.json"
