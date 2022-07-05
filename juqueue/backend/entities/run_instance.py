from __future__ import annotations

import typing
from pathlib import Path

from loguru import logger

from juqueue.logger import format_record

if typing.TYPE_CHECKING:
    from juqueue import RunDef
    from loguru import Logger


class RunInstance:
    run_def: RunDef

    def __init__(self, run_def: RunDef, work_dir: Path):
        if self.run_def.is_abstract:
            raise ValueError("Cannot instantiate an abstract run definition.")
        self.run_def = run_def
        self.work_dir = work_dir

        self.run_path.mkdir(parents=True, exist_ok=True)
        self.log_path.mkdir(parents=True, exist_ok=True)

        logger.add(self.run_path / "juqueue.log",
                   format=format_record,
                   rotation="1 day", retention="5 days", compression="gz",
                   filter=lambda r: r.get("run_id", None) == self.global_id)

    @property
    def parsed_cmd(self):
        return self.run_def.parsed_cmd(self.work_dir)

    @property
    def run_path(self) -> Path:
        return self.run_def.path.contextualize(
            work_dir=self.work_dir
        )

    @property
    def log_path(self):
        return self.run_def.log_path.contextualize(
            work_dir=self.work_dir
        )

    @property
    def metadata_path(self):
        return self.run_def.metadata_path.contextualize(
            work_dir=self.work_dir
        )

    @property
    def global_id(self):
        return self.run_def.global_id

    @property
    def id(self):
        return self.run_def.id

    @property
    def logger(self) -> Logger:
        return logger.bind(run_id=self.global_id)
