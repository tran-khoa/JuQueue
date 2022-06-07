from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Union


@dataclass
class Config:
    def_dir: Path
    work_dir: Path
    port: int
    debug: bool


class HasConfigField(Protocol):
    config: Config


class HasConfigProperty(Protocol):
    @property
    @abstractmethod
    def config(self) -> Config:
        return NotImplemented


HasConfig = Union[HasConfigProperty, HasConfigField]
