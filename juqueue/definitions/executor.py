from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ExecutorDef:
    venv: Optional[str] = None
    prepend_script: Optional[List[str]] = None
    cuda: bool = False
