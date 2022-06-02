from dataclasses import dataclass, field
from typing import List, Optional, Dict


@dataclass
class ExecutorDef:
    # Additional environment variables
    env: Dict[str, str] = field(default_factory=dict)

    # Paths appended to PYTHONPATH
    python_search_path: List[str] = field(default_factory=list)

    # Path to virtual environment
    venv: Optional[str] = None

    # Bash script lines executed prior to command execution
    prepend_script: Optional[List[str]] = None

    # Sets CUDA_VISIBLE_DEVICES to the assigned slots
    cuda: bool = False
