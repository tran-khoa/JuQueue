from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class ExecutorDef(BaseModel):
    # Additional environment variables
    env: Dict[str, str] = Field(default_factory=dict)

    # Paths appended to PYTHONPATH
    python_search_path: List[str] = Field(default_factory=list)

    # Path to virtual environment
    venv: Optional[str] = None

    # Bash script lines executed prior to command execution
    prepend_script: Optional[List[str]] = None

    # Sets CUDA_VISIBLE_DEVICES to the assigned slots
    cuda: bool = False
