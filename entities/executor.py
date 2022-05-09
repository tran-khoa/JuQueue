import shlex
import subprocess
from functools import partial
from pathlib import Path
from typing import Callable, Dict, Optional, Union

from config import Config
from entities.run import Run


class Executor:
    def environment(self, run: Run):
        env = run.env.copy()

        env['RUN_ID'] = run.uid
        env['EXPERIMENT_ID'] = run.experiment_name

        return env

    def execute(self, run: Run) -> int:
        return subprocess.call(run.cmd,
                               env=self.environment(run),
                               cwd=run.path.as_posix())

    def create(self, run: Run) -> Callable:
        return partial(self.execute, run)


class SingularityExecutor(Executor):
    CONTAINER_ZYGOTE_PATH = "/juqueue/zygote.sh"

    def __init__(self, container_path: Union[Path, str], binds: Optional[Dict[Union[str, Path], Union[str, Path]]] = None):
        self.container_path = Path(container_path)
        self.binds = binds or {}
        self.binds[Config.ROOT_DIR / "scripts" / "zygote.sh"] = SingularityExecutor.CONTAINER_ZYGOTE_PATH

    def environment(self, run: Run):
        env = super().environment(run)
        env['ZYGOTE_EXEC'] = shlex.join(run.cmd)
        env['ZYGOTE_DIR'] = run.path.as_posix()
        return env

    def execute(self, run: Run):
        cmd = ["singularity", "run"]
        for src, dst in self.binds:
            if src == "$RUN_PATH":
                src = run.path
            cmd.extend(["--bind", f"{src}:{dst}"])
        cmd.extend([self.container_path.as_posix(), "/bin/bash", SingularityExecutor.CONTAINER_ZYGOTE_PATH])

        return subprocess.call(cmd, env=self.environment(run))
