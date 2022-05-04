import shlex
import subprocess
from pathlib import Path
from typing import Optional, Dict, Union

from config import Config
from entities.run import Run


class Executor:
    def environment(self, run: Run):
        env = run.env.copy()

        env['RUN_ID'] = run.uid
        env['EXPERIMENT_ID'] = run.experiment.name

        return env

    def execute(self, run: Run) -> int:
        return subprocess.call(run.cmd,
                               env=self.environment(run),
                               cwd=run.path.as_posix())


class SingularityExecutor(Executor):
    CONTAINER_ZYGOTE_PATH = "/juqueue/zygote.sh"

    def __init__(self, container_path: Path, binds: Optional[Dict[Union[str, Path], Union[str, Path]]] = None):
        self.container_path = container_path
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
            cmd.extend(["--bind", f"{src}:{dst}"])
        cmd.extend([self.container_path.as_posix(), "/bin/bash", SingularityExecutor.CONTAINER_ZYGOTE_PATH])

        return subprocess.call(cmd, env=self.environment(run))
