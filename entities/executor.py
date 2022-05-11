import shlex
import subprocess
import threading
from functools import partial
from pathlib import Path
from threading import Thread
from typing import Callable, Dict, Optional, Union

from dask.distributed import Pub

from config import Config
from entities.run import Run


class Executor:
    def environment(self, run: Run) -> Dict[str, str]:
        env = run.env.copy()

        env['RUN_ID'] = run.run_id
        env['EXPERIMENT_ID'] = run.experiment_name

        return env

    def __heartbeat(self, run: Run):
        pub = Pub(f'{run.experiment_name}_heartbeat')
        pub.put(f"{run.run_id}")
        self.create_heartbeat_timer(run).start()

    def create_heartbeat_timer(self, run: Run):
        return threading.Timer(Config.HEARTBEAT_INTERVAL, partial(self.__heartbeat, run))

    def execute(self, run: Run) -> int:
        self.create_heartbeat_timer(run).start()

        stdout = (run.log_path / "stdout.log").open("at")
        stderr = (run.log_path / "stderr.log").open("at")
        status = subprocess.run(run.cmd,
                                env=self.environment(run),
                                cwd=run.path.as_posix(),
                                stdout=stdout,
                                stderr=stderr).returncode
        stdout.close()
        stderr.close()
        return status

    def create(self, run: Run) -> Callable:
        return partial(self.execute, run)


class SingularityExecutor(Executor):
    CONTAINER_ZYGOTE_PATH = "/juqueue/zygote.sh"

    def __init__(self, container_path: Union[Path, str],
                 binds: Optional[Dict[Union[str, Path], Union[str, Path]]] = None):
        self.container_path = Path(container_path)
        self.binds = binds or {}
        self.binds[Config.ROOT_DIR / "scripts" / "zygote.sh"] = SingularityExecutor.CONTAINER_ZYGOTE_PATH

    def environment(self, run: Run) -> Dict[str, str]:
        env = super().environment(run)
        env['ZYGOTE_EXEC'] = shlex.join(run.cmd)
        env['ZYGOTE_DIR'] = run.path.as_posix()
        return env

    def execute(self, run: Run) -> int:
        self.create_heartbeat_timer(run).start()

        stdout = (run.log_path / "stdout.log").open("at")
        stderr = (run.log_path / "stderr.log").open("at")

        cmd = ["singularity", "run"]
        for src, dst in self.binds.items():
            if src == "$RUN_PATH":
                src = run.path
            cmd.extend(["--bind", f"{src}:{dst}"])
        cmd.extend([self.container_path.as_posix(), "/bin/bash", SingularityExecutor.CONTAINER_ZYGOTE_PATH])

        result = subprocess.run(cmd,
                                env=self.environment(run),
                                stdout=stdout,
                                stderr=stderr).returncode
        stdout.close()
        stderr.close()
        return result
