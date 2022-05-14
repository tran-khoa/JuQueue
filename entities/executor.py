import datetime
import logging
import os
import random
import shlex
import stat
import subprocess
import tempfile
import threading
from functools import partial
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

from dask.distributed import Pub, Lock, get_client

from config import Config
from entities.run import Run
import platform


class Executor:
    def __init__(self, venv: Union[Path, str, None] = None, prepend_script: Optional[List[str]] = None):
        self.venv = Path(venv) if venv else None
        self.prepend_script = prepend_script

    def environment(self, run: Run) -> Dict[str, str]:
        env = os.environ.copy()
        env.update(run.env)

        env['RUN_ID'] = run.run_id
        env['EXPERIMENT_ID'] = run.experiment_name

        return env

    def heartbeat(self, run: Run):
        get_client().set_metadata([f"heartbeat", run.experiment_name, run.run_id],
                                  datetime.datetime.now().isoformat())
        threading.Timer(Config.HEARTBEAT_INTERVAL, partial(self.heartbeat, run)).start()

    def create_script(self, run: Run) -> str:
        # Create run script
        script = ["#!/bin/bash"]
        if self.prepend_script:
            script.extend(self.prepend_script)
        if self.venv:
            script.append(f". {(self.venv / 'bin' / 'activate').as_posix()}")
        script.append(shlex.join(run.parsed_cmd))
        return "\n".join(script)

    def execute(self, run: Run) -> int:
        self.heartbeat(run)

        script = self.create_script(run)
        env = self.environment(run)
        path = run.path.as_posix()

        with (run.log_path / "stdout.log").open("at") as stdout, (run.log_path / "stderr.log").open("at") as stderr:
            stderr.write(f"---------- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----------\n")
            stderr.flush()

            stdout.write(f"---------- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----------\n")
            stdout.write(f"cd {path}\n")
            for key, value in env.items():
                stdout.write(f"export {key}={value}\n")
            stdout.write(script)
            stdout.write("\n-----------------------------------------\n")
            stdout.flush()

            with tempfile.NamedTemporaryFile("wt", delete=False) as run_file:
                run_file.write(script)

            os.chmod(run_file.name, 0o700)

            status = subprocess.run([run_file.name],
                                    env=env,
                                    cwd=path,
                                    stdout=stdout,
                                    stderr=stderr,
                                    shell=True,
                                    executable="/bin/bash").returncode

            os.unlink(run_file.name)

        return status

    def create(self, run: Run) -> Callable:
        return partial(self.execute, run)


class GPUExecutor(Executor):
    def __init__(self,
                 gpus_per_node: int,
                 venv: Union[Path, str, None] = None,
                 prepend_script: Optional[List[str]] = None):
        super(GPUExecutor, self).__init__(venv, prepend_script)

        self.gpus_per_node = gpus_per_node
        self.__lock = Lock(f"gpu_lock_{platform.node()}")

    def environment(self, run: Run) -> Dict[str, str]:
        env = super(GPUExecutor, self).environment(run).copy()

        env['CUDA_VISIBLE_DEVICES'] = str(self.next_gpu())

        return env

    def next_gpu(self) -> int:
        gpu_dist_key = f"gpu_dist_{platform.node()}"

        self.__lock.acquire()

        dist = list(get_client().get_metadata(keys=[gpu_dist_key], default=[-1] * self.gpus_per_node))

        worker_pid = os.getpid()
        if os.getpid() in dist:
            selected_gpu = dist.index(worker_pid)
        else:
            selected_gpu = -1
            for gpu, pid in enumerate(dist):
                if pid < 0 or not self._is_pid_active(pid):
                    selected_gpu = gpu
                    break
            if selected_gpu < 0:
                logging.error(f"No free gpu available on {platform.node()}: {dist}")
                selected_gpu = random.randrange(self.gpus_per_node)
                logging.error(f"Assigning random GPU to worker {worker_pid}: {selected_gpu}!")
            dist[selected_gpu] = worker_pid

        get_client().set_metadata([gpu_dist_key], dist)
        self.__lock.release()

        return selected_gpu

    @staticmethod
    def _is_pid_active(pid) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True


class SingularityExecutor(Executor):
    CONTAINER_ZYGOTE_PATH = "/juqueue/zygote.sh"

    def __init__(self, container_path: Union[Path, str],
                 binds: Optional[Dict[Union[str, Path], Union[str, Path]]] = None,
                 singularity_params: Optional[List[str]] = None):
        super(SingularityExecutor, self).__init__()
        self.container_path = Path(container_path)
        self.binds = binds or {}
        self.binds[Config.ROOT_DIR / "scripts" / "zygote.sh"] = SingularityExecutor.CONTAINER_ZYGOTE_PATH
        self.singularity_params = singularity_params or []

    def environment(self, run: Run) -> Dict[str, str]:
        env = super().environment(run)
        env['ZYGOTE_EXEC'] = shlex.join(run.cmd)
        env['ZYGOTE_DIR'] = run.path.as_posix()
        return env

    def execute(self, run: Run) -> int:
        stdout = (run.log_path / "stdout.log").open("at")
        stderr = (run.log_path / "stderr.log").open("at")

        cmd = ["singularity", "run"]
        cmd.extend(self.singularity_params)
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
