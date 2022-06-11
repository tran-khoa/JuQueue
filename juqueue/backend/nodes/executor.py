from __future__ import annotations

import asyncio
import contextlib

import datetime
import os
import shlex
import tempfile
import typing
from pathlib import Path
from typing import Dict, List

import psutil
from loguru import logger
from psutil import NoSuchProcess

from .watchers import child_watcher
from juqueue.definitions import ExecutorDef

if typing.TYPE_CHECKING:
    from juqueue.definitions import RunDef


class Executor(ExecutorDef):

    work_dir: Path

    @classmethod
    def from_def(cls, executor_def: ExecutorDef, work_dir: Path):
        return cls.parse_obj({
            "work_dir": work_dir,
            **executor_def.dict()
        })

    def environment(self, run: RunDef, slots: List[int]) -> Dict[str, str]:
        env = self.env.copy()

        if self.cuda:
            env['CUDA_VISIBLE_DEVICES'] = ",".join(map(str, slots))

        if self.python_search_path:
            env['PYTHONPATH'] = env.get("PYTHONPATH", "") + ":" + ":".join(self.python_search_path)

        env['RUN_ID'] = run.id
        env['EXPERIMENT_ID'] = run.experiment_name

        return env

    def create_script(self, run: RunDef) -> str:
        # Create run script
        script = ["#!/bin/bash"]
        if self.prepend_script:
            script.extend(self.prepend_script)
        if self.venv:
            script.append(f". {str(Path(self.venv) / 'bin' / 'activate')}")
        exec_line = ["exec"] + run.parsed_cmd(work_dir=self.work_dir)
        script.append(shlex.join(exec_line))
        return "\n".join(script)

    def create_virtual_script(self, run: RunDef, slots: List[int]) -> str:
        lines = [f"cd {run.path.contextualize(self).as_posix()}"]
        for key, value in self.environment(run, slots).items():
            lines.append(f"export {key}={value}")
        lines.append(self.create_script(run))
        return "\n".join(lines)

    async def execute(self, run: RunDef, slots: List[int]) -> int:
        script = self.create_script(run)

        env = os.environ.copy()
        env.update(self.environment(run, slots))

        path = run.path.contextualize(self)
        log_path = run.log_path.contextualize(self)

        path.mkdir(parents=True, exist_ok=True)
        log_path.mkdir(parents=True, exist_ok=True)

        with (log_path / "stdout.log").open("at") as stdout, (log_path / "stderr.log").open("at") as stderr:
            stderr.write(f"---------- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----------\n")
            stderr.flush()

            stdout.write(f"---------- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----------\n")
            stdout.write(self.create_virtual_script(run, slots) + "\n")
            stdout.flush()

            with tempfile.NamedTemporaryFile("wt", delete=False) as run_file:
                run_file.write(script)

            os.chmod(run_file.name, 0o700)

            try:
                process = await asyncio.create_subprocess_shell(f"exec {run_file.name}",
                                                                env=env,
                                                                cwd=path,
                                                                stdout=stdout,
                                                                stderr=stderr,
                                                                executable="/bin/bash")
                child_task = asyncio.create_task(child_watcher(process.pid))

                return await process.wait()
            except asyncio.CancelledError:
                logger.debug("Cancelling running process...")

                if process is not None:
                    process.terminate()
                    await asyncio.sleep(1)
            except:
                logger.exception(f"Exception occured while executing {run}!")
                raise
            finally:
                child_task.cancel()

                child_pids = []
                try:
                    child_pids = await child_task
                except:
                    logger.exception("Could not obtain child_pids")

                for pid in child_pids:
                    with contextlib.suppress(NoSuchProcess):
                        p = psutil.Process(pid)
                        p.kill()

                if process is not None:
                    with contextlib.suppress(ProcessLookupError):
                        process.kill()

                os.unlink(run_file.name)
