from __future__ import annotations

import asyncio
import dataclasses
import datetime
import os
import shlex
import tempfile
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from juqueue.definitions import ExecutorDef

if typing.TYPE_CHECKING:
    from juqueue.definitions import RunDef


@dataclass
class Executor(ExecutorDef):

    @classmethod
    def from_def(cls, executor_def: ExecutorDef):
        return dataclasses.replace(cls(), **dataclasses.asdict(executor_def))

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
        script.append(shlex.join(run.parsed_cmd))
        return "\n".join(script)

    def create_virtual_script(self, run: RunDef, slots: List[int]) -> str:
        lines = [f"cd {run.path.as_posix()}"]
        for key, value in self.environment(run, slots).items():
            lines.append(f"export {key}={value}")
        lines.append(self.create_script(run))
        return "\n".join(lines)

    async def execute(self, run: RunDef, slots: List[int]) -> int:
        run.path.mkdir(parents=True, exist_ok=True)
        run.log_path.mkdir(parents=True, exist_ok=True)

        script = self.create_script(run)

        env = os.environ.copy()
        env.update(self.environment(run, slots))

        path = run.path.as_posix()

        with (run.log_path / "stdout.log").open("at") as stdout, (run.log_path / "stderr.log").open("at") as stderr:
            stderr.write(f"---------- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----------\n")
            stderr.flush()

            stdout.write(f"---------- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----------\n")
            stdout.write(self.create_virtual_script(run, slots))
            stdout.flush()

            with tempfile.NamedTemporaryFile("wt", delete=False) as run_file:
                run_file.write(script)

            os.chmod(run_file.name, 0o700)

            process = await asyncio.create_subprocess_shell(run_file.name,
                                                            env=env,
                                                            cwd=path,
                                                            stdout=stdout,
                                                            stderr=stderr,
                                                            executable="/bin/bash")
            status = await process.wait()

            os.unlink(run_file.name)

        return status
