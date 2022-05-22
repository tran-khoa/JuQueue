import hashlib
import itertools
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Literal, Optional

from cluster.base import Cluster
from cluster.slurm import SLURMCluster
from entities.executor import Executor, GPUExecutor
from entities.experiment import BaseExperiment
from entities.run import Run


@dataclass
class Experiment(BaseExperiment):
    @property
    def name(self) -> str:
        return "simclr_1layer_bias1"

    @property
    def status(self) -> Literal['active', 'inactive']:
        return "inactive"

    @cached_property
    def clusters(self) -> Dict[str, Optional[Cluster]]:
        return {
            "jureca-gpu":
                SLURMCluster(
                    name=self.name,
                    job_name=self.name,
                    queue="dc-gpu",
                    project="jinm60",
                    cores=128,
                    memory="500G",
                    interface="ib2",
                    scheduler_options={'interface': 'ib0'},
                    local_directory="/p/scratch/jinm60/tran4/dask",
                    log_directory=(self.path / "slurm-logs").as_posix(),
                    processes=4,
                    walltime="24:00:00",
                    extra=["--lifetime", "24h"],
                    job_extra=['--gres=gpu:4'],
                    env_extra=["module load CUDA/11.5",
                               "module load Python/3.9.6",
                               "module load cuDNN/8.3.1.22-CUDA-11.5",
                               "module load PyTorch/1.11-CUDA-11.5",
                               "module load torchvision/0.12.0-CUDA-11.5",
                               "module load Pillow-SIMD/9.0.1",
                               "module load SciPy-bundle/2021.10",
                               "module load matplotlib/3.4.3",
                               "module load typing-extensions/3.10.0.0"]
                )
        }

    @property
    def num_jobs(self) -> Dict[str, int]:
        return {"jureca-gpu": 1, "local": 0}

    @property
    def runs(self) -> List[Run]:
        runs = []

        base_run = Run(
            run_id="_base",
            is_abstract=True,
            python_search_path=["/p/project/jinm60/users/tran4/biasadapt_git"],
            env={"WANDB_MODE": "offline",
                 "WANDB_CACHE_DIR": "/p/scratch/jinm60/tran4/wandb",
                 "WANDB_RESUME": "auto",
                 "WANDB_GROUP": self.name},
            parameters={
                "data_path": "/p/project/jinm60/users/tran4/datasets",
                "wandb_project": "biasadapt",
                "batch_size": 4096,
                "log_frequency": 1000,
                "num_layers": 1,
                "max_epochs": 30,
                "data_workers": 1,
                "cleanup_checkpoints": True,
                "gpu": True,
                "amp": True,
                "task_subset": [0, 3, 6, 7, 27]
            },
            parameter_format="eq",
            cluster="jureca-gpu",
            cmd=["python3", "/p/project/jinm60/users/tran4/biasadapt_git/scripts/conv_biasfit/main.py", "emnist_finetune_multi",
                 "start_finetune"],
            experiment_name=self.name
        )

        # Sweep grid
        lr = [0.001, 0.0001, 0.00001]
        filters_init_gains = [0.3, 0.6, 1, 2]
        kernel_sizes = [3, 5, 7, 9]
        conv_channels = [64, 128, 256, 512]

        for l, k, c, f in itertools.product(lr, kernel_sizes, conv_channels, filters_init_gains):
            name = f"lr{l}_krn{k}_chn{c}_gain{f}"

            run = base_run.fork(run_id=name)
            run.parameters.update({
                "lr": l,
                "kernel_sizes": f"[{k}]",
                "conv_channels": f"[{c}]",
                "filters_init": f"KaimingUniformInitializer(gain={f})",
                "work_dir": (run.path / "output").as_posix()
            })
            run.cmd.extend(["--name", name])
            wandb_id = hashlib.sha224(name.encode("utf8")).hexdigest()[:24]
            run.env["WANDB_RUN_ID"] = wandb_id

            runs.append(run)

        return runs

    @property
    def executor(self) -> Executor:
        return GPUExecutor(
            gpus_per_node=4,
            venv="/p/project/jinm60/users/tran4/env_biasadapt"
        )
