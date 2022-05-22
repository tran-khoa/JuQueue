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

from .simclr_1layer import Experiment as PretrainExperiment


@dataclass
class Experiment(BaseExperiment):
    @property
    def name(self) -> str:
        return "random_eval_1layer"

    @property
    def status(self) -> Literal['active', 'inactive']:
        return "active"

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
                 "WANDB_RUN_GROUP": "random_1layer_eval"},
            parameters={
                "data_path": "/p/project/jinm60/users/tran4/datasets",
                "wandb_project": "biasadapt",
                "batch_size": 16384,
                "log_frequency": 200,
                "num_layers": 1,
                "max_epochs": 50,
                "data_workers": 1,
                "cleanup_checkpoints": True,
                "gpu": True,
                "amp": True
            },
            parameter_format="eq",
            cluster="jureca-gpu",
            cmd=["python3", "/p/project/jinm60/users/tran4/biasadapt_git/scripts/conv_biasfit/main.py",
                 "emnist_simclr_bias_dp",
                 "start"],
            experiment_name=self.name
        )

        # Model parameters
        base_run.parameters["task_bias_init"] = "KaimingUniformInitializer(gain=0.577)"
        base_run.parameters["use_mlp"] = False

        base_run.parameters["conv_channels"] = "[128]"
        base_run.parameters["lr"] = 0.01

        # Sweep
        lrs = [0.01]
        chns = [128]
        ks = [3, 5, 7, 9]

        for k, lr, chn in itertools.product(ks, lrs, chns):
            name = f"krn{k}_chn{chn}_lr{lr}"
            run = base_run.fork(run_id=name)
            run.parameters.update({
                "lr": lr,
                "kernel_sizes": f"[{k}]",
                "conv_channels": f"[{chn}]",
                "work_dir": (run.path / "output").as_posix()
            })
            run.cmd.extend(["--name", name])
            wandb_id = hashlib.sha224(run.global_id.encode("utf8")).hexdigest()[:24]
            run.env["WANDB_RUN_ID"] = wandb_id

            runs.append(run)

        return runs

    @property
    def executor(self) -> Executor:
        return GPUExecutor(
            gpus_per_node=4,
            venv="/p/project/jinm60/users/tran4/env_biasadapt"
        )
