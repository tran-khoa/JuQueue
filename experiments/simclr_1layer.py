import itertools
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

from dask_jobqueue import JobQueueCluster, SLURMCluster

from entities.executor import Executor, GPUExecutor
from entities.experiment import BaseExperiment
from entities.run import Run


@dataclass
class Experiment(BaseExperiment):
    @property
    def name(self) -> str:
        return "simclr_1layer"

    @property
    def status(self) -> Literal['active', 'inactive']:
        return "active"

    @property
    def clusters(self) -> Dict[str, Optional[JobQueueCluster]]:
        return {
            "jureca-gpu":
                SLURMCluster(
                    name=self.name,
                    job_name=self.name,
                    queue="dc-gpu",
                    project="jinm60",
                    cores=64,
                    memory="500G",
                    interface="ib2",
                    scheduler_options={'interface': 'ib0'},
                    local_directory="/p/scratch/jinm60/tran4/dask",
                    log_directory=(self.path / "slurm-logs").as_posix(),
                    processes=4,
                    walltime="24:00:00",
                    extra=[
                        "--lifetime", "24h",
                        "--resources", "slots=1"
                    ],
                    job_extra=['--gres=gpu:4'],
                    env_extra=["module load CUDA/11.5",
                               "module load Python/3.9.6",
                               "module load cuDNN/8.3.1.22-CUDA-11.5",
                               "module load PyTorch/1.11-CUDA-11.5",
                               "module load torchvision/0.12.0-CUDA-11.5",
                               "module load Pillow-SIMD/9.0.1",
                               "module load SciPy-bundle/2021.10",
                               "module load matplotlib/3.4.3",
                               "module load typing-extensions/3.10.0.0",
                               'echo "$(date) | Starting worker on $(hostname)" >> /p/scratch/jinm60/tran4/dask/workers.log']
                )
        }

    @property
    def num_jobs(self) -> Dict[str, int]:
        return {"jureca-gpu": 10, "local": 0}

    @property
    def runs(self) -> List[Run]:
        runs = []

        base_run = Run(
            run_id="_base",
            python_search_path=["/p/project/jinm60/users/tran4/biasadapt_git"],
            env={"WANDB_MODE": "offline"},
            parameters={
                "data_path": "/p/project/jinm60/users/tran4/datasets",
                "wandb_project": "biasadapt",
                "batch_size": 4096,
                "log_frequency": 1000,
                "num_layers": 1,
                "max_epochs": 50,
                "data_workers": 0,
                "cleanup_checkpoints": True,
                "gpu": True
            },
            parameter_format="eq",
            cluster="jureca-gpu",
            cmd=["python3", "/p/project/jinm60/users/tran4/biasadapt_git/scripts/conv_biasfit/main.py", "emnist_simclr",
                 "start_pretrain"],
            experiment_name=self.name
        )

        # sweep grid
        kernel_sizes = [3, 5, 7, 9]
        conv_channels = [64, 128, 256, 512]
        filters_init_gains = [0.3, 0.6, 1, 2]
        transforms = ["transforms.RandomResizedCrop(28,scale=(0.6,1.0),ratio=(1.,1.)),transforms.RandomRotation(45)",
                      "transforms.RandomResizedCrop(28,scale=(0.6,1.0),ratio=(1.,1.)),transforms.RandomErasing(p=0.5,scale=(0.2,0.33),ratio=(0.3,3.3),value=0.0),transforms.RandomRotation(45)"]

        for k, c, f in itertools.product(kernel_sizes, conv_channels, filters_init_gains):
            name = f"krn{k}_chn{c}_gain{f}"
            for idx, tf in enumerate(transforms):
                if idx == 1:
                    name += "_erasing"

                run = base_run.fork(run_id=name)
                run.parameters.update({
                    "kernel_sizes": f"[{k}]",
                    "conv_channels": f"[{c}]",
                    "filters_init": f"KaimingUniformInitializer(gain={f})",
                    "transforms": tf,
                    "work_dir": (run.path / "output").as_posix()
                })
                run.cmd.extend(["--name", name])
                runs.append(run)

        return runs

    @property
    def executor(self) -> Executor:
        return GPUExecutor(
            gpus_per_node=4,
            venv="/p/project/jinm60/users/tran4/env_biasadapt"
        )
