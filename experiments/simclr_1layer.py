import itertools
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

from dask_jobqueue import JobQueueCluster, SLURMCluster
from dask_jobqueue.local import LocalCluster

from entities.executor import Executor, SingularityExecutor
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
            "jureca-cpu":
                SLURMCluster(
                    queue="dc-cpu-bigmem",
                    project="jinm60",
                    cores=128,
                    memory="1024G",
                    interface="ib0",
                    log_directory=(self.path / "slurm-logs").as_posix(),
                    processes=32,
                    walltime="24:00:00",
                    extra=[
                        "--lifetime", "1h"
                    ]
                ),
            "jureca-gpu":
                SLURMCluster(
                    queue="dc-gpu",
                    project="jinm60",
                    cores=128,
                    memory="512G",
                    interface="ib0",
                    log_directory=(self.path / "slurm-logs").as_posix(),
                    processes=4,
                    walltime="24:00:00",
                    extra=[
                        "--lifetime", "1h"
                    ]
                ),
            "local": LocalCluster(cores=0, memory='0G')
        }

    @property
    def num_jobs(self) -> Dict[str, int]:
        return {"jureca-gpu": 10, "jureca-cpu": 0, "local": 0}

    @property
    def runs(self) -> List[Run]:
        runs = []

        base_run = Run(
            run_id="_base",
            env={"PYTHONPATH": "/work/biasadapt"},
            parameters={
                "data_path": "/datasets",
                "work_dir": "/rundir/outputs",
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
            cmd=["python3", "/work/biasadapt/scripts/conv_biasfit/main.py", "emnist_simclr", "start_pretrain"],
            experiment_name=self.name
        )

        # sweep grid
        kernel_sizes = [3, 5, 7, 9]
        conv_channels = [64, 128, 256, 512]
        filters_init_gains = [0.1, 0.3, 0.6, 1, 2]
        transforms = ["transforms.RandomResizedCrop(28,scale=(0.6,1.0),ratio=(1.,1.)),transforms.RandomRotation(45)",
                      "transforms.RandomResizedCrop(28,scale=(0.6,1.0),ratio=(1.,1.)),transforms.RandomErasing(p=0.5,scale=(0.2,0.33),ratio=(0.3,3.3),value=0),transforms.RandomRotation(45)"]

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
                    "transforms": tf
                })
                runs.append(run)

        return runs

    @property
    def executor(self) -> Executor:
        return SingularityExecutor(
            container_path="/p/project/jinm60/users/tran4/conv_biasfit.sif",
            binds={
                "$RUN_PATH": "/rundir",
                "/p/project/jinm60/users/tran4/datasets": "/datasets",
                "/p/project/jinm60/users/tran4/biasadapt_git": "/work/biasadapt"
            })
