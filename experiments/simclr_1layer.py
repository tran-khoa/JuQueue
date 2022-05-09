import itertools
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

from dask_jobqueue import JobQueueCluster, SLURMCluster

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
        return "inactive"

    @property
    def clusters(self) -> Dict[str, Optional[JobQueueCluster]]:
        return {
            "jureca-cpu":
                SLURMCluster(
                    queue="dc-cpu",
                    project="jinm60",
                    cores=128,
                    memory="127G",
                    interface="ib0",
                    log_directory=(self.path / "slurm-logs").as_posix(),
                    processes=1,
                    extra=[
                        "--lifetime", "1h"
                    ]
                ),
            "local": None
        }

    @property
    def num_jobs(self) -> Dict[str, int]:
        return {"jureca-cpu": 1}

    @property
    def runs(self) -> List[Run]:
        runs = []

        base_run = Run(
            uid="_base",
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
                "cleanup_checkpoints": True
            },
            parameter_format="eq",
            cluster="jureca-cpu",
            cmd=["python3", "main.py", "emnist_simclr", "start_pretrain"],
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

                run = base_run.fork(uid=name)
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
                "/p/project/jinm60/users/tran4/biasadapt_git":"/work/biasadapt"
            })
