from typing import Optional

CLUSTER_DEFINITIONS = {}


def register_cluster_def(cluster_type: str):
    def fn(cls):
        CLUSTER_DEFINITIONS[cluster_type] = cls
        return cls
    return fn


from .base import ClusterDef  # noqa
from .local import LocalClusterDef  # noqa
from .slurm import SLURMClusterDef  # noqa


def create_cluster_def(cluster_type: str, **kwargs) -> Optional[ClusterDef]:
    if cluster_type not in CLUSTER_DEFINITIONS:
        return None
    return CLUSTER_DEFINITIONS[cluster_type](**kwargs)


__all__ = [
    ClusterDef,
    LocalClusterDef,
    SLURMClusterDef,
    register_cluster_def,
    create_cluster_def
]
