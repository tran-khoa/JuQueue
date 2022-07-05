from typing import Literal

from juqueue.definitions.cluster import ClusterDef


class ClusterManager:
    def __init__(self, cluster_name: str):
        self.cluster_name = cluster_name
        self._cluster_def = None

    async def load_cluster_def(self, cluster_def: ClusterDef, force_reload: bool = False) -> \
            Literal["unchanged", "no_update", "updated"]:
        pass
