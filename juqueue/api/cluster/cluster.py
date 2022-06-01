from fastapi import APIRouter
from loguru import logger

from juqueue.backend import Backend
from juqueue.exceptions import NodeDeathError, NodeNotReadyError

router = APIRouter(tags=["Cluster"])


@router.get("/clusters")
async def get_clusters():
    result = {}

    for cm in Backend.instance().cluster_managers.values():
        node_infos = {}

        for node in cm.nodes.values():
            node_infos[node.index] = {"status": node.status,
                                      "worker": node.worker,
                                      "slots": None}
            if node.status == 'alive':
                try:
                    node_infos[node.index]["slots"] = await node.get_slots_info()
                except (NodeDeathError, NodeNotReadyError):
                    pass
                except:
                    logger.exception(f"Could not obtain slots of node {node}.")

        result[cm.cluster_name] = {
            "nodes": node_infos,
            "nodes_requested": cm.num_nodes_requested
        }

    return result
