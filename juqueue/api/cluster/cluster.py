from fastapi import APIRouter
from loguru import logger
from typing import Dict, Literal, Optional, List
from pydantic import BaseModel

from juqueue import RunDef
from juqueue.api.utils import SuccessResponse
from juqueue.backend.backend import Backend
from juqueue.exceptions import NodeDeathError, NodeNotReadyError

router = APIRouter(tags=["Cluster"])


class SlotInfo(BaseModel):
    index: int
    occupant: Optional[str]
    run_def: Optional[RunDef]


class NodeInfo(BaseModel):
    status: Literal["queued", "dead", "alive"]
    worker: str
    slots: List[SlotInfo]


class ClusterInfo(BaseModel):
    nodes_requested: int
    nodes: Dict[str, NodeInfo]


@router.get("/clusters",
            response_model=Dict[str, ClusterInfo])
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
                    node_infos[node.index]["status"] = "dead"
                except:
                    logger.exception(f"Could not obtain slots of node {node}.")

        result[cm.cluster_name] = {
            "nodes": node_infos,
            "nodes_requested": cm.num_nodes_requested
        }

    return result


@router.get("/clusters/{cluster_name}/rescale",
            response_model=SuccessResponse)
async def rescale_cluster(cluster_name: str):
    try:
        await Backend.instance().get_cluster_manager(cluster_name).rescale()
    except Exception as ex:
        return SuccessResponse.from_exception(ex)
    else:
        return SuccessResponse.with_success()


@router.get("/clusters/{cluster_name}/sync",
            response_model=SuccessResponse)
async def sync_cluster(cluster_name: str):
    try:
        await Backend.instance().get_cluster_manager(cluster_name).request_sync()
    except Exception as ex:
        return SuccessResponse.from_exception(ex)
    else:
        return SuccessResponse.with_success()
