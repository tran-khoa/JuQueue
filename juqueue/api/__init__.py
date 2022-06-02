from .experiments import experiment, run
from .cluster import cluster
from .juqueue import stop_server, reload

API_ROUTERS = [
    experiment.router,
    cluster.router,
    run.router,
    stop_server.router,
    reload.router
]
