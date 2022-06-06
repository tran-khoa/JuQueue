# JuQueue
Computation and workflow management system for **time-constrained** cluster environments.
This system is aimed at compute clusters, on which users are accounted for the runtime of an entire node, 
rather than the resources requested by a single job (e.g. JURECA).

Work in progress and potentially unstable.

## Concept
- **Runs**
  - Defines the command and its corresponding parameters.
  - Defines an Executor which determines environment variables, virtual environments, etc...
  - Commands should be robust to termination, i.e.  
    - Should resume from previous computation if terminated.
      - If the Node shuts down/fails, the Run will be requeued.
    - Upon failure, must return a non-zero status code. [will not be requeued]
    - Must return status code 0 if completed. [will not be requeued]
- **Experiment**
  - A logical group of Runs.
- **Clusters**
  - Each Cluster (currently `local` and `slurm`) defines a group of nodes.
  - A ClusterManager manages NodeManagers on computation nodes (e.g. via SLURM jobs).
    - Each NodeManager specifies a certain number of Slots and manages the execution of Runs in Python subprocesses.
    - As Runs are (un-)queued from/to the Cluster, or are completed/failed, the number of nodes is rescaled as necessary.
  - For now, the system is aggressive in minimizing the number of nodes, e.g.
    - Assume 4 nodes (each with 4 slots), each executing a single Run
    - Then 3 nodes are cancelled (along with the runs) and rescheduled to the remaining node. 

## Installation and Usage
```bash
git clone https://github.com/tran-khoa/JuQueue juqueue
cd juqueue
pip install -r requirements.txt
cp -r example_defs defs
ln -s [WORK_DIR] work  # optional
./juqueue.py
```
For now, there is no dedicated user interface, but OpenAPI is offered, 
thus JuQueue can be controlled via FastAPI's interactive docs 
available at [localhost:51234/docs](http://localhost:51234/docs).

## Documentation
For now, refer to the examples in [example_defs/](./example_defs) and FastAPI's docs,
available at [localhost:51234/docs](http://localhost:51234/docs)
or [localhost:51234/redoc](http://localhost:51234/redoc).

## TODO (by desc. priority)
- Add per-cluster core-h accounting function
- Adding heartbeats from process itself (by stdout, stderr)
- Adding dependencies
- Implement periodic health check as a safeguard (move away from NodeManagers)
- Adding Resource system
- Adding notifications API
- Specify set of possible clusters for runs, with some prioritization system
- Add web UI

## Specs: Scheduling Priority
- (TODO) Run-specific priority
- (TODO) Experiment-specific priority
- (TODO) Expected runtime
- First scheduled
