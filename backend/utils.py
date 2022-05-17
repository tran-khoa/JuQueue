from distributed import Actor

from entities.executor import Executor

ALL_EXPERIMENTS = "@ALL_EXPERIMENTS"
ALL_RUNS = "@ALL_RUNS"


class ExecutorActor(Executor, Actor):
    pass
