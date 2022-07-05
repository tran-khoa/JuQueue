import math


# noinspection PyArgumentList
class ScalingPolicy:

    """
    Describes how clusters are scaled, e.g. whether a node that is not fully utilized should be started.
    """
    @classmethod
    def maximize_running(cls, num_tasks: int, tasks_per_job: int) -> int:
        """ Start as many jobs as needed to run all runs in parallel"""
        return int(math.ceil(num_tasks / tasks_per_job))

    @classmethod
    def minimize_jobs(cls, num_tasks: int, tasks_per_job: int) -> int:
        """ Only start jobs that can be fully saturated"""
        return num_tasks // tasks_per_job

    @classmethod
    def min_saturation(cls, num_tasks: int, tasks_per_job: int, min_saturation: float):
        """ Each job is forced to be saturated by at least min_saturation (percent, [0.0-1.0])"""
        n, r = divmod(num_tasks, tasks_per_job)
        n += int(r / tasks_per_job > min_saturation)
        return n
