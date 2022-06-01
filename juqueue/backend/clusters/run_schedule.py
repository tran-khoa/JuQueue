from ..run_instance import RunInstance
from functools import total_ordering


@total_ordering
class RunSchedule:
    def __init__(self, run_instance: RunInstance):
        self.run_instance = run_instance
        self.valid = True

    @property
    def global_id(self):
        return self.run_instance.global_id

    @property
    def run_def(self):
        return self.run_instance.run_def

    def invalidate(self):
        self.valid = False

    def __eq__(self, other):
        if not isinstance(other, RunSchedule):
            return NotImplemented

        return (self.valid, self.run_instance.created_at) == (other.valid, other.run_instance.created_at)

    def __lt__(self, other):
        if not isinstance(other, RunSchedule):
            return NotImplemented

        return (self.valid, self.run_instance.created_at) == (other.valid, other.run_instance.created_at)
