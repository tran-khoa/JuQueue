import itertools
from typing import NamedTuple
from typing import Any, List, Set, Tuple


class Parameter(NamedTuple):
    key: str
    value: Any

    # Determines whether changing this parameter would result in a 'different' run
    # Examples: learning_rate (characteristic), max_epochs (not characteristic)
    characteristic: bool = True


class ParameterSet(NamedTuple):
    key: str
    values: List[Any]

    characteristic: bool = True

    @classmethod
    def cartesian(cls, *args) -> List[Tuple[Any]]:
        sets = []
        for s in args:
            if isinstance(s, Parameter):
                sets.append({s})
            elif isinstance(s, ParameterSet):
                sets.append({Parameter(s.key, val, characteristic=s.characteristic) for val in s.values})
        return list(itertools.product(*sets))
