from pathlib import Path, PurePosixPath
from typing import Dict


class PathDef(PurePosixPath):

    def __new__(cls, *args):
        new_path = cls._from_parts(args)  # noqa
        if any(part for part in new_path.parts[1:] if part == PathVars.WORK_DIR):
            raise ValueError(f"{PathVars.WORK_DIR} must be the first part of the path.")
        return new_path

    def contextualize(self, context=None, *, _=None, **kwargs) -> Path:
        def get(_key: str):
            try:
                if context is not None:
                    return getattr(context, _key)
                else:
                    return kwargs[_key]
            except (AttributeError, KeyError):
                raise ValueError(f"Could not contextualize {_key}, no value given.")

        parts = list(self.parts)
        if parts[0] == PathVars.WORK_DIR.string:
            assert isinstance(get("work_dir"), Path)
            parts = list(get("work_dir").parts) + parts[1:]

        for part in parts:
            for key, var in PathVars.INLINE_VARS.items():
                if var not in part:
                    continue
                part.replace(var, get(key))

        return Path(*parts)  # noqa

    @classmethod
    def __modify_schema__(cls, field_schema, *args, **kwargs):  # noqa
        field_schema['format'] = 'path'
        return field_schema

    @property
    def string(self):
        return str(Path(*self.parts))

    def __str__(self):
        return self.string

    def as_posix(self) -> str:
        raise ValueError("Must contextualize to Path!")

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        return v

    def __repr__(self) -> str:
        return f"PathDef({self.string})"


class PathVars:
    WORK_DIR: PathDef = PathDef("{{WORK_DIR}}")
    ASSIGNED_WORKER: str = "{{ASSIGNED_WORKER}}"

    INLINE_VARS: Dict[str, str] = {
        "{{assigned_worker}}": ASSIGNED_WORKER
    }
