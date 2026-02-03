"""Oracle backend."""

from .connection import OracleConnection
from .extractors import (
    FunctionExtractor,
    ProcedureExtractor,
    SequenceExtractor,
    SynonymExtractor,
    TableExtractor,
    TriggerExtractor,
    TypeExtractor,
    ViewExtractor,
)


def get_extractors() -> dict:
    """Get all extractors for Oracle."""
    return {
        "tables": TableExtractor,
        "views": ViewExtractor,
        "procedures": ProcedureExtractor,
        "functions": FunctionExtractor,
        "triggers": TriggerExtractor,
        "types": TypeExtractor,
        "sequences": SequenceExtractor,
        "synonyms": SynonymExtractor,
    }


__all__ = [
    "OracleConnection",
    "get_extractors",
]
