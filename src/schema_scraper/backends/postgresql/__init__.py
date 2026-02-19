"""PostgreSQL backend."""

from .connection import PostgreSQLConnection
from .extractors import (
    FunctionExtractor,
    ProcedureExtractor,
    SecurityExtractor,
    SequenceExtractor,
    TableExtractor,
    TriggerExtractor,
    TypeExtractor,
    ViewExtractor,
)


def get_extractors() -> dict:
    """Get all extractors for PostgreSQL."""
    return {
        "tables": TableExtractor,
        "views": ViewExtractor,
        "procedures": ProcedureExtractor,
        "functions": FunctionExtractor,
        "triggers": TriggerExtractor,
        "types": TypeExtractor,
        "sequences": SequenceExtractor,
        "synonyms": None,  # PostgreSQL doesn't have synonyms
        "security": SecurityExtractor,
    }


__all__ = [
    "PostgreSQLConnection",
    "get_extractors",
]
