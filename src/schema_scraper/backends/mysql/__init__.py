"""MySQL backend."""

from .connection import MySQLConnection
from .extractors import (
    FunctionExtractor,
    ProcedureExtractor,
    SecurityExtractor,
    TableExtractor,
    TriggerExtractor,
    ViewExtractor,
)


def get_extractors() -> dict:
    """Get all extractors for MySQL."""
    return {
        "tables": TableExtractor,
        "views": ViewExtractor,
        "procedures": ProcedureExtractor,
        "functions": FunctionExtractor,
        "triggers": TriggerExtractor,
        "types": None,  # MySQL doesn't have user-defined types
        "sequences": None,  # MySQL doesn't have sequences (uses AUTO_INCREMENT)
        "synonyms": None,  # MySQL doesn't have synonyms
        "security": SecurityExtractor,
    }


__all__ = [
    "MySQLConnection",
    "get_extractors",
]
