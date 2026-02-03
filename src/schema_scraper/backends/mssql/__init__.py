"""MS SQL Server backend."""

from .connection import MSSQLConnection
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
    """Get all extractors for MSSQL."""
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
    "MSSQLConnection",
    "get_extractors",
    "TableExtractor",
    "ViewExtractor",
    "ProcedureExtractor",
    "FunctionExtractor",
    "TriggerExtractor",
    "TypeExtractor",
    "SequenceExtractor",
    "SynonymExtractor",
]
