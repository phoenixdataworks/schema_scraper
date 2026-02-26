"""Snowflake backend."""

from .connection import SnowflakeConnection
from .extractors import (
    FunctionExtractor,
    ProcedureExtractor,
    SecurityExtractor,
    SequenceExtractor,
    TableExtractor,
    ViewExtractor,
)


def get_extractors() -> dict:
    """Get all extractors for Snowflake."""
    return {
        "tables": TableExtractor,
        "views": ViewExtractor,
        "procedures": ProcedureExtractor,
        "functions": FunctionExtractor,
        "triggers": None,  # Snowflake does not support triggers
        "types": None,  # Snowflake does not support user-defined types
        "sequences": SequenceExtractor,
        "synonyms": None,  # Snowflake does not support synonyms
        "security": SecurityExtractor,
    }


__all__ = [
    "SnowflakeConnection",
    "get_extractors",
    "TableExtractor",
    "ViewExtractor",
    "ProcedureExtractor",
    "FunctionExtractor",
    "SequenceExtractor",
    "SecurityExtractor",
]
