"""Abstract base class for schema extractors."""

import logging
from abc import ABC, abstractmethod
from typing import Any

from .connection import BaseConnection


class BaseExtractor(ABC):
    """Abstract base class for extracting schema metadata."""

    def __init__(self, connection: BaseConnection, config: Any):
        self.connection = connection
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> list[Any]:
        """Extract all objects of this type."""
        pass

    def _should_include_schema(self, schema_name: str) -> bool:
        """Check if a schema should be included based on config."""
        return self.config.should_include_schema(schema_name)
