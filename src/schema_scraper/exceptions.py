"""Custom exceptions for the schema scraper."""


class SchemaScraperError(Exception):
    """Base exception for all schema scraper errors."""

    pass


class ConnectionError(SchemaScraperError):
    """Error establishing database connection."""

    pass


class ConfigurationError(SchemaScraperError):
    """Error in configuration or parameters."""

    pass


class ExtractionError(SchemaScraperError):
    """Error extracting schema metadata."""

    pass


class GenerationError(SchemaScraperError):
    """Error generating output files."""

    pass


class BackendNotAvailableError(SchemaScraperError):
    """Required backend driver is not installed."""

    pass
