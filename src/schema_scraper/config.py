"""Configuration dataclasses for the schema scraper."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .exceptions import ConfigurationError


@dataclass
class ScraperConfig:
    """Configuration for the schema scraper."""

    # Database type
    db_type: str = "mssql"

    # Connection parameters (common)
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # MSSQL specific
    trusted_connection: bool = False
    connection_string: Optional[str] = None
    driver: Optional[str] = None

    # SQLite specific
    database_path: Optional[str] = None

    # Oracle specific
    service_name: Optional[str] = None
    sid: Optional[str] = None

    # Output settings
    output_dir: Path = field(default_factory=lambda: Path("./schema_docs"))

    # Filtering
    include_schemas: list[str] = field(default_factory=list)
    exclude_schemas: list[str] = field(default_factory=list)
    object_types: list[str] = field(
        default_factory=lambda: [
            "tables",
            "views",
            "procedures",
            "functions",
            "triggers",
            "types",
            "sequences",
            "synonyms",
            "security",
        ]
    )

    # Behavior
    dry_run: bool = False
    verbosity: int = 0

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if isinstance(self.output_dir, str):
            self.output_dir = Path(self.output_dir)

        # Set default excluded schemas based on db_type
        if not self.exclude_schemas:
            self.exclude_schemas = self._default_excluded_schemas()

        # Set default port based on db_type
        if self.port is None and self.host:
            self.port = self._default_port()

    def _default_excluded_schemas(self) -> list[str]:
        """Get default excluded schemas for the database type."""
        defaults = {
            "mssql": ["sys", "INFORMATION_SCHEMA", "guest"],
            "postgresql": ["pg_catalog", "information_schema", "pg_toast"],
            "mysql": ["information_schema", "performance_schema", "mysql", "sys"],
            "oracle": ["SYS", "SYSTEM", "OUTLN", "DIP", "ORACLE_OCM", "DBSNMP",
                      "APPQOSSYS", "WMSYS", "EXFSYS", "CTXSYS", "XDB", "ORDDATA",
                      "ORDSYS", "MDSYS", "OLAPSYS", "ANONYMOUS", "FLOWS_FILES"],
            "sqlite": [],
        }
        return defaults.get(self.db_type, [])

    def _default_port(self) -> int:
        """Get default port for the database type."""
        ports = {
            "mssql": 1433,
            "postgresql": 5432,
            "mysql": 3306,
            "oracle": 1521,
        }
        return ports.get(self.db_type, 0)

    def validate(self) -> None:
        """Validate the configuration is complete and consistent."""
        if self.db_type == "sqlite":
            if not self.database_path and not self.database:
                raise ConfigurationError("Database path is required for SQLite")
            return

        if self.db_type == "mssql" and self.connection_string:
            return

        if not self.host:
            raise ConfigurationError("Host is required")
        if not self.database:
            raise ConfigurationError("Database is required")

        if self.db_type == "mssql":
            if not self.trusted_connection and not (self.username and self.password):
                raise ConfigurationError(
                    "Either trusted_connection or username/password is required for MSSQL"
                )
        elif self.db_type == "oracle":
            if not self.service_name and not self.sid:
                raise ConfigurationError("Service name or SID is required for Oracle")
            if not self.username or not self.password:
                raise ConfigurationError("Username and password are required for Oracle")
        else:
            # PostgreSQL, MySQL
            if not self.username:
                raise ConfigurationError("Username is required")

    def should_include_schema(self, schema_name: str) -> bool:
        """Check if a schema should be included based on filters."""
        if self.include_schemas:
            return schema_name in self.include_schemas
        return schema_name not in self.exclude_schemas

    def should_extract(self, object_type: str) -> bool:
        """Check if an object type should be extracted."""
        return "all" in self.object_types or object_type in self.object_types
