"""Dataclasses for all database schema objects."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Column:
    """Represents a table or view column."""

    name: str
    data_type: str
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    default_value: Optional[str] = None
    is_identity: bool = False
    identity_seed: Optional[int] = None
    identity_increment: Optional[int] = None
    is_computed: bool = False
    computed_definition: Optional[str] = None
    collation: Optional[str] = None
    description: Optional[str] = None
    ordinal_position: int = 0

    @property
    def full_type(self) -> str:
        """Get the full type definition including length/precision."""
        # Handle types with length
        if self.data_type in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary",
                              "character varying", "character"):
            if self.max_length == -1:
                return f"{self.data_type}(max)"
            elif self.max_length:
                length = self.max_length
                # nvarchar stores 2 bytes per char in SQL Server
                if self.data_type.startswith("n") and length > 0:
                    length = length // 2
                return f"{self.data_type}({length})"
        # Handle numeric types with precision/scale
        elif self.data_type in ("decimal", "numeric"):
            if self.precision is not None:
                if self.scale is not None and self.scale > 0:
                    return f"{self.data_type}({self.precision},{self.scale})"
                return f"{self.data_type}({self.precision})"
        # Handle time types with fractional seconds
        elif self.data_type in ("datetime2", "datetimeoffset", "time"):
            if self.scale is not None and self.scale != 7:
                return f"{self.data_type}({self.scale})"
        return self.data_type


@dataclass
class PrimaryKey:
    """Represents a primary key constraint."""

    name: str
    columns: list[str]
    is_clustered: bool = True


@dataclass
class ForeignKey:
    """Represents a foreign key constraint."""

    name: str
    columns: list[str]
    referenced_schema: str
    referenced_table: str
    referenced_columns: list[str]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"


@dataclass
class Index:
    """Represents an index."""

    name: str
    columns: list[str]
    is_unique: bool = False
    is_clustered: bool = False
    is_primary_key: bool = False
    included_columns: list[str] = field(default_factory=list)
    filter_definition: Optional[str] = None
    index_type: str = "BTREE"


@dataclass
class CheckConstraint:
    """Represents a check constraint."""

    name: str
    definition: str


@dataclass
class Table:
    """Represents a database table."""

    schema_name: str
    name: str
    columns: list[Column] = field(default_factory=list)
    primary_key: Optional[PrimaryKey] = None
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    indexes: list[Index] = field(default_factory=list)
    check_constraints: list[CheckConstraint] = field(default_factory=list)
    description: Optional[str] = None
    row_count: int = 0
    total_space_kb: int = 0
    used_space_kb: int = 0
    referenced_by: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class View:
    """Represents a database view."""

    schema_name: str
    name: str
    columns: list[Column] = field(default_factory=list)
    definition: Optional[str] = None
    description: Optional[str] = None
    is_materialized: bool = False
    base_tables: list[str] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class Parameter:
    """Represents a stored procedure or function parameter."""

    name: str
    data_type: str
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_output: bool = False
    has_default: bool = False
    default_value: Optional[str] = None
    ordinal_position: int = 0

    @property
    def full_type(self) -> str:
        """Get the full type definition including length/precision."""
        if self.data_type in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary",
                              "character varying", "character"):
            if self.max_length == -1:
                return f"{self.data_type}(max)"
            elif self.max_length:
                length = self.max_length
                if self.data_type.startswith("n") and length > 0:
                    length = length // 2
                return f"{self.data_type}({length})"
        elif self.data_type in ("decimal", "numeric"):
            if self.precision is not None:
                if self.scale is not None and self.scale > 0:
                    return f"{self.data_type}({self.precision},{self.scale})"
                return f"{self.data_type}({self.precision})"
        return self.data_type


@dataclass
class Procedure:
    """Represents a stored procedure."""

    schema_name: str
    name: str
    parameters: list[Parameter] = field(default_factory=list)
    definition: Optional[str] = None
    description: Optional[str] = None
    language: str = "SQL"

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class FunctionColumn:
    """Represents a column returned by a table-valued function."""

    name: str
    data_type: str
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    ordinal_position: int = 0

    @property
    def full_type(self) -> str:
        """Get the full type definition."""
        if self.data_type in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary",
                              "character varying", "character"):
            if self.max_length == -1:
                return f"{self.data_type}(max)"
            elif self.max_length:
                length = self.max_length
                if self.data_type.startswith("n") and length > 0:
                    length = length // 2
                return f"{self.data_type}({length})"
        elif self.data_type in ("decimal", "numeric"):
            if self.precision is not None:
                if self.scale is not None and self.scale > 0:
                    return f"{self.data_type}({self.precision},{self.scale})"
                return f"{self.data_type}({self.precision})"
        return self.data_type


@dataclass
class Function:
    """Represents a user-defined function."""

    schema_name: str
    name: str
    function_type: str  # SCALAR, TABLE, AGGREGATE, WINDOW
    parameters: list[Parameter] = field(default_factory=list)
    return_type: Optional[str] = None  # For scalar functions
    return_columns: list[FunctionColumn] = field(default_factory=list)  # For table-valued
    definition: Optional[str] = None
    description: Optional[str] = None
    language: str = "SQL"

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class Trigger:
    """Represents a DML trigger."""

    schema_name: str
    name: str
    parent_table_schema: str
    parent_table_name: str
    trigger_type: str  # BEFORE, AFTER, INSTEAD OF
    events: list[str] = field(default_factory=list)  # INSERT, UPDATE, DELETE
    definition: Optional[str] = None
    is_disabled: bool = False
    description: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class TypeColumn:
    """Represents a column in a table type."""

    name: str
    data_type: str
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    ordinal_position: int = 0

    @property
    def full_type(self) -> str:
        """Get the full type definition."""
        if self.data_type in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary",
                              "character varying", "character"):
            if self.max_length == -1:
                return f"{self.data_type}(max)"
            elif self.max_length:
                length = self.max_length
                if self.data_type.startswith("n") and length > 0:
                    length = length // 2
                return f"{self.data_type}({length})"
        elif self.data_type in ("decimal", "numeric"):
            if self.precision is not None:
                if self.scale is not None and self.scale > 0:
                    return f"{self.data_type}({self.precision},{self.scale})"
                return f"{self.data_type}({self.precision})"
        return self.data_type


@dataclass
class UserDefinedType:
    """Represents a user-defined type."""

    schema_name: str
    name: str
    type_category: str  # DOMAIN, COMPOSITE, ENUM, RANGE, TABLE_TYPE, ALIAS_TYPE
    base_type: Optional[str] = None  # For alias/domain types
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    columns: list[TypeColumn] = field(default_factory=list)  # For composite/table types
    enum_values: list[str] = field(default_factory=list)  # For enum types
    check_constraint: Optional[str] = None  # For domain types
    description: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class Sequence:
    """Represents a sequence."""

    schema_name: str
    name: str
    data_type: str
    start_value: int
    increment: int
    min_value: int
    max_value: int
    is_cycling: bool = False
    cache_size: Optional[int] = None
    current_value: Optional[int] = None
    description: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class Synonym:
    """Represents a synonym (or alias in some databases)."""

    schema_name: str
    name: str
    base_object_name: str
    target_server: Optional[str] = None
    target_database: Optional[str] = None
    target_schema: Optional[str] = None
    target_object: Optional[str] = None
    description: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


@dataclass
class Schema:
    """Represents a database schema with all its objects."""

    name: str
    tables: list[Table] = field(default_factory=list)
    views: list[View] = field(default_factory=list)
    procedures: list[Procedure] = field(default_factory=list)
    functions: list[Function] = field(default_factory=list)
    triggers: list[Trigger] = field(default_factory=list)
    types: list[UserDefinedType] = field(default_factory=list)
    sequences: list[Sequence] = field(default_factory=list)
    synonyms: list[Synonym] = field(default_factory=list)


@dataclass
class Database:
    """Represents the entire database schema."""

    name: str
    db_type: str = "unknown"
    server: Optional[str] = None
    version: Optional[str] = None
    schemas: list[Schema] = field(default_factory=list)
    tables: list[Table] = field(default_factory=list)
    views: list[View] = field(default_factory=list)
    procedures: list[Procedure] = field(default_factory=list)
    functions: list[Function] = field(default_factory=list)
    triggers: list[Trigger] = field(default_factory=list)
    types: list[UserDefinedType] = field(default_factory=list)
    sequences: list[Sequence] = field(default_factory=list)
    synonyms: list[Synonym] = field(default_factory=list)
