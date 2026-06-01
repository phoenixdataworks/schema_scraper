"""Object dependency extraction and lineage helpers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Optional

from .base.models import Database, Function, ObjectReference, Procedure, View

if TYPE_CHECKING:
    from .base.connection import BaseConnection

# SQL DML patterns for inferring write targets from definition text.
_WRITE_PATTERNS = (
    re.compile(
        r"\bINSERT\s+INTO\s+(?:\[?(?:(\w+)\]?\.)?)?\[?(\w+)\]?",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bUPDATE\s+(?:\[?(?:(\w+)\]?\.)?)?\[?(\w+)\]?",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bDELETE\s+(?:FROM\s+)?(?:\[?(?:(\w+)\]?\.)?)?\[?(\w+)\]?",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bMERGE\s+INTO\s+(?:\[?(?:(\w+)\]?\.)?)?\[?(\w+)\]?",
        re.IGNORECASE,
    ),
)

# Patterns for inferring read targets when catalog metadata is unavailable.
_READ_PATTERNS = (
    re.compile(
        r"\b(?:FROM|JOIN)\s+(?:\[?(?:(\w+)\]?\.)?)?\[?(\w+)\]?",
        re.IGNORECASE,
    ),
)

_MSSQL_TYPE_MAP = {
    "U": ("table", "read"),
    "V": ("view", "read"),
    "P": ("procedure", "execute"),
    "FN": ("function", "execute"),
    "IF": ("function", "execute"),
    "TF": ("function", "execute"),
    "SN": ("synonym", "read"),
}


def infer_write_targets(
    definition: Optional[str],
    default_schema: str,
) -> list[tuple[str, str]]:
    """Extract schema.object pairs written by INSERT/UPDATE/DELETE/MERGE."""
    if not definition:
        return []

    targets: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for pattern in _WRITE_PATTERNS:
        for match in pattern.finditer(definition):
            schema = match.group(1) or default_schema
            name = match.group(2)
            if not name:
                continue
            key = (schema, name)
            if key not in seen:
                seen.add(key)
                targets.append(key)
    return targets


def infer_read_targets(
    definition: Optional[str],
    default_schema: str,
) -> list[tuple[str, str]]:
    """Extract schema.object pairs read via FROM/JOIN when catalog data is missing."""
    if not definition:
        return []

    targets: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for pattern in _READ_PATTERNS:
        for match in pattern.finditer(definition):
            schema = match.group(1) or default_schema
            name = match.group(2)
            if not name:
                continue
            lowered = name.lower()
            if lowered in {"inserted", "deleted", "dual"}:
                continue
            key = (schema, name)
            if key not in seen:
                seen.add(key)
                targets.append(key)
    return targets


def classify_dependencies(
    catalog_refs: list[ObjectReference],
    definition: Optional[str],
    default_schema: str,
) -> tuple[list[ObjectReference], list[ObjectReference], list[ObjectReference]]:
    """Split catalog refs into reads/writes/executes and enrich with definition inference."""
    reads: list[ObjectReference] = []
    writes: list[ObjectReference] = []
    executes: list[ObjectReference] = []
    ref_index: dict[tuple[str, str], ObjectReference] = {}

    for ref in catalog_refs:
        key = (ref.schema_name.lower(), ref.object_name.lower())
        ref_index[key] = ref
        if ref.access == "execute":
            executes.append(ref)
        elif ref.access == "write":
            writes.append(ref)
        else:
            reads.append(ref)

    for schema, name in infer_write_targets(definition, default_schema):
        key = (schema.lower(), name.lower())
        existing = ref_index.get(key)
        if existing and existing.access == "read":
            writes.append(
                ObjectReference(
                    schema_name=existing.schema_name,
                    object_name=existing.object_name,
                    object_type=existing.object_type,
                    access="write",
                    source=existing.source,
                )
            )
        elif not existing:
            writes.append(
                ObjectReference(
                    schema_name=schema,
                    object_name=name,
                    object_type="table",
                    access="write",
                    source="inferred",
                )
            )

    if not catalog_refs and definition:
        for schema, name in infer_read_targets(definition, default_schema):
            reads.append(
                ObjectReference(
                    schema_name=schema,
                    object_name=name,
                    object_type="table",
                    access="read",
                    source="inferred",
                )
            )

    return reads, writes, executes


def sync_view_base_tables(view: View) -> None:
    """Keep legacy base_tables in sync with reads_from."""
    view.base_tables = sorted(
        {
            ref.full_name
            for ref in view.reads_from
            if ref.object_type in ("table", "view") and ref.access == "read"
        }
    )


def get_mssql_dependencies(
    connection: "BaseConnection",
    schema_name: str,
    object_name: str,
    definition: Optional[str],
) -> tuple[list[ObjectReference], list[ObjectReference], list[ObjectReference]]:
    """Query sys.sql_expression_dependencies for an MSSQL object."""
    query = """
        SELECT DISTINCT
            SCHEMA_NAME(ref.schema_id) AS ref_schema,
            ref.name AS ref_name,
            ref.type AS ref_type
        FROM sys.sql_expression_dependencies d
        JOIN sys.objects obj ON d.referencing_id = obj.object_id
        JOIN sys.schemas s ON obj.schema_id = s.schema_id
        JOIN sys.objects ref ON d.referenced_id = ref.object_id
        WHERE s.name = ? AND obj.name = ?
        AND ref.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF', 'SN')
        ORDER BY ref_schema, ref_name
    """
    rows = connection.execute_dict(query, (schema_name, object_name))
    catalog_refs: list[ObjectReference] = []
    for row in rows:
        object_type, access = _MSSQL_TYPE_MAP.get(row["ref_type"], ("unknown", "unknown"))
        catalog_refs.append(
            ObjectReference(
                schema_name=row["ref_schema"],
                object_name=row["ref_name"],
                object_type=object_type,
                access=access,
                source="catalog",
            )
        )
    return classify_dependencies(catalog_refs, definition, schema_name)


def get_postgresql_dependencies(
    connection: "BaseConnection",
    schema_name: str,
    object_name: str,
    object_kind: str,
    definition: Optional[str],
) -> tuple[list[ObjectReference], list[ObjectReference], list[ObjectReference]]:
    """Query PostgreSQL catalogs for object dependencies."""
    catalog_refs: list[ObjectReference] = []

    if object_kind == "view":
        query = """
            SELECT DISTINCT table_schema AS ref_schema, table_name AS ref_name
            FROM information_schema.view_table_usage
            WHERE view_schema = %s AND view_name = %s
            ORDER BY ref_schema, ref_name
        """
        rows = connection.execute_dict(query, (schema_name, object_name))
        for row in rows:
            catalog_refs.append(
                ObjectReference(
                    schema_name=row["ref_schema"],
                    object_name=row["ref_name"],
                    object_type="table",
                    access="read",
                    source="catalog",
                )
            )
    else:
        query = """
            SELECT DISTINCT
                COALESCE(ref_n.nspname, proc_n.nspname) AS ref_schema,
                COALESCE(ref_c.relname, proc_p.proname) AS ref_name,
                CASE
                    WHEN ref_c.relkind = 'r' THEN 'table'
                    WHEN ref_c.relkind IN ('v', 'm') THEN 'view'
                    WHEN proc_p.oid IS NOT NULL THEN
                        CASE proc_p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END
                    ELSE 'unknown'
                END AS object_type
            FROM pg_depend d
            JOIN pg_proc p ON d.objid = p.oid
            JOIN pg_namespace pn ON p.pronamespace = pn.oid
            LEFT JOIN pg_class ref_c ON d.refobjid = ref_c.oid
                AND ref_c.relkind IN ('r', 'v', 'm')
            LEFT JOIN pg_namespace ref_n ON ref_c.relnamespace = ref_n.oid
            LEFT JOIN pg_proc proc_p ON d.refobjid = proc_p.oid
            LEFT JOIN pg_namespace proc_n ON proc_p.pronamespace = proc_n.oid
            WHERE pn.nspname = %s AND p.proname = %s
            AND p.prokind = %s
            AND d.deptype = 'n'
            AND (ref_c.oid IS NOT NULL OR proc_p.oid IS NOT NULL)
            ORDER BY ref_schema, ref_name
        """
        prokind = "p" if object_kind == "procedure" else "f"
        rows = connection.execute_dict(query, (schema_name, object_name, prokind))
        for row in rows:
            object_type = row["object_type"]
            access = "execute" if object_type in ("procedure", "function") else "read"
            catalog_refs.append(
                ObjectReference(
                    schema_name=row["ref_schema"],
                    object_name=row["ref_name"],
                    object_type=object_type,
                    access=access,
                    source="catalog",
                )
            )

    return classify_dependencies(catalog_refs, definition, schema_name)


def get_oracle_dependencies(
    connection: "BaseConnection",
    schema_name: str,
    object_name: str,
    object_kind: str,
    definition: Optional[str],
) -> tuple[list[ObjectReference], list[ObjectReference], list[ObjectReference]]:
    """Query all_dependencies for an Oracle object."""
    type_map = {
        "view": "VIEW",
        "procedure": "PROCEDURE",
        "function": "FUNCTION",
    }
    oracle_type = type_map.get(object_kind, object_kind.upper())

    query = """
        SELECT DISTINCT
            referenced_owner AS ref_schema,
            referenced_name AS ref_name,
            referenced_type AS ref_type
        FROM all_dependencies
        WHERE owner = :1 AND name = :2 AND type = :3
        AND referenced_type IN ('TABLE', 'VIEW', 'SYNONYM', 'PROCEDURE', 'FUNCTION', 'PACKAGE')
        ORDER BY ref_schema, ref_name
    """
    rows = connection.execute_dict(query, (schema_name, object_name, oracle_type))
    catalog_refs: list[ObjectReference] = []
    for row in rows:
        ref_type = row["ref_type"]
        if ref_type in ("PROCEDURE", "PACKAGE"):
            object_type, access = "procedure", "execute"
        elif ref_type == "FUNCTION":
            object_type, access = "function", "execute"
        elif ref_type == "VIEW":
            object_type, access = "view", "read"
        elif ref_type == "SYNONYM":
            object_type, access = "synonym", "read"
        else:
            object_type, access = "table", "read"
        catalog_refs.append(
            ObjectReference(
                schema_name=row["ref_schema"],
                object_name=row["ref_name"],
                object_type=object_type,
                access=access,
                source="catalog",
            )
        )
    return classify_dependencies(catalog_refs, definition, schema_name)


def get_inferred_dependencies(
    definition: Optional[str],
    default_schema: str,
) -> tuple[list[ObjectReference], list[ObjectReference], list[ObjectReference]]:
    """Fallback dependency extraction from SQL definition text only."""
    return classify_dependencies([], definition, default_schema)


def apply_dependencies(
    obj: View | Procedure | Function,
    reads: list[ObjectReference],
    writes: list[ObjectReference],
    executes: list[ObjectReference],
) -> None:
    """Attach dependency lists to a view, procedure, or function."""
    obj.reads_from = reads
    obj.writes_to = writes
    obj.executes = executes
    if isinstance(obj, View):
        sync_view_base_tables(obj)


def build_reverse_dependencies(database: Database) -> None:
    """Populate used_by on tables, views, procedures, and functions."""
    table_map = {(t.schema_name.lower(), t.name.lower()): t for t in database.tables}
    view_map = {(v.schema_name.lower(), v.name.lower()): v for v in database.views}
    proc_map = {(p.schema_name.lower(), p.name.lower()): p for p in database.procedures}
    func_map = {(f.schema_name.lower(), f.name.lower()): f for f in database.functions}

    def _record_usage(
        ref: ObjectReference,
        consumer_schema: str,
        consumer_name: str,
        consumer_type: str,
    ) -> None:
        key = (ref.schema_name.lower(), ref.object_name.lower())
        entry = (consumer_schema, consumer_name, consumer_type)
        if ref.object_type == "table" and key in table_map:
            if entry not in table_map[key].used_by:
                table_map[key].used_by.append(entry)
        elif ref.object_type == "view" and key in view_map:
            if entry not in view_map[key].used_by:
                view_map[key].used_by.append(entry)
        elif ref.object_type == "procedure" and key in proc_map:
            if entry not in proc_map[key].used_by:
                proc_map[key].used_by.append(entry)
        elif ref.object_type == "function" and key in func_map:
            if entry not in func_map[key].used_by:
                func_map[key].used_by.append(entry)

    consumers: list[tuple[View | Procedure | Function, str]] = []
    for view in database.views:
        consumers.append((view, "view"))
    for proc in database.procedures:
        consumers.append((proc, "procedure"))
    for func in database.functions:
        consumers.append((func, "function"))

    for obj, consumer_type in consumers:
        refs = list(obj.reads_from) + list(obj.writes_to)
        if isinstance(obj, (Procedure, Function)):
            refs.extend(obj.executes)
        for ref in refs:
            _record_usage(ref, obj.schema_name, obj.name, consumer_type)
