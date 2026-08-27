"""Cross-object dependency graph for schema documentation.

Builds directed ``reads_from`` / ``used_by`` relationships from:

1. View ``base_tables`` already extracted by a backend
2. Database catalog views (MSSQL, PostgreSQL, Oracle, MySQL) when available
3. A conservative scan of object SQL definitions against the known catalog
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable, Optional

from .base.models import (
    Database,
    Function,
    ObjectRef,
    Procedure,
    Relationship,
    Table,
    View,
)

logger = logging.getLogger(__name__)

OBJECT_TYPE_DIR = {
    "table": "tables",
    "view": "views",
    "procedure": "procedures",
    "function": "functions",
    "trigger": "triggers",
    "type": "types",
    "sequence": "sequences",
    "synonym": "synonyms",
}

_TYPE_ALIASES = {
    "u": "table",
    "user_table": "table",
    "base table": "table",
    "table": "table",
    "r": "table",
    "foreign_table": "table",
    "v": "view",
    "view": "view",
    "m": "view",
    "materialized view": "view",
    "materialized_view": "view",
    "p": "procedure",
    "sql_stored_procedure": "procedure",
    "stored_procedure": "procedure",
    "procedure": "procedure",
    "fn": "function",
    "if": "function",
    "tf": "function",
    "af": "function",
    "fs": "function",
    "ft": "function",
    "sql_scalar_function": "function",
    "sql_inline_table_valued_function": "function",
    "sql_table_valued_function": "function",
    "function": "function",
    "sql_trigger": "trigger",
    "tr": "trigger",
    "trigger": "trigger",
    "sn": "synonym",
    "synonym": "synonym",
    "so": "sequence",
    "sequence_object": "sequence",
    "s": "sequence",
    "sequence": "sequence",
    "tt": "type",
    "type": "type",
    "type_table": "type",
}

_SQL_NOISE_RE = re.compile(
    r"""
    --[^\n]*                  # line comments
    |/\*.*?\*/                # block comments
    |N?'(?:''|[^'])*'         # quoted strings (optional N prefix)
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

Documentable = Table | View | Procedure | Function


def normalize_object_type(raw: Optional[str]) -> str:
    """Normalize a catalog type name to a documentation object type."""
    if not raw:
        return "unknown"
    key = raw.strip().lower().replace("-", " ").replace(" ", "_")
    key = key.replace("__", "_")
    if key in _TYPE_ALIASES:
        return _TYPE_ALIASES[key]
    # Second pass: collapse "SQL_STORED_PROCEDURE" style already handled;
    # try without underscores for values like "BASE TABLE".
    spaced = raw.strip().lower().replace("_", " ")
    return _TYPE_ALIASES.get(spaced, raw.strip().lower())


def object_doc_dir(object_type: str) -> Optional[str]:
    """Return the markdown subdirectory for an object type, if documented."""
    return OBJECT_TYPE_DIR.get(normalize_object_type(object_type))


def split_qualified_name(value: str, default_schema: str = "") -> tuple[str, str]:
    """Split ``schema.name`` (or a bare name) into schema and object name."""
    text = (value or "").strip().strip("[]").strip("`").strip('"')
    if not text:
        return default_schema, ""
    # Handle [schema].[name] / "schema"."name" / `schema`.`name`
    parts = re.split(r"\s*\.\s*", text, maxsplit=1)
    if len(parts) == 1:
        return default_schema, _strip_identifier(parts[0])
    return _strip_identifier(parts[0]), _strip_identifier(parts[1])


def _strip_identifier(value: str) -> str:
    return value.strip().strip("[]").strip("`").strip('"')


def documentable_objects(database: Database) -> list[Documentable]:
    """Return extracted objects that can participate in the dependency graph."""
    objects: list[Documentable] = []
    objects.extend(database.tables)
    objects.extend(database.views)
    objects.extend(database.procedures)
    objects.extend(database.functions)
    return objects


def _object_type_for(obj: Documentable) -> str:
    if isinstance(obj, Table):
        return "table"
    if isinstance(obj, View):
        return "view"
    if isinstance(obj, Procedure):
        return "procedure"
    if isinstance(obj, Function):
        return "function"
    return "unknown"


def _ref_for(obj: Documentable) -> ObjectRef:
    return ObjectRef(obj.schema_name, obj.name, _object_type_for(obj))


def _key(schema_name: str, name: str) -> tuple[str, str]:
    return (schema_name or "").lower(), (name or "").lower()


def catalog_index(database: Database) -> dict[tuple[str, str], ObjectRef]:
    """Map ``(schema, name)`` (lowercased) to the extracted object reference."""
    index: dict[tuple[str, str], ObjectRef] = {}
    for obj in documentable_objects(database):
        index[_key(obj.schema_name, obj.name)] = _ref_for(obj)
    return index


def _row_get(row: dict[str, Any], *names: str) -> Any:
    """Read a row value, ignoring driver-specific column-name casing."""
    lower_map = {str(k).lower(): v for k, v in row.items()}
    for name in names:
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None


def relationships_from_view_base_tables(database: Database) -> list[Relationship]:
    """Turn already-extracted ``View.base_tables`` into directed edges."""
    relationships: list[Relationship] = []
    index = catalog_index(database)
    for view in database.views:
        for raw in view.base_tables:
            schema, name = split_qualified_name(raw, default_schema=view.schema_name)
            if not name:
                continue
            target = index.get(_key(schema, name)) or ObjectRef(
                schema, name, "table"
            )
            relationships.append(
                Relationship(
                    from_schema=view.schema_name,
                    from_name=view.name,
                    from_type="view",
                    to_schema=target.schema_name,
                    to_name=target.name,
                    to_type=target.object_type,
                )
            )
    return relationships


def extract_catalog_relationships(
    connection: Any,
    database: Database,
    config: Any,
) -> list[Relationship]:
    """Query backend catalog views for SQL expression dependencies."""
    extractors = {
        "mssql": _extract_mssql,
        "postgresql": _extract_postgresql,
        "oracle": _extract_oracle,
        "mysql": _extract_mysql,
    }
    extractor = extractors.get(database.db_type)
    if extractor is None:
        return []
    try:
        rows = extractor(connection)
    except Exception as exc:
        logger.warning(
            "Catalog dependency query failed for %s: %s",
            database.db_type,
            exc,
        )
        return []

    index = catalog_index(database)
    relationships: list[Relationship] = []
    for row in rows:
        from_schema = _row_get(row, "from_schema") or ""
        from_name = _row_get(row, "from_name") or ""
        to_schema = _row_get(row, "to_schema") or ""
        to_name = _row_get(row, "to_name") or ""
        if not from_name or not to_name:
            continue
        if config and hasattr(config, "should_include_schema"):
            if not config.should_include_schema(from_schema):
                continue
        from_type = normalize_object_type(_row_get(row, "from_type"))
        to_type = normalize_object_type(_row_get(row, "to_type"))
        source = index.get(_key(from_schema, from_name)) or ObjectRef(
            from_schema, from_name, from_type
        )
        target = index.get(_key(to_schema, to_name)) or ObjectRef(
            to_schema, to_name, to_type if to_type != "unknown" else "table"
        )
        relationships.append(
            Relationship(
                from_schema=source.schema_name,
                from_name=source.name,
                from_type=source.object_type,
                to_schema=target.schema_name,
                to_name=target.name,
                to_type=target.object_type,
            )
        )
    return relationships


def _extract_mssql(connection: Any) -> list[dict[str, Any]]:
    query = """
        SELECT
            OBJECT_SCHEMA_NAME(d.referencing_id) AS from_schema,
            OBJECT_NAME(d.referencing_id) AS from_name,
            o1.type AS from_type,
            COALESCE(
                d.referenced_schema_name,
                OBJECT_SCHEMA_NAME(d.referenced_id),
                ''
            ) AS to_schema,
            d.referenced_entity_name AS to_name,
            COALESCE(o2.type, '') AS to_type
        FROM sys.sql_expression_dependencies d
        INNER JOIN sys.objects o1 ON d.referencing_id = o1.object_id
        LEFT JOIN sys.objects o2 ON d.referenced_id = o2.object_id
        WHERE d.referenced_entity_name IS NOT NULL
          AND o1.is_ms_shipped = 0
    """
    return connection.execute_dict(query)


def _extract_postgresql(connection: Any) -> list[dict[str, Any]]:
    query = """
        SELECT
            view_schema AS from_schema,
            view_name AS from_name,
            'view' AS from_type,
            table_schema AS to_schema,
            table_name AS to_name,
            'table' AS to_type
        FROM information_schema.view_table_usage
    """
    rows = list(connection.execute_dict(query))
    try:
        routine_rows = connection.execute_dict(
            """
            SELECT
                specific_schema AS from_schema,
                routine_name AS from_name,
                routine_type AS from_type,
                table_schema AS to_schema,
                table_name AS to_name,
                'table' AS to_type
            FROM information_schema.routine_table_usage
            """
        )
        rows.extend(routine_rows)
    except Exception:
        logger.debug("information_schema.routine_table_usage is not available")
    return rows


def _extract_oracle(connection: Any) -> list[dict[str, Any]]:
    query = """
        SELECT
            owner AS from_schema,
            name AS from_name,
            type AS from_type,
            referenced_owner AS to_schema,
            referenced_name AS to_name,
            referenced_type AS to_type
        FROM all_dependencies
        WHERE type IN (
            'TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION', 'TRIGGER', 'SYNONYM'
        )
          AND referenced_type IN (
            'TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION', 'TRIGGER',
            'SYNONYM', 'SEQUENCE', 'MATERIALIZED VIEW'
          )
          AND owner NOT IN (
            'SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM', 'DBSNMP',
            'APPQOSSYS', 'WMSYS', 'XDB', 'ORDSYS', 'MDSYS', 'CTXSYS'
          )
    """
    return connection.execute_dict(query)


def _extract_mysql(connection: Any) -> list[dict[str, Any]]:
    query = """
        SELECT
            VIEW_SCHEMA AS from_schema,
            VIEW_NAME AS from_name,
            'view' AS from_type,
            TABLE_SCHEMA AS to_schema,
            TABLE_NAME AS to_name,
            'table' AS to_type
        FROM information_schema.VIEW_TABLE_USAGE
    """
    return connection.execute_dict(query)


def strip_sql_noise(sql: str) -> str:
    """Remove comments and string literals so name scans stay conservative."""
    return _SQL_NOISE_RE.sub(" ", sql or "")


def _identifier_regex(schema_name: str, name: str) -> re.Pattern[str]:
    schema = re.escape(schema_name)
    ident = re.escape(name)
    body = rf"""
        (?:
            (?:{schema}|\[{schema}\]|"{schema}"|`{schema}`)
            \s*\.\s*
        )?
        (?:{ident}|\[{ident}\]|"{ident}"|`{ident}`)
    """
    return re.compile(rf"(?<![\w]){body}(?![\w])", re.IGNORECASE | re.VERBOSE)


def relationships_from_sql_definitions(database: Database) -> list[Relationship]:
    """Find known catalog objects mentioned in extracted SQL definitions."""
    index = catalog_index(database)
    by_name: dict[str, list[ObjectRef]] = {}
    for ref in index.values():
        by_name.setdefault(ref.name.lower(), []).append(ref)

    compiled = {
        key: _identifier_regex(ref.schema_name, ref.name) for key, ref in index.items()
    }
    relationships: list[Relationship] = []

    for obj in documentable_objects(database):
        sql = getattr(obj, "definition", None)
        if not sql:
            continue
        text = strip_sql_noise(sql)
        source = _ref_for(obj)
        seen: set[tuple[str, str]] = set()

        for key, ref in index.items():
            if key == _key(obj.schema_name, obj.name):
                continue
            pattern = compiled[key]
            if not pattern.search(text):
                continue
            # Unqualified matches are accepted only when unique or same-schema.
            qualified = bool(
                re.search(
                    rf"(?<![\w]){re.escape(ref.schema_name)}\s*\.",
                    text,
                    re.IGNORECASE,
                )
            )
            if not qualified:
                candidates = by_name.get(ref.name.lower(), [])
                same_schema = [
                    c for c in candidates if c.schema_name.lower() == obj.schema_name.lower()
                ]
                if len(candidates) > 1 and not same_schema:
                    continue
                if same_schema and ref not in same_schema:
                    continue
            seen.add(key)
            relationships.append(
                Relationship(
                    from_schema=source.schema_name,
                    from_name=source.name,
                    from_type=source.object_type,
                    to_schema=ref.schema_name,
                    to_name=ref.name,
                    to_type=ref.object_type,
                )
            )

    return relationships


def dedupe_relationships(relationships: Iterable[Relationship]) -> list[Relationship]:
    """Drop duplicate edges, keeping first-seen casing and types."""
    seen: set[tuple[str, str, str, str, str]] = set()
    unique: list[Relationship] = []
    for rel in relationships:
        if rel.from_name.lower() == rel.to_name.lower() and (
            rel.from_schema.lower() == rel.to_schema.lower()
        ):
            continue
        key = (
            rel.from_schema.lower(),
            rel.from_name.lower(),
            rel.to_schema.lower(),
            rel.to_name.lower(),
            rel.kind,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(rel)
    return unique


def _append_unique(items: list[ObjectRef], ref: ObjectRef) -> None:
    key = _key(ref.schema_name, ref.name)
    if any(_key(existing.schema_name, existing.name) == key for existing in items):
        return
    items.append(ref)


def apply_relationships(database: Database, relationships: Iterable[Relationship]) -> None:
    """Attach ``reads_from`` and reverse ``used_by`` lists on extracted objects."""
    objects: dict[tuple[str, str], Documentable] = {
        _key(obj.schema_name, obj.name): obj for obj in documentable_objects(database)
    }
    index = catalog_index(database)

    for rel in relationships:
        source_obj = objects.get(_key(rel.from_schema, rel.from_name))
        target_ref = index.get(_key(rel.to_schema, rel.to_name)) or rel.target
        source_ref = index.get(_key(rel.from_schema, rel.from_name)) or rel.source

        if source_obj is not None and hasattr(source_obj, "reads_from"):
            _append_unique(source_obj.reads_from, target_ref)

        target_obj = objects.get(_key(rel.to_schema, rel.to_name))
        if target_obj is not None and hasattr(target_obj, "used_by"):
            _append_unique(target_obj.used_by, source_ref)

    for obj in objects.values():
        obj.reads_from.sort(key=lambda r: (r.schema_name.lower(), r.name.lower()))
        obj.used_by.sort(key=lambda r: (r.schema_name.lower(), r.name.lower()))


def attach_object_dependencies(
    connection: Any,
    database: Database,
    config: Any = None,
) -> list[Relationship]:
    """Collect, dedupe, and attach the object dependency graph.

    Safe to call with ``connection=None``; catalog queries are then skipped
    and only ``base_tables`` plus SQL-definition scans are used.
    """
    relationships: list[Relationship] = []
    relationships.extend(relationships_from_view_base_tables(database))
    if connection is not None:
        relationships.extend(extract_catalog_relationships(connection, database, config))
    relationships.extend(relationships_from_sql_definitions(database))
    relationships = dedupe_relationships(relationships)
    apply_relationships(database, relationships)
    logger.info("Resolved %s object dependency edges", len(relationships))
    return relationships
