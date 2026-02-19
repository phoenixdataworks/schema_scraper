"""SQLite schema extractors."""

import logging
import re
from typing import Any, Optional

from ...base import BaseExtractor
from ...base.models import (
    Column,
    ForeignKey,
    Index,
    PrimaryKey,
    Table,
    Trigger,
    UniqueConstraint,
    View,
)

logger = logging.getLogger(__name__)


class TableExtractor(BaseExtractor):
    """Extracts table metadata from SQLite."""

    def extract(self) -> list[Table]:
        """Extract all tables with their metadata."""
        tables = self._get_tables()
        logger.info(f"Found {len(tables)} tables")

        for table in tables:
            table.columns = self._get_columns(table.name)
            table.primary_key = self._get_primary_key(table.name)
            table.foreign_keys = self._get_foreign_keys(table.name)
            table.indexes = self._get_indexes(table.name)
            table.unique_constraints = self._get_unique_constraints(table.name)
            table.triggers = self._get_table_triggers(table.name)
            table.row_count = self._get_row_count(table.name)

        self._build_references(tables)
        return tables

    def _get_tables(self) -> list[Table]:
        """Get list of all tables."""
        query = """
            SELECT name AS table_name
            FROM sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """
        rows = self.connection.execute_dict(query)
        return [Table(schema_name="main", name=row["table_name"]) for row in rows]

    def _get_columns(self, table_name: str) -> list[Column]:
        """Get columns for a table."""
        query = f"PRAGMA table_xinfo('{table_name}')"
        rows = self.connection.execute_dict(query)
        columns = []

        for row in rows:
            # Skip hidden columns that aren't generated (hidden = 1)
            # hidden = 0: normal column
            # hidden = 2: generated column (STORED)
            # hidden = 3: virtual column (VIRTUAL)
            if row["hidden"] == 1:
                continue

            # Parse type for length/precision
            data_type = row["type"].upper() if row["type"] else "TEXT"
            max_length = None
            precision = None
            scale = None

            # Extract length from types like VARCHAR(255)
            match = re.match(r"(\w+)\((\d+)(?:,\s*(\d+))?\)", data_type)
            if match:
                data_type = match.group(1)
                if match.group(3):
                    precision = int(match.group(2))
                    scale = int(match.group(3))
                else:
                    max_length = int(match.group(2))

            # Check if this is a computed column
            is_computed = row["hidden"] in (2, 3)
            computed_definition = row["sql"] if is_computed else None

            columns.append(
                Column(
                    name=row["name"],
                    data_type=data_type,
                    max_length=max_length,
                    precision=precision,
                    scale=scale,
                    is_nullable=not row["notnull"],
                    default_value=row["dflt_value"],
                    is_identity=bool(row["pk"]) and "AUTOINCREMENT" in self._get_create_sql(table_name).upper(),
                    is_computed=is_computed,
                    computed_definition=computed_definition,
                    ordinal_position=row["cid"] + 1,
                )
            )
        return columns

    def _get_primary_key(self, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key for a table."""
        query = f"PRAGMA table_info('{table_name}')"
        rows = self.connection.execute_dict(query)
        pk_columns = [row["name"] for row in rows if row["pk"] > 0]

        if not pk_columns:
            return None

        # Sort by pk order
        pk_rows = sorted([row for row in rows if row["pk"] > 0], key=lambda x: x["pk"])
        pk_columns = [row["name"] for row in pk_rows]

        return PrimaryKey(
            name=f"pk_{table_name}",
            columns=pk_columns,
            is_clustered=True,  # SQLite PKs are always clustered (rowid)
        )

    def _get_foreign_keys(self, table_name: str) -> list[ForeignKey]:
        """Get foreign keys for a table."""
        query = f"PRAGMA foreign_key_list('{table_name}')"
        rows = self.connection.execute_dict(query)

        # Group by FK id
        fk_map: dict[int, dict] = {}
        for row in rows:
            fk_id = row["id"]
            if fk_id not in fk_map:
                fk_map[fk_id] = {
                    "table": row["table"],
                    "on_update": row["on_update"],
                    "on_delete": row["on_delete"],
                    "columns": [],
                    "referenced_columns": [],
                }
            fk_map[fk_id]["columns"].append(row["from"])
            fk_map[fk_id]["referenced_columns"].append(row["to"])

        return [
            ForeignKey(
                name=f"fk_{table_name}_{fk_id}",
                columns=fk["columns"],
                referenced_schema="main",
                referenced_table=fk["table"],
                referenced_columns=fk["referenced_columns"],
                on_delete=fk["on_delete"],
                on_update=fk["on_update"],
            )
            for fk_id, fk in fk_map.items()
        ]

    def _get_indexes(self, table_name: str) -> list[Index]:
        """Get indexes for a table."""
        query = f"PRAGMA index_list('{table_name}')"
        idx_rows = self.connection.execute_dict(query)
        indexes = []

        for idx_row in idx_rows:
            col_query = f"PRAGMA index_info('{idx_row['name']}')"
            col_rows = self.connection.execute_dict(col_query)
            columns = [row["name"] for row in sorted(col_rows, key=lambda x: x["seqno"])]

            indexes.append(
                Index(
                    name=idx_row["name"],
                    columns=columns,
                    is_unique=bool(idx_row["unique"]),
                    is_primary_key=idx_row["origin"] == "pk",
                    index_type="BTREE",
                )
            )
        return indexes

    def _get_unique_constraints(self, table_name: str) -> list[UniqueConstraint]:
        """Get unique constraints for a table."""
        # SQLite doesn't have separate unique constraints - unique indexes serve this purpose
        # We'll extract unique indexes that are not primary keys as unique constraints
        query = f"PRAGMA index_list('{table_name}')"
        rows = self.connection.execute_dict(query)

        unique_constraints = []
        for row in rows:
            if row["unique"] and not row["origin"] == "pk":
                col_query = f"PRAGMA index_info('{row['name']}')"
                col_rows = self.connection.execute_dict(col_query)
                columns = [r["name"] for r in sorted(col_rows, key=lambda x: x["seqno"])]

                unique_constraints.append(
                    UniqueConstraint(
                        name=row["name"],
                        columns=columns,
                    )
                )
        return unique_constraints

    def _get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(*) FROM '{table_name}'"
        try:
            return self.connection.execute_scalar(query) or 0
        except Exception:
            return 0

    def _get_table_triggers(self, table_name: str) -> list[Trigger]:
        """Get triggers for a table."""
        query = """
            SELECT name, tbl_name, sql
            FROM sqlite_master
            WHERE type = 'trigger' AND tbl_name = ?
        """
        rows = self.connection.execute_dict(query, (table_name,))
        triggers = []

        for row in rows:
            sql = row["sql"] or ""
            sql_upper = sql.upper()

            # Parse trigger type and events from SQL
            if "INSTEAD OF" in sql_upper:
                trigger_type = "INSTEAD OF"
            elif "BEFORE" in sql_upper:
                trigger_type = "BEFORE"
            else:
                trigger_type = "AFTER"

            events = []
            if "INSERT" in sql_upper:
                events.append("INSERT")
            if "UPDATE" in sql_upper:
                events.append("UPDATE")
            if "DELETE" in sql_upper:
                events.append("DELETE")

            triggers.append(
                Trigger(
                    schema_name="main",
                    name=row["name"],
                    parent_table_schema="main",
                    parent_table_name=row["tbl_name"],
                    trigger_type=trigger_type,
                    events=events,
                    definition=sql,
                )
            )
        return triggers

    def _get_create_sql(self, table_name: str) -> str:
        """Get the CREATE TABLE SQL for a table."""
        query = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?"
        return self.connection.execute_scalar(query, (table_name,)) or ""

    def _build_references(self, tables: list[Table]) -> None:
        """Build the referenced_by list for each table."""
        table_map = {t.name: t for t in tables}

        for table in tables:
            for fk in table.foreign_keys:
                if fk.referenced_table in table_map:
                    table_map[fk.referenced_table].referenced_by.append(
                        (table.schema_name, table.name, fk.name)
                    )


class ViewExtractor(BaseExtractor):
    """Extracts view metadata from SQLite."""

    def extract(self) -> list[View]:
        """Extract all views with their metadata."""
        views = self._get_views()
        logger.info(f"Found {len(views)} views")

        for view in views:
            view.columns = self._get_columns(view.name)
            view.definition = self._get_definition(view.name)

        return views

    def _get_views(self) -> list[View]:
        """Get list of all views."""
        query = """
            SELECT name AS view_name
            FROM sqlite_master
            WHERE type = 'view'
            ORDER BY name
        """
        rows = self.connection.execute_dict(query)
        return [View(schema_name="main", name=row["view_name"]) for row in rows]

    def _get_columns(self, view_name: str) -> list[Column]:
        """Get columns for a view."""
        query = f"PRAGMA table_info('{view_name}')"
        rows = self.connection.execute_dict(query)
        return [
            Column(
                name=row["name"],
                data_type=row["type"].upper() if row["type"] else "TEXT",
                is_nullable=not row["notnull"],
                ordinal_position=row["cid"] + 1,
            )
            for row in rows
        ]

    def _get_definition(self, view_name: str) -> Optional[str]:
        """Get the SQL definition of a view."""
        query = "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = ?"
        return self.connection.execute_scalar(query, (view_name,))


class TriggerExtractor(BaseExtractor):
    """Extracts trigger metadata from SQLite."""

    def extract(self) -> list[Trigger]:
        """Extract all triggers."""
        triggers = self._get_triggers()
        logger.info(f"Found {len(triggers)} triggers")
        return triggers

    def _get_triggers(self) -> list[Trigger]:
        """Get list of all triggers."""
        query = """
            SELECT name, tbl_name, sql
            FROM sqlite_master
            WHERE type = 'trigger'
            ORDER BY name
        """
        rows = self.connection.execute_dict(query)
        triggers = []

        for row in rows:
            sql = row["sql"] or ""
            sql_upper = sql.upper()

            # Parse trigger type and events from SQL
            if "INSTEAD OF" in sql_upper:
                trigger_type = "INSTEAD OF"
            elif "BEFORE" in sql_upper:
                trigger_type = "BEFORE"
            else:
                trigger_type = "AFTER"

            events = []
            if "INSERT" in sql_upper:
                events.append("INSERT")
            if "UPDATE" in sql_upper:
                events.append("UPDATE")
            if "DELETE" in sql_upper:
                events.append("DELETE")

            triggers.append(
                Trigger(
                    schema_name="main",
                    name=row["name"],
                    parent_table_schema="main",
                    parent_table_name=row["tbl_name"],
                    trigger_type=trigger_type,
                    events=events,
                    definition=sql,
                )
            )
        return triggers
