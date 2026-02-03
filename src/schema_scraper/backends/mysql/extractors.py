"""MySQL schema extractors."""

import logging
from typing import Any, Optional

from ...base import BaseExtractor
from ...base.models import (
    CheckConstraint,
    Column,
    ForeignKey,
    Function,
    Index,
    Parameter,
    PrimaryKey,
    Procedure,
    Table,
    Trigger,
    View,
)

logger = logging.getLogger(__name__)


class TableExtractor(BaseExtractor):
    """Extracts table metadata from MySQL."""

    def extract(self) -> list[Table]:
        """Extract all tables with their metadata."""
        tables = self._get_tables()
        logger.info(f"Found {len(tables)} tables")

        for table in tables:
            table.columns = self._get_columns(table.schema_name, table.name)
            table.primary_key = self._get_primary_key(table.schema_name, table.name)
            table.foreign_keys = self._get_foreign_keys(table.schema_name, table.name)
            table.indexes = self._get_indexes(table.schema_name, table.name)
            table.check_constraints = self._get_check_constraints(table.schema_name, table.name)
            stats = self._get_table_stats(table.schema_name, table.name)
            table.row_count = stats.get("row_count", 0)
            table.total_space_kb = stats.get("total_space_kb", 0)
            table.description = stats.get("description")

        self._build_references(tables)
        return tables

    def _get_tables(self) -> list[Table]:
        """Get list of all tables."""
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY table_schema, table_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Table(schema_name=row["table_schema"], name=row["table_name"])
            for row in rows
            if self._should_include_schema(row["table_schema"])
        ]

    def _get_columns(self, schema_name: str, table_name: str) -> list[Column]:
        """Get columns for a table."""
        query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length AS max_length,
                numeric_precision AS `precision`,
                numeric_scale AS scale,
                is_nullable = 'YES' AS is_nullable,
                column_default AS default_value,
                extra LIKE '%%auto_increment%%' AS is_identity,
                generation_expression AS computed_definition,
                collation_name,
                ordinal_position,
                column_comment AS description
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            Column(
                name=row["column_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_nullable=bool(row["is_nullable"]),
                default_value=row["default_value"],
                is_identity=bool(row["is_identity"]),
                is_computed=bool(row["computed_definition"]),
                computed_definition=row["computed_definition"],
                collation=row["collation_name"],
                ordinal_position=row["ordinal_position"],
                description=row["description"] if row["description"] else None,
            )
            for row in rows
        ]

    def _get_primary_key(self, schema_name: str, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key for a table."""
        query = """
            SELECT
                constraint_name,
                GROUP_CONCAT(column_name ORDER BY ordinal_position) AS columns
            FROM information_schema.key_column_usage
            WHERE table_schema = %s AND table_name = %s
            AND constraint_name = 'PRIMARY'
            GROUP BY constraint_name
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        if not rows:
            return None
        row = rows[0]
        return PrimaryKey(name=row["constraint_name"], columns=row["columns"].split(","), is_clustered=True)

    def _get_foreign_keys(self, schema_name: str, table_name: str) -> list[ForeignKey]:
        """Get foreign keys for a table."""
        query = """
            SELECT
                kcu.constraint_name,
                GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) AS columns,
                kcu.referenced_table_schema AS referenced_schema,
                kcu.referenced_table_name AS referenced_table,
                GROUP_CONCAT(kcu.referenced_column_name ORDER BY kcu.ordinal_position) AS referenced_columns,
                rc.delete_rule AS on_delete,
                rc.update_rule AS on_update
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
                AND kcu.constraint_schema = rc.constraint_schema
            WHERE kcu.table_schema = %s AND kcu.table_name = %s
            AND kcu.referenced_table_name IS NOT NULL
            GROUP BY kcu.constraint_name, kcu.referenced_table_schema,
                     kcu.referenced_table_name, rc.delete_rule, rc.update_rule
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            ForeignKey(
                name=row["constraint_name"],
                columns=row["columns"].split(","),
                referenced_schema=row["referenced_schema"],
                referenced_table=row["referenced_table"],
                referenced_columns=row["referenced_columns"].split(","),
                on_delete=row["on_delete"],
                on_update=row["on_update"],
            )
            for row in rows
        ]

    def _get_indexes(self, schema_name: str, table_name: str) -> list[Index]:
        """Get indexes for a table."""
        query = """
            SELECT
                index_name,
                NOT non_unique AS is_unique,
                index_name = 'PRIMARY' AS is_primary_key,
                index_type,
                GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
            FROM information_schema.statistics
            WHERE table_schema = %s AND table_name = %s
            GROUP BY index_name, non_unique, index_type
            ORDER BY index_name
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            Index(
                name=row["index_name"],
                columns=row["columns"].split(","),
                is_unique=bool(row["is_unique"]),
                is_primary_key=bool(row["is_primary_key"]),
                is_clustered=row["index_name"] == "PRIMARY",
                index_type=row["index_type"],
            )
            for row in rows
        ]

    def _get_check_constraints(self, schema_name: str, table_name: str) -> list[CheckConstraint]:
        """Get check constraints for a table (MySQL 8.0.16+)."""
        query = """
            SELECT
                tc.constraint_name,
                cc.check_clause AS definition
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
                AND tc.constraint_schema = cc.constraint_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s
            AND tc.constraint_type = 'CHECK'
        """
        try:
            rows = self.connection.execute_dict(query, (schema_name, table_name))
            return [CheckConstraint(name=row["constraint_name"], definition=row["definition"]) for row in rows]
        except Exception:
            return []  # Check constraints not supported in older MySQL

    def _get_table_stats(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Get row count and space statistics."""
        query = """
            SELECT
                table_rows AS row_count,
                ROUND((data_length + index_length) / 1024) AS total_space_kb,
                table_comment AS description
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        if rows:
            return {
                "row_count": rows[0]["row_count"] or 0,
                "total_space_kb": rows[0]["total_space_kb"] or 0,
                "description": rows[0]["description"] if rows[0]["description"] else None,
            }
        return {"row_count": 0, "total_space_kb": 0, "description": None}

    def _build_references(self, tables: list[Table]) -> None:
        """Build the referenced_by list for each table."""
        query = """
            SELECT
                kcu.table_schema AS parent_schema,
                kcu.table_name AS parent_table,
                kcu.constraint_name AS fk_name,
                kcu.referenced_table_schema AS referenced_schema,
                kcu.referenced_table_name AS referenced_table
            FROM information_schema.key_column_usage kcu
            WHERE kcu.referenced_table_name IS NOT NULL
        """
        rows = self.connection.execute_dict(query)
        table_map = {(t.schema_name, t.name): t for t in tables}

        for row in rows:
            ref_key = (row["referenced_schema"], row["referenced_table"])
            if ref_key in table_map:
                table_map[ref_key].referenced_by.append(
                    (row["parent_schema"], row["parent_table"], row["fk_name"])
                )


class ViewExtractor(BaseExtractor):
    """Extracts view metadata from MySQL."""

    def extract(self) -> list[View]:
        """Extract all views with their metadata."""
        views = self._get_views()
        logger.info(f"Found {len(views)} views")

        for view in views:
            view.columns = self._get_columns(view.schema_name, view.name)
            view.definition = self._get_definition(view.schema_name, view.name)

        return views

    def _get_views(self) -> list[View]:
        """Get list of all views."""
        query = """
            SELECT table_schema AS schema_name, table_name AS view_name
            FROM information_schema.views
            WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY table_schema, table_name
        """
        rows = self.connection.execute_dict(query)
        return [
            View(schema_name=row["schema_name"], name=row["view_name"])
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_columns(self, schema_name: str, view_name: str) -> list[Column]:
        """Get columns for a view."""
        query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length AS max_length,
                numeric_precision AS `precision`,
                numeric_scale AS scale,
                is_nullable = 'YES' AS is_nullable,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, view_name))
        return [
            Column(
                name=row["column_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_nullable=bool(row["is_nullable"]),
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, view_name: str) -> Optional[str]:
        """Get the SQL definition of a view."""
        query = """
            SELECT view_definition
            FROM information_schema.views
            WHERE table_schema = %s AND table_name = %s
        """
        return self.connection.execute_scalar(query, (schema_name, view_name))


class ProcedureExtractor(BaseExtractor):
    """Extracts stored procedure metadata from MySQL."""

    def extract(self) -> list[Procedure]:
        """Extract all stored procedures."""
        procedures = self._get_procedures()
        logger.info(f"Found {len(procedures)} stored procedures")

        for proc in procedures:
            proc.parameters = self._get_parameters(proc.schema_name, proc.name)
            proc.definition = self._get_definition(proc.schema_name, proc.name)

        return procedures

    def _get_procedures(self) -> list[Procedure]:
        """Get list of all stored procedures."""
        query = """
            SELECT
                routine_schema AS schema_name,
                routine_name AS procedure_name,
                routine_comment AS description
            FROM information_schema.routines
            WHERE routine_type = 'PROCEDURE'
            AND routine_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY routine_schema, routine_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Procedure(
                schema_name=row["schema_name"],
                name=row["procedure_name"],
                description=row["description"] if row["description"] else None,
                language="SQL",
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, proc_name: str) -> list[Parameter]:
        """Get parameters for a procedure."""
        query = """
            SELECT
                parameter_name,
                data_type,
                character_maximum_length AS max_length,
                numeric_precision AS `precision`,
                numeric_scale AS scale,
                parameter_mode IN ('OUT', 'INOUT') AS is_output,
                ordinal_position
            FROM information_schema.parameters
            WHERE specific_schema = %s AND specific_name = %s
            AND parameter_name IS NOT NULL
            ORDER BY ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, proc_name))
        return [
            Parameter(
                name=row["parameter_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_output=bool(row["is_output"]),
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, proc_name: str) -> Optional[str]:
        """Get procedure definition."""
        query = """
            SELECT routine_definition
            FROM information_schema.routines
            WHERE routine_schema = %s AND routine_name = %s AND routine_type = 'PROCEDURE'
        """
        return self.connection.execute_scalar(query, (schema_name, proc_name))


class FunctionExtractor(BaseExtractor):
    """Extracts function metadata from MySQL."""

    def extract(self) -> list[Function]:
        """Extract all functions."""
        functions = self._get_functions()
        logger.info(f"Found {len(functions)} functions")

        for func in functions:
            func.parameters = self._get_parameters(func.schema_name, func.name)
            func.definition = self._get_definition(func.schema_name, func.name)

        return functions

    def _get_functions(self) -> list[Function]:
        """Get list of all functions."""
        query = """
            SELECT
                routine_schema AS schema_name,
                routine_name AS function_name,
                data_type AS return_type,
                routine_comment AS description
            FROM information_schema.routines
            WHERE routine_type = 'FUNCTION'
            AND routine_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY routine_schema, routine_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Function(
                schema_name=row["schema_name"],
                name=row["function_name"],
                function_type="SCALAR",
                return_type=row["return_type"],
                description=row["description"] if row["description"] else None,
                language="SQL",
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, func_name: str) -> list[Parameter]:
        """Get parameters for a function."""
        query = """
            SELECT
                parameter_name,
                data_type,
                character_maximum_length AS max_length,
                numeric_precision AS `precision`,
                numeric_scale AS scale,
                ordinal_position
            FROM information_schema.parameters
            WHERE specific_schema = %s AND specific_name = %s
            AND parameter_mode = 'IN'
            AND parameter_name IS NOT NULL
            ORDER BY ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, func_name))
        return [
            Parameter(
                name=row["parameter_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, func_name: str) -> Optional[str]:
        """Get function definition."""
        query = """
            SELECT routine_definition
            FROM information_schema.routines
            WHERE routine_schema = %s AND routine_name = %s AND routine_type = 'FUNCTION'
        """
        return self.connection.execute_scalar(query, (schema_name, func_name))


class TriggerExtractor(BaseExtractor):
    """Extracts trigger metadata from MySQL."""

    def extract(self) -> list[Trigger]:
        """Extract all triggers."""
        triggers = self._get_triggers()
        logger.info(f"Found {len(triggers)} triggers")
        return triggers

    def _get_triggers(self) -> list[Trigger]:
        """Get list of all triggers."""
        query = """
            SELECT
                trigger_schema AS schema_name,
                trigger_name,
                event_object_schema AS parent_schema,
                event_object_table AS parent_table,
                action_timing AS trigger_type,
                event_manipulation AS event,
                action_statement AS definition
            FROM information_schema.triggers
            WHERE trigger_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
            ORDER BY trigger_schema, trigger_name
        """
        rows = self.connection.execute_dict(query)
        triggers = []

        for row in rows:
            if not self._should_include_schema(row["schema_name"]):
                continue
            triggers.append(
                Trigger(
                    schema_name=row["schema_name"],
                    name=row["trigger_name"],
                    parent_table_schema=row["parent_schema"],
                    parent_table_name=row["parent_table"],
                    trigger_type=row["trigger_type"],
                    events=[row["event"]],
                    definition=row["definition"],
                )
            )
        return triggers
