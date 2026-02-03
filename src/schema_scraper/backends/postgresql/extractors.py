"""PostgreSQL schema extractors."""

import logging
from typing import Any, Optional

from ...base import BaseExtractor
from ...base.models import (
    CheckConstraint,
    Column,
    ForeignKey,
    Function,
    FunctionColumn,
    Index,
    Parameter,
    PrimaryKey,
    Procedure,
    Sequence,
    Table,
    Trigger,
    TypeColumn,
    UserDefinedType,
    View,
)

logger = logging.getLogger(__name__)


class TableExtractor(BaseExtractor):
    """Extracts table metadata from PostgreSQL."""

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
            table.description = self._get_description(table.schema_name, table.name)
            stats = self._get_table_stats(table.schema_name, table.name)
            table.row_count = stats.get("row_count", 0)
            table.total_space_kb = stats.get("total_space_kb", 0)

        self._build_references(tables)
        return tables

    def _get_tables(self) -> list[Table]:
        """Get list of all tables."""
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
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
                c.column_name,
                c.data_type,
                c.character_maximum_length AS max_length,
                c.numeric_precision AS precision,
                c.numeric_scale AS scale,
                c.is_nullable = 'YES' AS is_nullable,
                c.column_default AS default_value,
                c.is_identity = 'YES' AS is_identity,
                c.identity_generation,
                c.ordinal_position,
                c.collation_name,
                pgd.description
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_statio_all_tables st
                ON c.table_schema = st.schemaname AND c.table_name = st.relname
            LEFT JOIN pg_catalog.pg_description pgd
                ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            Column(
                name=row["column_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_nullable=row["is_nullable"],
                default_value=row["default_value"],
                is_identity=row["is_identity"],
                collation=row["collation_name"],
                ordinal_position=row["ordinal_position"],
                description=row["description"],
            )
            for row in rows
        ]

    def _get_primary_key(self, schema_name: str, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key for a table."""
        query = """
            SELECT
                tc.constraint_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s AND tc.table_name = %s
            GROUP BY tc.constraint_name
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        if not rows:
            return None
        row = rows[0]
        return PrimaryKey(name=row["constraint_name"], columns=row["columns"], is_clustered=False)

    def _get_foreign_keys(self, schema_name: str, table_name: str) -> list[ForeignKey]:
        """Get foreign keys for a table."""
        query = """
            SELECT
                tc.constraint_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns,
                ccu.table_schema AS referenced_schema,
                ccu.table_name AS referenced_table,
                array_agg(ccu.column_name ORDER BY kcu.ordinal_position) AS referenced_columns,
                rc.delete_rule AS on_delete,
                rc.update_rule AS on_update
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = %s AND tc.table_name = %s
            GROUP BY tc.constraint_name, ccu.table_schema, ccu.table_name, rc.delete_rule, rc.update_rule
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            ForeignKey(
                name=row["constraint_name"],
                columns=row["columns"],
                referenced_schema=row["referenced_schema"],
                referenced_table=row["referenced_table"],
                referenced_columns=row["referenced_columns"],
                on_delete=row["on_delete"],
                on_update=row["on_update"],
            )
            for row in rows
        ]

    def _get_indexes(self, schema_name: str, table_name: str) -> list[Index]:
        """Get indexes for a table."""
        query = """
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary_key,
                am.amname AS index_type,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns,
                pg_get_expr(ix.indpred, ix.indrelid) AS filter_definition
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = %s AND t.relname = %s
            GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname, ix.indpred, ix.indrelid
            ORDER BY i.relname
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            Index(
                name=row["index_name"],
                columns=row["columns"],
                is_unique=row["is_unique"],
                is_primary_key=row["is_primary_key"],
                index_type=row["index_type"].upper(),
                filter_definition=row["filter_definition"],
            )
            for row in rows
        ]

    def _get_check_constraints(self, schema_name: str, table_name: str) -> list[CheckConstraint]:
        """Get check constraints for a table."""
        query = """
            SELECT
                tc.constraint_name,
                cc.check_clause AS definition
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name AND tc.constraint_schema = cc.constraint_schema
            WHERE tc.constraint_type = 'CHECK'
            AND tc.table_schema = %s AND tc.table_name = %s
            AND tc.constraint_name NOT LIKE '%%_not_null'
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [CheckConstraint(name=row["constraint_name"], definition=row["definition"]) for row in rows]

    def _get_description(self, schema_name: str, table_name: str) -> Optional[str]:
        """Get table description from pg_description."""
        query = """
            SELECT obj_description(c.oid) AS description
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
        return self.connection.execute_scalar(query, (schema_name, table_name))

    def _get_table_stats(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Get row count and space statistics."""
        query = """
            SELECT
                COALESCE(c.reltuples::bigint, 0) AS row_count,
                COALESCE(pg_total_relation_size(c.oid) / 1024, 0) AS total_space_kb
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        if rows:
            return {"row_count": rows[0]["row_count"], "total_space_kb": rows[0]["total_space_kb"]}
        return {"row_count": 0, "total_space_kb": 0}

    def _build_references(self, tables: list[Table]) -> None:
        """Build the referenced_by list for each table."""
        query = """
            SELECT
                tc.table_schema AS parent_schema,
                tc.table_name AS parent_table,
                tc.constraint_name AS fk_name,
                ccu.table_schema AS referenced_schema,
                ccu.table_name AS referenced_table
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
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
    """Extracts view metadata from PostgreSQL."""

    def extract(self) -> list[View]:
        """Extract all views with their metadata."""
        views = self._get_views()
        logger.info(f"Found {len(views)} views")

        for view in views:
            view.columns = self._get_columns(view.schema_name, view.name)
            view.definition = self._get_definition(view.schema_name, view.name)
            view.description = self._get_description(view.schema_name, view.name)

        return views

    def _get_views(self) -> list[View]:
        """Get list of all views."""
        query = """
            SELECT
                schemaname AS schema_name,
                viewname AS view_name,
                FALSE AS is_materialized
            FROM pg_views
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT
                schemaname AS schema_name,
                matviewname AS view_name,
                TRUE AS is_materialized
            FROM pg_matviews
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name, view_name
        """
        rows = self.connection.execute_dict(query)
        return [
            View(schema_name=row["schema_name"], name=row["view_name"], is_materialized=row["is_materialized"])
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_columns(self, schema_name: str, view_name: str) -> list[Column]:
        """Get columns for a view."""
        query = """
            SELECT
                c.column_name,
                c.data_type,
                c.character_maximum_length AS max_length,
                c.numeric_precision AS precision,
                c.numeric_scale AS scale,
                c.is_nullable = 'YES' AS is_nullable,
                c.ordinal_position
            FROM information_schema.columns c
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, view_name))
        return [
            Column(
                name=row["column_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_nullable=row["is_nullable"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, view_name: str) -> Optional[str]:
        """Get the SQL definition of a view."""
        # Try regular view first
        query = "SELECT definition FROM pg_views WHERE schemaname = %s AND viewname = %s"
        result = self.connection.execute_scalar(query, (schema_name, view_name))
        if result:
            return result
        # Try materialized view
        query = "SELECT definition FROM pg_matviews WHERE schemaname = %s AND matviewname = %s"
        return self.connection.execute_scalar(query, (schema_name, view_name))

    def _get_description(self, schema_name: str, view_name: str) -> Optional[str]:
        """Get view description."""
        query = """
            SELECT obj_description(c.oid) AS description
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """
        return self.connection.execute_scalar(query, (schema_name, view_name))


class ProcedureExtractor(BaseExtractor):
    """Extracts stored procedure metadata from PostgreSQL."""

    def extract(self) -> list[Procedure]:
        """Extract all stored procedures."""
        procedures = self._get_procedures()
        logger.info(f"Found {len(procedures)} stored procedures")

        for proc in procedures:
            proc.parameters = self._get_parameters(proc.schema_name, proc.name)
            proc.definition = self._get_definition(proc.schema_name, proc.name)
            proc.description = self._get_description(proc.schema_name, proc.name)

        return procedures

    def _get_procedures(self) -> list[Procedure]:
        """Get list of all stored procedures (PostgreSQL 11+)."""
        query = """
            SELECT
                n.nspname AS schema_name,
                p.proname AS procedure_name,
                l.lanname AS language
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_language l ON p.prolang = l.oid
            WHERE p.prokind = 'p'
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, p.proname
        """
        rows = self.connection.execute_dict(query)
        return [
            Procedure(schema_name=row["schema_name"], name=row["procedure_name"], language=row["language"])
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, proc_name: str) -> list[Parameter]:
        """Get parameters for a procedure."""
        query = """
            SELECT
                p.parameter_name,
                p.data_type,
                p.character_maximum_length AS max_length,
                p.numeric_precision AS precision,
                p.numeric_scale AS scale,
                p.parameter_mode = 'OUT' OR p.parameter_mode = 'INOUT' AS is_output,
                p.parameter_default IS NOT NULL AS has_default,
                p.parameter_default AS default_value,
                p.ordinal_position
            FROM information_schema.parameters p
            WHERE p.specific_schema = %s
            AND p.specific_name LIKE %s || '%%'
            ORDER BY p.ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, proc_name))
        return [
            Parameter(
                name=row["parameter_name"] or f"param{row['ordinal_position']}",
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_output=row["is_output"],
                has_default=row["has_default"],
                default_value=row["default_value"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, proc_name: str) -> Optional[str]:
        """Get procedure definition."""
        query = """
            SELECT pg_get_functiondef(p.oid) AS definition
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s AND p.prokind = 'p'
        """
        return self.connection.execute_scalar(query, (schema_name, proc_name))

    def _get_description(self, schema_name: str, proc_name: str) -> Optional[str]:
        """Get procedure description."""
        query = """
            SELECT obj_description(p.oid) AS description
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s AND p.prokind = 'p'
        """
        return self.connection.execute_scalar(query, (schema_name, proc_name))


class FunctionExtractor(BaseExtractor):
    """Extracts function metadata from PostgreSQL."""

    def extract(self) -> list[Function]:
        """Extract all functions."""
        functions = self._get_functions()
        logger.info(f"Found {len(functions)} functions")

        for func in functions:
            func.parameters = self._get_parameters(func.schema_name, func.name)
            func.definition = self._get_definition(func.schema_name, func.name)
            func.description = self._get_description(func.schema_name, func.name)
            if func.function_type == "TABLE":
                func.return_columns = self._get_return_columns(func.schema_name, func.name)

        return functions

    def _get_functions(self) -> list[Function]:
        """Get list of all functions."""
        query = """
            SELECT
                n.nspname AS schema_name,
                p.proname AS function_name,
                CASE
                    WHEN p.proretset THEN 'TABLE'
                    WHEN p.prokind = 'a' THEN 'AGGREGATE'
                    WHEN p.prokind = 'w' THEN 'WINDOW'
                    ELSE 'SCALAR'
                END AS function_type,
                pg_get_function_result(p.oid) AS return_type,
                l.lanname AS language
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_language l ON p.prolang = l.oid
            WHERE p.prokind IN ('f', 'a', 'w')
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, p.proname
        """
        rows = self.connection.execute_dict(query)
        return [
            Function(
                schema_name=row["schema_name"],
                name=row["function_name"],
                function_type=row["function_type"],
                return_type=row["return_type"],
                language=row["language"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, func_name: str) -> list[Parameter]:
        """Get parameters for a function."""
        query = """
            SELECT
                p.parameter_name,
                p.data_type,
                p.character_maximum_length AS max_length,
                p.numeric_precision AS precision,
                p.numeric_scale AS scale,
                p.parameter_mode IN ('OUT', 'INOUT') AS is_output,
                p.parameter_default IS NOT NULL AS has_default,
                p.parameter_default AS default_value,
                p.ordinal_position
            FROM information_schema.parameters p
            WHERE p.specific_schema = %s
            AND p.specific_name LIKE %s || '%%'
            AND p.parameter_mode != 'OUT'
            ORDER BY p.ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, func_name))
        return [
            Parameter(
                name=row["parameter_name"] or f"param{row['ordinal_position']}",
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_output=row["is_output"],
                has_default=row["has_default"],
                default_value=row["default_value"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, func_name: str) -> Optional[str]:
        """Get function definition."""
        query = """
            SELECT pg_get_functiondef(p.oid) AS definition
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s AND p.prokind IN ('f', 'a', 'w')
            LIMIT 1
        """
        return self.connection.execute_scalar(query, (schema_name, func_name))

    def _get_description(self, schema_name: str, func_name: str) -> Optional[str]:
        """Get function description."""
        query = """
            SELECT obj_description(p.oid) AS description
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s
            LIMIT 1
        """
        return self.connection.execute_scalar(query, (schema_name, func_name))

    def _get_return_columns(self, schema_name: str, func_name: str) -> list[FunctionColumn]:
        """Get return columns for table-valued functions."""
        query = """
            SELECT
                p.parameter_name AS column_name,
                p.data_type,
                p.character_maximum_length AS max_length,
                p.numeric_precision AS precision,
                p.numeric_scale AS scale,
                TRUE AS is_nullable,
                p.ordinal_position
            FROM information_schema.parameters p
            WHERE p.specific_schema = %s
            AND p.specific_name LIKE %s || '%%'
            AND p.parameter_mode = 'OUT'
            ORDER BY p.ordinal_position
        """
        rows = self.connection.execute_dict(query, (schema_name, func_name))
        return [
            FunctionColumn(
                name=row["column_name"] or f"column{row['ordinal_position']}",
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_nullable=row["is_nullable"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]


class TriggerExtractor(BaseExtractor):
    """Extracts trigger metadata from PostgreSQL."""

    def extract(self) -> list[Trigger]:
        """Extract all triggers."""
        triggers = self._get_triggers()
        logger.info(f"Found {len(triggers)} triggers")
        return triggers

    def _get_triggers(self) -> list[Trigger]:
        """Get list of all triggers."""
        query = """
            SELECT
                n.nspname AS schema_name,
                t.tgname AS trigger_name,
                tn.nspname AS parent_schema,
                c.relname AS parent_table,
                CASE
                    WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
                    WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
                    ELSE 'AFTER'
                END AS trigger_type,
                t.tgtype & 4 = 4 AS is_insert,
                t.tgtype & 8 = 8 AS is_delete,
                t.tgtype & 16 = 16 AS is_update,
                NOT t.tgenabled = 'D' AS is_enabled,
                pg_get_triggerdef(t.oid) AS definition
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_namespace tn ON c.relnamespace = tn.oid
            WHERE NOT t.tgisinternal
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, t.tgname
        """
        rows = self.connection.execute_dict(query)
        triggers = []

        for row in rows:
            if not self._should_include_schema(row["schema_name"]):
                continue
            events = []
            if row["is_insert"]:
                events.append("INSERT")
            if row["is_update"]:
                events.append("UPDATE")
            if row["is_delete"]:
                events.append("DELETE")
            triggers.append(
                Trigger(
                    schema_name=row["schema_name"],
                    name=row["trigger_name"],
                    parent_table_schema=row["parent_schema"],
                    parent_table_name=row["parent_table"],
                    trigger_type=row["trigger_type"],
                    events=events,
                    definition=row["definition"],
                    is_disabled=not row["is_enabled"],
                )
            )
        return triggers


class TypeExtractor(BaseExtractor):
    """Extracts user-defined type metadata from PostgreSQL."""

    def extract(self) -> list[UserDefinedType]:
        """Extract all user-defined types."""
        types = self._get_types()
        logger.info(f"Found {len(types)} user-defined types")

        for udt in types:
            if udt.type_category == "COMPOSITE":
                udt.columns = self._get_composite_columns(udt.schema_name, udt.name)
            elif udt.type_category == "ENUM":
                udt.enum_values = self._get_enum_values(udt.schema_name, udt.name)

        return types

    def _get_types(self) -> list[UserDefinedType]:
        """Get list of all user-defined types."""
        query = """
            SELECT
                n.nspname AS schema_name,
                t.typname AS type_name,
                CASE
                    WHEN t.typtype = 'c' THEN 'COMPOSITE'
                    WHEN t.typtype = 'e' THEN 'ENUM'
                    WHEN t.typtype = 'd' THEN 'DOMAIN'
                    WHEN t.typtype = 'r' THEN 'RANGE'
                    ELSE 'OTHER'
                END AS type_category,
                bt.typname AS base_type,
                t.typnotnull AS is_not_null,
                pg_get_constraintdef(con.oid) AS check_constraint,
                obj_description(t.oid) AS description
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            LEFT JOIN pg_type bt ON t.typbasetype = bt.oid
            LEFT JOIN pg_constraint con ON con.contypid = t.oid
            WHERE t.typtype IN ('c', 'e', 'd', 'r')
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid AND c.relkind = 'r')
            ORDER BY n.nspname, t.typname
        """
        rows = self.connection.execute_dict(query)
        return [
            UserDefinedType(
                schema_name=row["schema_name"],
                name=row["type_name"],
                type_category=row["type_category"],
                base_type=row["base_type"],
                is_nullable=not row["is_not_null"] if row["is_not_null"] is not None else True,
                check_constraint=row["check_constraint"],
                description=row["description"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_composite_columns(self, schema_name: str, type_name: str) -> list[TypeColumn]:
        """Get columns for a composite type."""
        query = """
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                NOT a.attnotnull AS is_nullable,
                a.attnum AS ordinal_position
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = t.typrelid
            WHERE n.nspname = %s AND t.typname = %s
            AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        rows = self.connection.execute_dict(query, (schema_name, type_name))
        return [
            TypeColumn(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=row["is_nullable"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_enum_values(self, schema_name: str, type_name: str) -> list[str]:
        """Get values for an enum type."""
        query = """
            SELECT e.enumlabel
            FROM pg_enum e
            JOIN pg_type t ON e.enumtypid = t.oid
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = %s AND t.typname = %s
            ORDER BY e.enumsortorder
        """
        rows = self.connection.execute_dict(query, (schema_name, type_name))
        return [row["enumlabel"] for row in rows]


class SequenceExtractor(BaseExtractor):
    """Extracts sequence metadata from PostgreSQL."""

    def extract(self) -> list[Sequence]:
        """Extract all sequences."""
        sequences = self._get_sequences()
        logger.info(f"Found {len(sequences)} sequences")
        return sequences

    def _get_sequences(self) -> list[Sequence]:
        """Get list of all sequences."""
        query = """
            SELECT
                schemaname AS schema_name,
                sequencename AS sequence_name,
                data_type,
                start_value,
                increment_by AS increment,
                min_value,
                max_value,
                cycle AS is_cycling,
                cache_size,
                last_value AS current_value
            FROM pg_sequences
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, sequencename
        """
        rows = self.connection.execute_dict(query)
        return [
            Sequence(
                schema_name=row["schema_name"],
                name=row["sequence_name"],
                data_type=row["data_type"],
                start_value=row["start_value"] or 1,
                increment=row["increment"] or 1,
                min_value=row["min_value"] or 1,
                max_value=row["max_value"] or 9223372036854775807,
                is_cycling=row["is_cycling"],
                cache_size=row["cache_size"],
                current_value=row["current_value"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]
