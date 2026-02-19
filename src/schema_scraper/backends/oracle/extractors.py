"""Oracle schema extractors."""

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
    Partition,
    PartitionScheme,
    PrimaryKey,
    Procedure,
    Sequence,
    Synonym,
    Table,
    TablePartitioning,
    Trigger,
    TypeColumn,
    UniqueConstraint,
    UserDefinedType,
    View,
)

logger = logging.getLogger(__name__)


class TableExtractor(BaseExtractor):
    """Extracts table metadata from Oracle."""

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
            table.unique_constraints = self._get_unique_constraints(table.schema_name, table.name)
            table.triggers = self._get_table_triggers(table.schema_name, table.name)
            table.partitioning = self._get_partitioning(table.schema_name, table.name)
            table.description = self._get_description(table.schema_name, table.name)
            stats = self._get_table_stats(table.schema_name, table.name)
            table.row_count = stats.get("row_count", 0)
            table.total_space_kb = stats.get("total_space_kb", 0)

        self._build_references(tables)
        return tables

    def _get_tables(self) -> list[Table]:
        """Get list of all tables."""
        query = """
            SELECT owner AS schema_name, table_name
            FROM all_tables
            WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                               'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                               'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS')
            ORDER BY owner, table_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Table(schema_name=row["schema_name"], name=row["table_name"])
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_columns(self, schema_name: str, table_name: str) -> list[Column]:
        """Get columns for a table."""
        query = """
            SELECT
                c.column_name,
                c.data_type,
                c.data_length AS max_length,
                c.data_precision AS precision,
                c.data_scale AS scale,
                CASE WHEN c.nullable = 'Y' THEN 1 ELSE 0 END AS is_nullable,
                c.data_default AS default_value,
                c.column_id AS ordinal_position,
                CASE WHEN c.identity_column = 'YES' THEN 1 ELSE 0 END AS is_identity,
                CASE WHEN c.virtual_column = 'YES' THEN 1 ELSE 0 END AS is_virtual,
                cc.comments AS description
            FROM all_tab_columns c
            LEFT JOIN all_col_comments cc
                ON c.owner = cc.owner AND c.table_name = cc.table_name AND c.column_name = cc.column_name
            WHERE c.owner = :1 AND c.table_name = :2
            ORDER BY c.column_id
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
                default_value=str(row["default_value"]).strip() if row["default_value"] and not row["is_virtual"] else None,
                is_identity=bool(row["is_identity"]),
                is_computed=bool(row["is_virtual"]),
                computed_definition=str(row["default_value"]).strip() if row["is_virtual"] and row["default_value"] else None,
                ordinal_position=row["ordinal_position"],
                description=row["description"],
            )
            for row in rows
        ]

    def _get_primary_key(self, schema_name: str, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key for a table."""
        query = """
            SELECT constraint_name
            FROM all_constraints
            WHERE owner = :1 AND table_name = :2 AND constraint_type = 'P'
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        if not rows:
            return None

        pk_name = rows[0]["constraint_name"]
        col_query = """
            SELECT column_name
            FROM all_cons_columns
            WHERE owner = :1 AND constraint_name = :2
            ORDER BY position
        """
        col_rows = self.connection.execute_dict(col_query, (schema_name, pk_name))
        return PrimaryKey(
            name=pk_name,
            columns=[row["column_name"] for row in col_rows],
            is_clustered=False,
        )

    def _get_foreign_keys(self, schema_name: str, table_name: str) -> list[ForeignKey]:
        """Get foreign keys for a table."""
        query = """
            SELECT
                c.constraint_name,
                c.r_owner AS referenced_schema,
                rc.table_name AS referenced_table,
                c.delete_rule AS on_delete
            FROM all_constraints c
            JOIN all_constraints rc ON c.r_constraint_name = rc.constraint_name AND c.r_owner = rc.owner
            WHERE c.owner = :1 AND c.table_name = :2 AND c.constraint_type = 'R'
        """
        fk_rows = self.connection.execute_dict(query, (schema_name, table_name))
        foreign_keys = []

        for fk_row in fk_rows:
            # Get FK columns
            col_query = """
                SELECT column_name
                FROM all_cons_columns
                WHERE owner = :1 AND constraint_name = :2
                ORDER BY position
            """
            fk_cols = self.connection.execute_dict(col_query, (schema_name, fk_row["constraint_name"]))

            # Get referenced columns
            ref_query = """
                SELECT cc.column_name
                FROM all_constraints c
                JOIN all_cons_columns cc ON c.r_constraint_name = cc.constraint_name AND c.r_owner = cc.owner
                WHERE c.owner = :1 AND c.constraint_name = :2
                ORDER BY cc.position
            """
            ref_cols = self.connection.execute_dict(ref_query, (schema_name, fk_row["constraint_name"]))

            foreign_keys.append(
                ForeignKey(
                    name=fk_row["constraint_name"],
                    columns=[row["column_name"] for row in fk_cols],
                    referenced_schema=fk_row["referenced_schema"],
                    referenced_table=fk_row["referenced_table"],
                    referenced_columns=[row["column_name"] for row in ref_cols],
                    on_delete=fk_row["on_delete"] or "NO ACTION",
                    on_update="NO ACTION",  # Oracle doesn't support ON UPDATE
                )
            )
        return foreign_keys

    def _get_indexes(self, schema_name: str, table_name: str) -> list[Index]:
        """Get indexes for a table."""
        query = """
            SELECT
                i.index_name,
                i.uniqueness = 'UNIQUE' AS is_unique,
                i.index_type,
                CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END AS is_primary_key
            FROM all_indexes i
            LEFT JOIN all_constraints c
                ON i.owner = c.owner AND i.index_name = c.index_name AND c.constraint_type = 'P'
            WHERE i.owner = :1 AND i.table_name = :2
            ORDER BY i.index_name
        """
        idx_rows = self.connection.execute_dict(query, (schema_name, table_name))
        indexes = []

        for idx_row in idx_rows:
            col_query = """
                SELECT column_name
                FROM all_ind_columns
                WHERE index_owner = :1 AND index_name = :2
                ORDER BY column_position
            """
            col_rows = self.connection.execute_dict(col_query, (schema_name, idx_row["index_name"]))

            # Get filter definition if any
            filter_query = """
                SELECT index_type FROM all_indexes
                WHERE owner = :1 AND index_name = :2 AND index_type LIKE '%FUNCTION-BASED%'
            """
            filter_rows = self.connection.execute_dict(filter_query, (schema_name, idx_row["index_name"]))
            filter_definition = None
            if filter_rows:
                # For function-based indexes, we could potentially extract the expression
                # but it's complex to parse. For now, we'll just indicate it's function-based
                filter_definition = "FUNCTION-BASED INDEX"

            indexes.append(
                Index(
                    name=idx_row["index_name"],
                    columns=[row["column_name"] for row in col_rows],
                    is_unique=bool(idx_row["is_unique"]),
                    is_primary_key=bool(idx_row["is_primary_key"]),
                    index_type=idx_row["index_type"],
                    filter_definition=filter_definition,
                )
            )
        return indexes

    def _get_check_constraints(self, schema_name: str, table_name: str) -> list[CheckConstraint]:
        """Get check constraints for a table."""
        query = """
            SELECT constraint_name, search_condition AS definition
            FROM all_constraints
            WHERE owner = :1 AND table_name = :2
            AND constraint_type = 'C'
            AND generated = 'USER NAME'
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            CheckConstraint(
                name=row["constraint_name"],
                definition=str(row["definition"]) if row["definition"] else "",
            )
            for row in rows
        ]

    def _get_unique_constraints(self, schema_name: str, table_name: str) -> list[UniqueConstraint]:
        """Get unique constraints for a table."""
        query = """
            SELECT constraint_name
            FROM all_constraints
            WHERE owner = :1 AND table_name = :2 AND constraint_type = 'U'
        """
        constraint_rows = self.connection.execute_dict(query, (schema_name, table_name))
        unique_constraints = []

        for constraint_row in constraint_rows:
            col_query = """
                SELECT column_name
                FROM all_cons_columns
                WHERE owner = :1 AND constraint_name = :2
                ORDER BY position
            """
            col_rows = self.connection.execute_dict(col_query, (schema_name, constraint_row["constraint_name"]))
            unique_constraints.append(
                UniqueConstraint(
                    name=constraint_row["constraint_name"],
                    columns=[row["column_name"] for row in col_rows],
                )
            )
        return unique_constraints

    def _get_partitioning(self, schema_name: str, table_name: str) -> Optional[TablePartitioning]:
        """Get partitioning information for a table."""
        # Check if table is partitioned
        partition_query = """
            SELECT partitioning_type, partition_count, partitioning_key_count
            FROM all_part_tables
            WHERE owner = :1 AND table_name = :2
        """

        partition_rows = self.connection.execute_dict(partition_query, (schema_name, table_name))
        if not partition_rows:
            return TablePartitioning(is_partitioned=False)

        row = partition_rows[0]

        # Get partition key columns
        key_query = """
            SELECT column_name
            FROM all_part_key_columns
            WHERE owner = :1 AND name = :2
            ORDER BY column_position
        """
        key_rows = self.connection.execute_dict(key_query, (schema_name, table_name))
        partition_column = ", ".join([row["column_name"] for row in key_rows])

        # Get partition details
        partitions_query = """
            SELECT
                partition_name,
                partition_position,
                high_value,
                tablespace_name,
                num_rows
            FROM all_tab_partitions
            WHERE table_owner = :1 AND table_name = :2
            ORDER BY partition_position
        """
        partitions_rows = self.connection.execute_dict(partitions_query, (schema_name, table_name))

        partitions = []
        for part_row in partitions_rows:
            partitions.append(Partition(
                partition_number=part_row["partition_position"],
                boundary_value=str(part_row["high_value"]) if part_row["high_value"] else None,
                tablespace_name=part_row["tablespace_name"],
                row_count=part_row["num_rows"] or 0,
            ))

        # Get subpartitions if this is composite partitioning
        subpartitions_query = """
            SELECT
                partition_name,
                subpartition_name,
                subpartition_position,
                high_value,
                tablespace_name,
                num_rows
            FROM all_tab_subpartitions
            WHERE table_owner = :1 AND table_name = :2
            ORDER BY partition_name, subpartition_position
        """
        subpartitions_rows = self.connection.execute_dict(subpartitions_query, (schema_name, table_name))

        # Add subpartitions as additional partitions
        for subpart_row in subpartitions_rows:
            partitions.append(Partition(
                partition_number=len(partitions) + 1,
                boundary_value=f"{subpart_row['partition_name']}: {subpart_row['high_value']}",
                tablespace_name=subpart_row["tablespace_name"],
                row_count=subpart_row["num_rows"] or 0,
            ))

        partition_scheme = PartitionScheme(
            name=f"{table_name}_partitioning",
            partition_column=partition_column,
            partition_type=row["partitioning_type"],
            partitions=partitions,
        )

        return TablePartitioning(
            partition_scheme=partition_scheme,
            is_partitioned=True,
        )

    def _get_table_triggers(self, schema_name: str, table_name: str) -> list[Trigger]:
        """Get triggers for a table."""
        query = """
            SELECT
                trigger_name,
                trigger_type,
                triggering_event AS events,
                trigger_body AS definition,
                status = 'DISABLED' AS is_disabled
            FROM all_triggers
            WHERE table_owner = :1 AND table_name = :2
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        triggers = []

        for row in rows:
            # Parse trigger type
            trigger_type = row["trigger_type"]
            if "BEFORE" in trigger_type.upper():
                timing = "BEFORE"
            elif "AFTER" in trigger_type.upper():
                timing = "AFTER"
            elif "INSTEAD OF" in trigger_type.upper():
                timing = "INSTEAD OF"
            else:
                timing = trigger_type

            # Parse events
            events = [e.strip() for e in row["events"].upper().split(" OR ")]

            triggers.append(
                Trigger(
                    schema_name=schema_name,
                    name=row["trigger_name"],
                    parent_table_schema=schema_name,
                    parent_table_name=table_name,
                    trigger_type=timing,
                    events=events,
                    definition=row["definition"],
                    is_disabled=bool(row["is_disabled"]),
                )
            )
        return triggers

    def _get_description(self, schema_name: str, table_name: str) -> Optional[str]:
        """Get table description."""
        query = """
            SELECT comments
            FROM all_tab_comments
            WHERE owner = :1 AND table_name = :2
        """
        return self.connection.execute_scalar(query, (schema_name, table_name))

    def _get_table_stats(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Get row count and space statistics."""
        query = """
            SELECT
                NVL(num_rows, 0) AS row_count,
                NVL(blocks * 8, 0) AS total_space_kb
            FROM all_tables
            WHERE owner = :1 AND table_name = :2
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        if rows:
            return {"row_count": rows[0]["row_count"], "total_space_kb": rows[0]["total_space_kb"]}
        return {"row_count": 0, "total_space_kb": 0}

    def _build_references(self, tables: list[Table]) -> None:
        """Build the referenced_by list for each table."""
        query = """
            SELECT
                c.owner AS parent_schema,
                c.table_name AS parent_table,
                c.constraint_name AS fk_name,
                c.r_owner AS referenced_schema,
                rc.table_name AS referenced_table
            FROM all_constraints c
            JOIN all_constraints rc ON c.r_constraint_name = rc.constraint_name AND c.r_owner = rc.owner
            WHERE c.constraint_type = 'R'
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
    """Extracts view metadata from Oracle."""

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
            SELECT owner AS schema_name, view_name
            FROM all_views
            WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                               'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                               'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS')
            ORDER BY owner, view_name
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
                data_length AS max_length,
                data_precision AS precision,
                data_scale AS scale,
                CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END AS is_nullable,
                column_id AS ordinal_position
            FROM all_tab_columns
            WHERE owner = :1 AND table_name = :2
            ORDER BY column_id
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
        query = "SELECT text FROM all_views WHERE owner = :1 AND view_name = :2"
        return self.connection.execute_scalar(query, (schema_name, view_name))

    def _get_description(self, schema_name: str, view_name: str) -> Optional[str]:
        """Get view description."""
        query = "SELECT comments FROM all_tab_comments WHERE owner = :1 AND table_name = :2"
        return self.connection.execute_scalar(query, (schema_name, view_name))


class ProcedureExtractor(BaseExtractor):
    """Extracts stored procedure metadata from Oracle."""

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
            SELECT owner AS schema_name, object_name AS procedure_name
            FROM all_procedures
            WHERE object_type = 'PROCEDURE'
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                             'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                             'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS')
            ORDER BY owner, object_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Procedure(schema_name=row["schema_name"], name=row["procedure_name"], language="PL/SQL")
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, proc_name: str) -> list[Parameter]:
        """Get parameters for a procedure."""
        query = """
            SELECT
                argument_name,
                data_type,
                data_length AS max_length,
                data_precision AS precision,
                data_scale AS scale,
                in_out IN ('OUT', 'IN/OUT') AS is_output,
                default_value IS NOT NULL AS has_default,
                default_value,
                position AS ordinal_position
            FROM all_arguments
            WHERE owner = :1 AND object_name = :2
            AND argument_name IS NOT NULL
            ORDER BY position
        """
        rows = self.connection.execute_dict(query, (schema_name, proc_name))
        return [
            Parameter(
                name=row["argument_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_output=bool(row["is_output"]),
                has_default=bool(row["has_default"]),
                default_value=str(row["default_value"]) if row["default_value"] else None,
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, proc_name: str) -> Optional[str]:
        """Get procedure definition."""
        query = """
            SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY line) AS definition
            FROM all_source
            WHERE owner = :1 AND name = :2 AND type = 'PROCEDURE'
        """
        return self.connection.execute_scalar(query, (schema_name, proc_name))


class FunctionExtractor(BaseExtractor):
    """Extracts function metadata from Oracle."""

    def extract(self) -> list[Function]:
        """Extract all functions."""
        functions = self._get_functions()
        logger.info(f"Found {len(functions)} functions")

        for func in functions:
            func.parameters = self._get_parameters(func.schema_name, func.name)
            func.definition = self._get_definition(func.schema_name, func.name)
            func.return_type = self._get_return_type(func.schema_name, func.name)

        return functions

    def _get_functions(self) -> list[Function]:
        """Get list of all functions."""
        query = """
            SELECT owner AS schema_name, object_name AS function_name
            FROM all_procedures
            WHERE object_type = 'FUNCTION'
            AND owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                             'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                             'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS')
            ORDER BY owner, object_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Function(
                schema_name=row["schema_name"],
                name=row["function_name"],
                function_type="SCALAR",
                language="PL/SQL",
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, func_name: str) -> list[Parameter]:
        """Get parameters for a function."""
        query = """
            SELECT
                argument_name,
                data_type,
                data_length AS max_length,
                data_precision AS precision,
                data_scale AS scale,
                in_out IN ('OUT', 'IN/OUT') AS is_output,
                position AS ordinal_position
            FROM all_arguments
            WHERE owner = :1 AND object_name = :2
            AND argument_name IS NOT NULL
            AND in_out != 'OUT'
            ORDER BY position
        """
        rows = self.connection.execute_dict(query, (schema_name, func_name))
        return [
            Parameter(
                name=row["argument_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_output=bool(row["is_output"]),
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, func_name: str) -> Optional[str]:
        """Get function definition."""
        query = """
            SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY line) AS definition
            FROM all_source
            WHERE owner = :1 AND name = :2 AND type = 'FUNCTION'
        """
        return self.connection.execute_scalar(query, (schema_name, func_name))

    def _get_return_type(self, schema_name: str, func_name: str) -> Optional[str]:
        """Get function return type."""
        query = """
            SELECT data_type
            FROM all_arguments
            WHERE owner = :1 AND object_name = :2 AND position = 0
        """
        return self.connection.execute_scalar(query, (schema_name, func_name))


class TriggerExtractor(BaseExtractor):
    """Extracts trigger metadata from Oracle."""

    def extract(self) -> list[Trigger]:
        """Extract all triggers."""
        triggers = self._get_triggers()
        logger.info(f"Found {len(triggers)} triggers")
        return triggers

    def _get_triggers(self) -> list[Trigger]:
        """Get list of all triggers."""
        query = """
            SELECT
                owner AS schema_name,
                trigger_name,
                table_owner AS parent_schema,
                table_name AS parent_table,
                trigger_type,
                triggering_event AS events,
                trigger_body AS definition,
                status = 'DISABLED' AS is_disabled
            FROM all_triggers
            WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                               'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                               'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS')
            ORDER BY owner, trigger_name
        """
        rows = self.connection.execute_dict(query)
        triggers = []

        for row in rows:
            if not self._should_include_schema(row["schema_name"]):
                continue
            # Parse trigger type
            trigger_type = row["trigger_type"]
            if "BEFORE" in trigger_type.upper():
                timing = "BEFORE"
            elif "AFTER" in trigger_type.upper():
                timing = "AFTER"
            elif "INSTEAD OF" in trigger_type.upper():
                timing = "INSTEAD OF"
            else:
                timing = trigger_type

            # Parse events
            events = [e.strip() for e in row["events"].upper().split(" OR ")]

            triggers.append(
                Trigger(
                    schema_name=row["schema_name"],
                    name=row["trigger_name"],
                    parent_table_schema=row["parent_schema"],
                    parent_table_name=row["parent_table"],
                    trigger_type=timing,
                    events=events,
                    definition=row["definition"],
                    is_disabled=bool(row["is_disabled"]),
                )
            )
        return triggers


class TypeExtractor(BaseExtractor):
    """Extracts user-defined type metadata from Oracle."""

    def extract(self) -> list[UserDefinedType]:
        """Extract all user-defined types."""
        types = self._get_types()
        logger.info(f"Found {len(types)} user-defined types")

        for udt in types:
            if udt.type_category == "OBJECT":
                udt.columns = self._get_object_attributes(udt.schema_name, udt.name)

        return types

    def _get_types(self) -> list[UserDefinedType]:
        """Get list of all user-defined types."""
        query = """
            SELECT
                owner AS schema_name,
                type_name,
                typecode AS type_category
            FROM all_types
            WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                               'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                               'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS')
            ORDER BY owner, type_name
        """
        rows = self.connection.execute_dict(query)
        return [
            UserDefinedType(
                schema_name=row["schema_name"],
                name=row["type_name"],
                type_category=row["type_category"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_object_attributes(self, schema_name: str, type_name: str) -> list[TypeColumn]:
        """Get attributes for an object type."""
        query = """
            SELECT
                attr_name AS column_name,
                attr_type_name AS data_type,
                length AS max_length,
                precision,
                scale,
                attr_no AS ordinal_position
            FROM all_type_attrs
            WHERE owner = :1 AND type_name = :2
            ORDER BY attr_no
        """
        rows = self.connection.execute_dict(query, (schema_name, type_name))
        return [
            TypeColumn(
                name=row["column_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]


class SequenceExtractor(BaseExtractor):
    """Extracts sequence metadata from Oracle."""

    def extract(self) -> list[Sequence]:
        """Extract all sequences."""
        sequences = self._get_sequences()
        logger.info(f"Found {len(sequences)} sequences")
        return sequences

    def _get_sequences(self) -> list[Sequence]:
        """Get list of all sequences."""
        query = """
            SELECT
                sequence_owner AS schema_name,
                sequence_name,
                min_value,
                max_value,
                increment_by AS increment_val,
                CASE WHEN cycle_flag = 'Y' THEN 1 ELSE 0 END AS is_cycling,
                cache_size,
                last_number AS current_value
            FROM all_sequences
            WHERE sequence_owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                                        'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                                        'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS')
            ORDER BY sequence_owner, sequence_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Sequence(
                schema_name=row["schema_name"],
                name=row["sequence_name"],
                data_type="NUMBER",
                start_value=row["min_value"],
                increment=row["increment_val"],
                min_value=row["min_value"],
                max_value=row["max_value"],
                is_cycling=bool(row["is_cycling"]),
                cache_size=row["cache_size"],
                current_value=row["current_value"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]


class SynonymExtractor(BaseExtractor):
    """Extracts synonym metadata from Oracle."""

    def extract(self) -> list[Synonym]:
        """Extract all synonyms."""
        synonyms = self._get_synonyms()
        logger.info(f"Found {len(synonyms)} synonyms")
        return synonyms

    def _get_synonyms(self) -> list[Synonym]:
        """Get list of all synonyms."""
        query = """
            SELECT
                owner AS schema_name,
                synonym_name,
                table_owner AS target_schema,
                table_name AS target_object,
                db_link AS target_database
            FROM all_synonyms
            WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM',
                               'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS',
                               'XDB', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS', 'PUBLIC')
            ORDER BY owner, synonym_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Synonym(
                schema_name=row["schema_name"],
                name=row["synonym_name"],
                base_object_name=f"{row['target_schema']}.{row['target_object']}",
                target_schema=row["target_schema"],
                target_object=row["target_object"],
                target_database=row["target_database"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]
