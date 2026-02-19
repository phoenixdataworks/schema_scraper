"""MS SQL Server schema extractors."""

import logging
import re
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
    Permission,
    PrimaryKey,
    Procedure,
    Role,
    RoleMembership,
    Sequence,
    Synonym,
    Table,
    TablePartitioning,
    Trigger,
    TypeColumn,
    UniqueConstraint,
    User,
    UserDefinedType,
    View,
)

logger = logging.getLogger(__name__)


class MSSQLBaseExtractor(BaseExtractor):
    """Base extractor with MSSQL-specific helpers."""

    def get_extended_property(
        self,
        schema_name: str,
        object_name: str,
        column_name: Optional[str] = None,
    ) -> Optional[str]:
        """Get MS_Description extended property for an object."""
        if column_name:
            query = """
                SELECT CAST(ep.value AS NVARCHAR(MAX))
                FROM sys.extended_properties ep
                JOIN sys.objects o ON ep.major_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
                WHERE ep.name = 'MS_Description'
                AND s.name = ?
                AND o.name = ?
                AND c.name = ?
            """
            return self.connection.execute_scalar(query, (schema_name, object_name, column_name))
        else:
            query = """
                SELECT CAST(ep.value AS NVARCHAR(MAX))
                FROM sys.extended_properties ep
                JOIN sys.objects o ON ep.major_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE ep.name = 'MS_Description'
                AND ep.minor_id = 0
                AND s.name = ?
                AND o.name = ?
            """
            return self.connection.execute_scalar(query, (schema_name, object_name))


class TableExtractor(MSSQLBaseExtractor):
    """Extracts table metadata from SQL Server."""

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
            table.description = self.get_extended_property(table.schema_name, table.name)
            stats = self._get_table_stats(table.schema_name, table.name)
            table.row_count = stats.get("row_count", 0)
            table.total_space_kb = stats.get("total_space_kb", 0)
            table.used_space_kb = stats.get("used_space_kb", 0)

        self._build_references(tables)
        return tables

    def _get_tables(self) -> list[Table]:
        """Get list of all tables."""
        query = """
            SELECT s.name AS schema_name, t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.is_ms_shipped = 0
            ORDER BY s.name, t.name
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
                c.name AS column_name, t.name AS data_type,
                c.max_length, c.precision, c.scale, c.is_nullable,
                dc.definition AS default_value, c.is_identity,
                CAST(ic.seed_value AS BIGINT) AS identity_seed,
                CAST(ic.increment_value AS BIGINT) AS identity_increment,
                c.is_computed, cc.definition AS computed_definition,
                c.collation_name, c.column_id AS ordinal_position
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            JOIN sys.tables tb ON c.object_id = tb.object_id
            JOIN sys.schemas s ON tb.schema_id = s.schema_id
            LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
            LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
            WHERE s.name = ? AND tb.name = ?
            ORDER BY c.column_id
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        columns = []
        for row in rows:
            col = Column(
                name=row["column_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_nullable=row["is_nullable"],
                default_value=row["default_value"],
                is_identity=row["is_identity"],
                identity_seed=row["identity_seed"],
                identity_increment=row["identity_increment"],
                is_computed=row["is_computed"],
                computed_definition=row["computed_definition"],
                collation=row["collation_name"],
                ordinal_position=row["ordinal_position"],
            )
            col.description = self.get_extended_property(schema_name, table_name, row["column_name"])
            columns.append(col)
        return columns

    def _get_primary_key(self, schema_name: str, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key for a table."""
        query = """
            SELECT kc.name AS constraint_name, i.type_desc AS index_type
            FROM sys.key_constraints kc
            JOIN sys.tables t ON kc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id
            WHERE kc.type = 'PK' AND s.name = ? AND t.name = ?
        """
        pk_row = self.connection.execute_dict(query, (schema_name, table_name))
        if not pk_row:
            return None

        pk_info = pk_row[0]
        columns_query = """
            SELECT c.name
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE i.is_primary_key = 1 AND s.name = ? AND t.name = ?
            ORDER BY ic.key_ordinal
        """
        col_rows = self.connection.execute_dict(columns_query, (schema_name, table_name))
        return PrimaryKey(
            name=pk_info["constraint_name"],
            columns=[row["name"] for row in col_rows],
            is_clustered=pk_info["index_type"] == "CLUSTERED",
        )

    def _get_foreign_keys(self, schema_name: str, table_name: str) -> list[ForeignKey]:
        """Get foreign keys for a table."""
        query = """
            SELECT
                fk.name AS fk_name, rs.name AS referenced_schema, rt.name AS referenced_table,
                fk.delete_referential_action_desc AS on_delete,
                fk.update_referential_action_desc AS on_update
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
            WHERE s.name = ? AND t.name = ?
        """
        fk_rows = self.connection.execute_dict(query, (schema_name, table_name))
        foreign_keys = []

        for fk_row in fk_rows:
            columns_query = """
                SELECT pc.name AS parent_column, rc.name AS referenced_column
                FROM sys.foreign_key_columns fkc
                JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
                JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
                JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id
                WHERE fk.name = ?
                ORDER BY fkc.constraint_column_id
            """
            col_rows = self.connection.execute_dict(columns_query, (fk_row["fk_name"],))
            foreign_keys.append(
                ForeignKey(
                    name=fk_row["fk_name"],
                    columns=[row["parent_column"] for row in col_rows],
                    referenced_schema=fk_row["referenced_schema"],
                    referenced_table=fk_row["referenced_table"],
                    referenced_columns=[row["referenced_column"] for row in col_rows],
                    on_delete=fk_row["on_delete"].replace("_", " "),
                    on_update=fk_row["on_update"].replace("_", " "),
                )
            )
        return foreign_keys

    def _get_indexes(self, schema_name: str, table_name: str) -> list[Index]:
        """Get indexes for a table."""
        query = """
            SELECT
                i.name AS index_name, i.is_unique, i.type_desc AS index_type,
                i.is_primary_key, i.filter_definition
            FROM sys.indexes i
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ? AND i.name IS NOT NULL
            ORDER BY i.index_id
        """
        idx_rows = self.connection.execute_dict(query, (schema_name, table_name))
        indexes = []

        for idx_row in idx_rows:
            columns_query = """
                SELECT c.name, ic.is_included_column
                FROM sys.index_columns ic
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                JOIN sys.tables t ON i.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE i.name = ? AND s.name = ? AND t.name = ?
                ORDER BY ic.key_ordinal, ic.index_column_id
            """
            col_rows = self.connection.execute_dict(
                columns_query, (idx_row["index_name"], schema_name, table_name)
            )
            indexes.append(
                Index(
                    name=idx_row["index_name"],
                    columns=[row["name"] for row in col_rows if not row["is_included_column"]],
                    is_unique=idx_row["is_unique"],
                    is_clustered=idx_row["index_type"] == "CLUSTERED",
                    is_primary_key=idx_row["is_primary_key"],
                    included_columns=[row["name"] for row in col_rows if row["is_included_column"]],
                    filter_definition=idx_row["filter_definition"],
                    index_type=idx_row["index_type"],
                )
            )
        return indexes

    def _get_check_constraints(self, schema_name: str, table_name: str) -> list[CheckConstraint]:
        """Get check constraints for a table."""
        query = """
            SELECT cc.name AS constraint_name, cc.definition
            FROM sys.check_constraints cc
            JOIN sys.tables t ON cc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [CheckConstraint(name=row["constraint_name"], definition=row["definition"]) for row in rows]

    def _get_unique_constraints(self, schema_name: str, table_name: str) -> list[UniqueConstraint]:
        """Get unique constraints for a table."""
        query = """
            SELECT kc.name AS constraint_name
            FROM sys.key_constraints kc
            JOIN sys.tables t ON kc.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE kc.type = 'UQ' AND s.name = ? AND t.name = ?
        """
        constraint_rows = self.connection.execute_dict(query, (schema_name, table_name))
        unique_constraints = []

        for constraint_row in constraint_rows:
            columns_query = """
                SELECT c.name
                FROM sys.index_columns ic
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.key_constraints kc ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
                JOIN sys.tables t ON kc.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE kc.name = ? AND s.name = ? AND t.name = ?
                ORDER BY ic.key_ordinal
            """
            col_rows = self.connection.execute_dict(
                columns_query, (constraint_row["constraint_name"], schema_name, table_name)
            )
            unique_constraints.append(
                UniqueConstraint(
                    name=constraint_row["constraint_name"],
                    columns=[row["name"] for row in col_rows],
                )
            )
        return unique_constraints

    def _get_partitioning(self, schema_name: str, table_name: str) -> Optional[TablePartitioning]:
        """Get partitioning information for a table."""
        # Check if table is partitioned
        partition_check_query = """
            SELECT ps.name AS partition_scheme_name, pf.name AS partition_function_name,
                   pf.fanout AS partition_count, pf.boundary_value_on_right
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
            WHERE s.name = ? AND t.name = ?
        """

        partition_rows = self.connection.execute_dict(partition_check_query, (schema_name, table_name))
        if not partition_rows:
            return TablePartitioning(is_partitioned=False)

        row = partition_rows[0]

        # Get partition column
        column_query = """
            SELECT c.name AS column_name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            JOIN sys.partition_parameters pp ON ps.function_id = pp.function_id
            JOIN sys.columns c ON pp.object_id = c.object_id AND pp.parameter_id = c.column_id
            WHERE s.name = ? AND t.name = ?
        """
        column_rows = self.connection.execute_dict(column_query, (schema_name, table_name))
        partition_column = column_rows[0]["column_name"] if column_rows else ""

        # Get partition boundaries
        boundary_query = """
            SELECT prv.boundary_id, prv.value AS boundary_value, fg.name AS filegroup_name
            FROM sys.partition_range_values prv
            JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
            JOIN sys.partition_schemes ps ON pf.function_id = ps.function_id
            JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
                   AND prv.boundary_id = dds.destination_id
            JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
            WHERE pf.name = ?
            ORDER BY prv.boundary_id
        """
        boundary_rows = self.connection.execute_dict(boundary_query, (row["partition_function_name"],))

        # Get partition statistics
        partition_stats_query = """
            SELECT p.partition_number, p.rows AS row_count, fg.name AS filegroup_name,
                   p.data_compression_desc AS data_compression
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
            JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
                   AND i.data_space_id = dds.partition_scheme_id
            JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
            WHERE s.name = ? AND t.name = ?
            ORDER BY p.partition_number
        """
        stats_rows = self.connection.execute_dict(partition_stats_query, (schema_name, table_name))

        # Build partitions list
        partitions = []
        stats_dict = {row["partition_number"]: row for row in stats_rows}

        for boundary_row in boundary_rows:
            partition_num = boundary_row["boundary_id"] + 1  # SQL Server partitions are 1-based
            stats = stats_dict.get(partition_num, {})

            partitions.append(Partition(
                partition_number=partition_num,
                boundary_value=str(boundary_row["boundary_value"]) if boundary_row["boundary_value"] else None,
                filegroup_name=boundary_row["filegroup_name"],
                row_count=stats.get("row_count", 0),
                data_compression=stats.get("data_compression"),
            ))

        # Handle the last partition (no boundary)
        if stats_rows and len(stats_rows) > len(boundary_rows):
            last_partition = stats_rows[-1]
            if last_partition["partition_number"] > len(boundary_rows):
                partitions.append(Partition(
                    partition_number=last_partition["partition_number"],
                    filegroup_name=last_partition["filegroup_name"],
                    row_count=last_partition["row_count"],
                    data_compression=last_partition["data_compression"],
                ))

        partition_scheme = PartitionScheme(
            name=row["partition_scheme_name"],
            partition_function_name=row["partition_function_name"],
            partition_column=partition_column,
            partition_type="RANGE",
            boundary_type="RIGHT" if row["boundary_value_on_right"] else "LEFT",
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
                tr.name AS trigger_name,
                CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END AS trigger_type,
                OBJECTPROPERTY(tr.object_id, 'ExecIsInsertTrigger') AS is_insert,
                OBJECTPROPERTY(tr.object_id, 'ExecIsUpdateTrigger') AS is_update,
                OBJECTPROPERTY(tr.object_id, 'ExecIsDeleteTrigger') AS is_delete,
                tr.is_disabled
            FROM sys.triggers tr
            JOIN sys.tables pt ON tr.parent_id = pt.object_id
            JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
            WHERE tr.is_ms_shipped = 0 AND tr.parent_class = 1
            AND ps.name = ? AND pt.name = ?
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        triggers = []

        for row in rows:
            events = []
            if row["is_insert"]:
                events.append("INSERT")
            if row["is_update"]:
                events.append("UPDATE")
            if row["is_delete"]:
                events.append("DELETE")

            # Get trigger definition
            def_query = """
                SELECT m.definition
                FROM sys.sql_modules m
                JOIN sys.triggers tr ON m.object_id = tr.object_id
                WHERE tr.name = ?
            """
            def_rows = self.connection.execute_dict(def_query, (row["trigger_name"],))
            definition = def_rows[0]["definition"] if def_rows else None

            triggers.append(
                Trigger(
                    schema_name=schema_name,
                    name=row["trigger_name"],
                    parent_table_schema=schema_name,
                    parent_table_name=table_name,
                    trigger_type=row["trigger_type"],
                    events=events,
                    definition=definition,
                    is_disabled=bool(row["is_disabled"]),
                )
            )
        return triggers

    def _get_table_stats(self, schema_name: str, table_name: str) -> dict[str, Any]:
        """Get row count and space statistics for a table."""
        query = """
            SELECT
                SUM(p.rows) AS row_count,
                SUM(a.total_pages) * 8 AS total_space_kb,
                SUM(a.used_pages) * 8 AS used_space_kb
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON t.object_id = i.object_id
            JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE s.name = ? AND t.name = ? AND i.index_id IN (0, 1)
            GROUP BY t.object_id
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        if rows:
            return {
                "row_count": rows[0]["row_count"] or 0,
                "total_space_kb": rows[0]["total_space_kb"] or 0,
                "used_space_kb": rows[0]["used_space_kb"] or 0,
            }
        return {"row_count": 0, "total_space_kb": 0, "used_space_kb": 0}

    def _build_references(self, tables: list[Table]) -> None:
        """Build the referenced_by list for each table."""
        query = """
            SELECT
                ps.name AS parent_schema, pt.name AS parent_table, fk.name AS fk_name,
                rs.name AS referenced_schema, rt.name AS referenced_table
            FROM sys.foreign_keys fk
            JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
            JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
        """
        rows = self.connection.execute_dict(query)
        table_map = {(t.schema_name, t.name): t for t in tables}

        for row in rows:
            ref_key = (row["referenced_schema"], row["referenced_table"])
            if ref_key in table_map:
                table_map[ref_key].referenced_by.append(
                    (row["parent_schema"], row["parent_table"], row["fk_name"])
                )


class ViewExtractor(MSSQLBaseExtractor):
    """Extracts view metadata from SQL Server."""

    def extract(self) -> list[View]:
        """Extract all views with their metadata."""
        views = self._get_views()
        logger.info(f"Found {len(views)} views")

        for view in views:
            view.columns = self._get_columns(view.schema_name, view.name)
            view.definition = self._get_definition(view.schema_name, view.name)
            view.description = self.get_extended_property(view.schema_name, view.name)
            view.base_tables = self._get_base_tables(view.schema_name, view.name)

        return views

    def _get_views(self) -> list[View]:
        """Get list of all views."""
        query = """
            SELECT s.name AS schema_name, v.name AS view_name,
                   OBJECTPROPERTY(v.object_id, 'IsSchemaBound') AS is_schema_bound
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE v.is_ms_shipped = 0
            ORDER BY s.name, v.name
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
                c.name AS column_name, t.name AS data_type,
                c.max_length, c.precision, c.scale, c.is_nullable,
                c.collation_name, c.column_id AS ordinal_position
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            JOIN sys.views v ON c.object_id = v.object_id
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = ? AND v.name = ?
            ORDER BY c.column_id
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
                collation=row["collation_name"],
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, view_name: str) -> Optional[str]:
        """Get the full SQL definition of a view."""
        query = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.views v ON m.object_id = v.object_id
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = ? AND v.name = ?
        """
        return self.connection.execute_scalar(query, (schema_name, view_name))

    def _get_base_tables(self, schema_name: str, view_name: str) -> list[str]:
        """Get the base tables referenced by a view."""
        query = """
            SELECT DISTINCT SCHEMA_NAME(o.schema_id) + '.' + o.name AS table_name
            FROM sys.sql_expression_dependencies d
            JOIN sys.views v ON d.referencing_id = v.object_id
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            JOIN sys.objects o ON d.referenced_id = o.object_id
            WHERE s.name = ? AND v.name = ? AND o.type IN ('U', 'V')
            ORDER BY table_name
        """
        rows = self.connection.execute_dict(query, (schema_name, view_name))
        return [row["table_name"] for row in rows]


class ProcedureExtractor(MSSQLBaseExtractor):
    """Extracts stored procedure metadata from SQL Server."""

    def extract(self) -> list[Procedure]:
        """Extract all stored procedures with their metadata."""
        procedures = self._get_procedures()
        logger.info(f"Found {len(procedures)} stored procedures")

        for proc in procedures:
            proc.parameters = self._get_parameters(proc.schema_name, proc.name)
            proc.definition = self._get_definition(proc.schema_name, proc.name)
            proc.description = self.get_extended_property(proc.schema_name, proc.name)

        return procedures

    def _get_procedures(self) -> list[Procedure]:
        """Get list of all stored procedures."""
        query = """
            SELECT s.name AS schema_name, p.name AS procedure_name
            FROM sys.procedures p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE p.is_ms_shipped = 0 AND p.type = 'P'
            ORDER BY s.name, p.name
        """
        rows = self.connection.execute_dict(query)
        return [
            Procedure(schema_name=row["schema_name"], name=row["procedure_name"], language="T-SQL")
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, procedure_name: str) -> list[Parameter]:
        """Get parameters for a stored procedure."""
        query = """
            SELECT
                p.name AS parameter_name, t.name AS data_type,
                p.max_length, p.precision, p.scale, p.is_output,
                p.has_default_value, p.default_value, p.parameter_id AS ordinal_position
            FROM sys.parameters p
            JOIN sys.types t ON p.user_type_id = t.user_type_id
            JOIN sys.procedures pr ON p.object_id = pr.object_id
            JOIN sys.schemas s ON pr.schema_id = s.schema_id
            WHERE s.name = ? AND pr.name = ? AND p.parameter_id > 0
            ORDER BY p.parameter_id
        """
        rows = self.connection.execute_dict(query, (schema_name, procedure_name))
        return [
            Parameter(
                name=row["parameter_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_output=row["is_output"],
                has_default=row["has_default_value"],
                default_value=str(row["default_value"]) if row["default_value"] is not None else None,
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, procedure_name: str) -> Optional[str]:
        """Get the full SQL definition of a stored procedure."""
        query = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.procedures p ON m.object_id = p.object_id
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE s.name = ? AND p.name = ?
        """
        return self.connection.execute_scalar(query, (schema_name, procedure_name))


class FunctionExtractor(MSSQLBaseExtractor):
    """Extracts user-defined function metadata from SQL Server."""

    def extract(self) -> list[Function]:
        """Extract all user-defined functions with their metadata."""
        functions = self._get_functions()
        logger.info(f"Found {len(functions)} user-defined functions")

        for func in functions:
            func.parameters = self._get_parameters(func.schema_name, func.name)
            func.definition = self._get_definition(func.schema_name, func.name)
            func.description = self.get_extended_property(func.schema_name, func.name)

            if func.function_type == "SCALAR":
                func.return_type = self._get_return_type(func.schema_name, func.name)
            else:
                func.return_columns = self._get_return_columns(func.schema_name, func.name)

        return functions

    def _get_functions(self) -> list[Function]:
        """Get list of all user-defined functions."""
        query = """
            SELECT
                s.name AS schema_name, o.name AS function_name,
                CASE o.type
                    WHEN 'FN' THEN 'SCALAR'
                    WHEN 'IF' THEN 'INLINE_TABLE_VALUED'
                    WHEN 'TF' THEN 'TABLE_VALUED'
                    ELSE 'UNKNOWN'
                END AS function_type
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('FN', 'IF', 'TF') AND o.is_ms_shipped = 0
            ORDER BY s.name, o.name
        """
        rows = self.connection.execute_dict(query)
        return [
            Function(
                schema_name=row["schema_name"],
                name=row["function_name"],
                function_type=row["function_type"],
                language="T-SQL",
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_parameters(self, schema_name: str, function_name: str) -> list[Parameter]:
        """Get parameters for a function."""
        query = """
            SELECT
                p.name AS parameter_name, t.name AS data_type,
                p.max_length, p.precision, p.scale, p.is_output,
                p.has_default_value, p.default_value, p.parameter_id AS ordinal_position
            FROM sys.parameters p
            JOIN sys.types t ON p.user_type_id = t.user_type_id
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = ? AND o.name = ? AND p.parameter_id > 0
            ORDER BY p.parameter_id
        """
        rows = self.connection.execute_dict(query, (schema_name, function_name))
        return [
            Parameter(
                name=row["parameter_name"],
                data_type=row["data_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_output=row["is_output"],
                has_default=row["has_default_value"],
                default_value=str(row["default_value"]) if row["default_value"] is not None else None,
                ordinal_position=row["ordinal_position"],
            )
            for row in rows
        ]

    def _get_definition(self, schema_name: str, function_name: str) -> Optional[str]:
        """Get the full SQL definition of a function."""
        query = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON m.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = ? AND o.name = ?
        """
        return self.connection.execute_scalar(query, (schema_name, function_name))

    def _get_return_type(self, schema_name: str, function_name: str) -> Optional[str]:
        """Get the return type for a scalar function."""
        query = """
            SELECT t.name AS data_type, p.max_length, p.precision, p.scale
            FROM sys.parameters p
            JOIN sys.types t ON p.user_type_id = t.user_type_id
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = ? AND o.name = ? AND p.parameter_id = 0
        """
        rows = self.connection.execute_dict(query, (schema_name, function_name))
        if not rows:
            return None
        row = rows[0]
        data_type = row["data_type"]
        if data_type in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary"):
            length = "max" if row["max_length"] == -1 else str(row["max_length"])
            if data_type.startswith("n") and row["max_length"] and row["max_length"] > 0:
                length = str(row["max_length"] // 2)
            return f"{data_type}({length})"
        elif data_type in ("decimal", "numeric"):
            return f"{data_type}({row['precision']},{row['scale']})"
        return data_type

    def _get_return_columns(self, schema_name: str, function_name: str) -> list[FunctionColumn]:
        """Get return columns for a table-valued function."""
        query = """
            SELECT
                c.name AS column_name, t.name AS data_type,
                c.max_length, c.precision, c.scale, c.is_nullable,
                c.column_id AS ordinal_position
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            JOIN sys.objects o ON c.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = ? AND o.name = ?
            ORDER BY c.column_id
        """
        rows = self.connection.execute_dict(query, (schema_name, function_name))
        return [
            FunctionColumn(
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


class TriggerExtractor(MSSQLBaseExtractor):
    """Extracts trigger metadata from SQL Server."""

    def extract(self) -> list[Trigger]:
        """Extract all DML triggers with their metadata."""
        triggers = self._get_triggers()
        logger.info(f"Found {len(triggers)} triggers")

        for trigger in triggers:
            trigger.definition = self._get_definition(trigger.schema_name, trigger.name)
            trigger.description = self.get_extended_property(trigger.schema_name, trigger.name)

        return triggers

    def _get_triggers(self) -> list[Trigger]:
        """Get list of all DML triggers."""
        query = """
            SELECT
                s.name AS schema_name, tr.name AS trigger_name,
                ps.name AS parent_schema, pt.name AS parent_table,
                CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END AS trigger_type,
                tr.is_disabled,
                OBJECTPROPERTY(tr.object_id, 'ExecIsInsertTrigger') AS is_insert,
                OBJECTPROPERTY(tr.object_id, 'ExecIsUpdateTrigger') AS is_update,
                OBJECTPROPERTY(tr.object_id, 'ExecIsDeleteTrigger') AS is_delete
            FROM sys.triggers tr
            JOIN sys.tables pt ON tr.parent_id = pt.object_id
            JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
            JOIN sys.schemas s ON pt.schema_id = s.schema_id
            WHERE tr.is_ms_shipped = 0 AND tr.parent_class = 1
            ORDER BY s.name, tr.name
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
                    is_disabled=row["is_disabled"],
                )
            )
        return triggers

    def _get_definition(self, schema_name: str, trigger_name: str) -> Optional[str]:
        """Get the full SQL definition of a trigger."""
        query = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.triggers tr ON m.object_id = tr.object_id
            JOIN sys.tables t ON tr.parent_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND tr.name = ?
        """
        return self.connection.execute_scalar(query, (schema_name, trigger_name))


class TypeExtractor(MSSQLBaseExtractor):
    """Extracts user-defined type metadata from SQL Server."""

    def extract(self) -> list[UserDefinedType]:
        """Extract all user-defined types with their metadata."""
        types = self._get_types()
        logger.info(f"Found {len(types)} user-defined types")

        for udt in types:
            if udt.type_category == "TABLE_TYPE":
                udt.columns = self._get_table_type_columns(udt.schema_name, udt.name)

        return types

    def _get_types(self) -> list[UserDefinedType]:
        """Get list of all user-defined types."""
        query = """
            SELECT
                s.name AS schema_name, t.name AS type_name,
                CASE
                    WHEN t.is_table_type = 1 THEN 'TABLE_TYPE'
                    WHEN t.is_assembly_type = 1 THEN 'CLR_TYPE'
                    ELSE 'ALIAS_TYPE'
                END AS type_category,
                bt.name AS base_type, t.max_length, t.precision, t.scale, t.is_nullable
            FROM sys.types t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.types bt ON t.system_type_id = bt.user_type_id AND bt.is_user_defined = 0
            WHERE t.is_user_defined = 1
            ORDER BY s.name, t.name
        """
        rows = self.connection.execute_dict(query)
        return [
            UserDefinedType(
                schema_name=row["schema_name"],
                name=row["type_name"],
                type_category=row["type_category"],
                base_type=row["base_type"],
                max_length=row["max_length"],
                precision=row["precision"],
                scale=row["scale"],
                is_nullable=row["is_nullable"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]

    def _get_table_type_columns(self, schema_name: str, type_name: str) -> list[TypeColumn]:
        """Get columns for a table type."""
        query = """
            SELECT
                c.name AS column_name, bt.name AS data_type,
                c.max_length, c.precision, c.scale, c.is_nullable,
                c.column_id AS ordinal_position
            FROM sys.table_types tt
            JOIN sys.schemas s ON tt.schema_id = s.schema_id
            JOIN sys.columns c ON tt.type_table_object_id = c.object_id
            JOIN sys.types bt ON c.user_type_id = bt.user_type_id
            WHERE s.name = ? AND tt.name = ?
            ORDER BY c.column_id
        """
        rows = self.connection.execute_dict(query, (schema_name, type_name))
        return [
            TypeColumn(
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


class SequenceExtractor(MSSQLBaseExtractor):
    """Extracts sequence metadata from SQL Server."""

    def extract(self) -> list[Sequence]:
        """Extract all sequences with their metadata."""
        sequences = self._get_sequences()
        logger.info(f"Found {len(sequences)} sequences")
        return sequences

    def _get_sequences(self) -> list[Sequence]:
        """Get list of all sequences."""
        query = """
            SELECT
                s.name AS schema_name, seq.name AS sequence_name, t.name AS data_type,
                CAST(seq.start_value AS BIGINT) AS start_value,
                CAST(seq.increment AS BIGINT) AS increment,
                CAST(seq.minimum_value AS BIGINT) AS min_value,
                CAST(seq.maximum_value AS BIGINT) AS max_value,
                seq.is_cycling, seq.cache_size,
                CAST(seq.current_value AS BIGINT) AS current_value
            FROM sys.sequences seq
            JOIN sys.schemas s ON seq.schema_id = s.schema_id
            JOIN sys.types t ON seq.user_type_id = t.user_type_id
            ORDER BY s.name, seq.name
        """
        rows = self.connection.execute_dict(query)
        return [
            Sequence(
                schema_name=row["schema_name"],
                name=row["sequence_name"],
                data_type=row["data_type"],
                start_value=row["start_value"],
                increment=row["increment"],
                min_value=row["min_value"],
                max_value=row["max_value"],
                is_cycling=row["is_cycling"],
                cache_size=row["cache_size"],
                current_value=row["current_value"],
            )
            for row in rows
            if self._should_include_schema(row["schema_name"])
        ]


class SynonymExtractor(MSSQLBaseExtractor):
    """Extracts synonym metadata from SQL Server."""

    def extract(self) -> list[Synonym]:
        """Extract all synonyms with their metadata."""
        synonyms = self._get_synonyms()
        logger.info(f"Found {len(synonyms)} synonyms")
        return synonyms

    def _get_synonyms(self) -> list[Synonym]:
        """Get list of all synonyms."""
        query = """
            SELECT s.name AS schema_name, syn.name AS synonym_name, syn.base_object_name
            FROM sys.synonyms syn
            JOIN sys.schemas s ON syn.schema_id = s.schema_id
            ORDER BY s.name, syn.name
        """
        rows = self.connection.execute_dict(query)
        synonyms = []

        for row in rows:
            if not self._should_include_schema(row["schema_name"]):
                continue
            parsed = self._parse_base_object(row["base_object_name"])
            synonyms.append(
                Synonym(
                    schema_name=row["schema_name"],
                    name=row["synonym_name"],
                    base_object_name=row["base_object_name"],
                    target_server=parsed.get("server"),
                    target_database=parsed.get("database"),
                    target_schema=parsed.get("schema"),
                    target_object=parsed.get("object"),
                )
            )
        return synonyms

    def _parse_base_object(self, base_object_name: str) -> dict[str, Optional[str]]:
        """Parse the base object name into components."""
        parts = base_object_name.replace("[", "").replace("]", "").split(".")
        if len(parts) == 1:
            return {"server": None, "database": None, "schema": None, "object": parts[0]}
        elif len(parts) == 2:
            return {"server": None, "database": None, "schema": parts[0], "object": parts[1]}
        elif len(parts) == 3:
            return {"server": None, "database": parts[0], "schema": parts[1], "object": parts[2]}
        elif len(parts) >= 4:
            return {"server": parts[0], "database": parts[1], "schema": parts[2], "object": parts[3]}
        return {"server": None, "database": None, "schema": None, "object": base_object_name}


class SecurityExtractor(MSSQLBaseExtractor):
    """Extracts security metadata from SQL Server."""

    def extract(self) -> dict:
        """Extract all security metadata."""
        users = self._extract_users()
        logger.info(f"Found {len(users)} users")
        roles = self._extract_roles()
        logger.info(f"Found {len(roles)} roles")
        permissions = self._extract_permissions()
        logger.info(f"Found {len(permissions)} permissions")
        memberships = self._extract_role_memberships()
        logger.info(f"Found {len(memberships)} role memberships")

        return {
            "users": users,
            "roles": roles,
            "permissions": permissions,
            "role_memberships": memberships,
        }

    def _extract_users(self) -> list[User]:
        """Extract all database users."""
        query = """
            SELECT
                u.name AS user_name,
                s.name AS schema_name,
                u.type_desc AS user_type,
                u.is_disabled,
                u.default_schema_name,
                u.create_date,
                u.modify_date
            FROM sys.database_principals u
            LEFT JOIN sys.schemas s ON u.default_schema_name = s.name
            WHERE u.type IN ('S', 'U', 'G', 'E')  -- SQL user, Windows user, Windows group, external user
            ORDER BY u.name
        """
        rows = self.connection.execute_dict(query)
        return [
            User(
                name=row["user_name"],
                schema_name=row["schema_name"],
                authentication_type=row["user_type"],
                is_disabled=bool(row["is_disabled"]),
                default_schema=row["default_schema_name"],
                create_date=str(row["create_date"]) if row["create_date"] else None,
                modify_date=str(row["modify_date"]) if row["modify_date"] else None,
            )
            for row in rows
        ]

    def _extract_roles(self) -> list[Role]:
        """Extract all database roles."""
        query = """
            SELECT
                r.name AS role_name,
                r.type_desc AS role_type,
                r.is_disabled,
                r.create_date,
                r.modify_date
            FROM sys.database_principals r
            WHERE r.type = 'R'  -- Database role
            ORDER BY r.name
        """
        rows = self.connection.execute_dict(query)
        return [
            Role(
                name=row["role_name"],
                role_type=row["role_type"],
                is_disabled=bool(row["is_disabled"]),
                create_date=str(row["create_date"]) if row["create_date"] else None,
                modify_date=str(row["modify_date"]) if row["modify_date"] else None,
            )
            for row in rows
        ]

    def _extract_permissions(self) -> list[Permission]:
        """Extract all object-level permissions."""
        query = """
            SELECT
                dp.name AS grantee_name,
                dp.type_desc AS grantee_type,
                s.name AS object_schema,
                o.name AS object_name,
                o.type_desc AS object_type,
                p.permission_name,
                p.state_desc AS state,
                gp.name AS grantor_name
            FROM sys.database_permissions p
            JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
            JOIN sys.objects o ON p.major_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            LEFT JOIN sys.database_principals gp ON p.grantor_principal_id = gp.principal_id
            WHERE p.class = 1  -- Object permissions
            ORDER BY s.name, o.name, dp.name, p.permission_name
        """
        rows = self.connection.execute_dict(query)
        return [
            Permission(
                grantee=row["grantee_name"],
                grantee_type=row["grantee_type"],
                object_schema=row["object_schema"],
                object_name=row["object_name"],
                object_type=row["object_type"],
                permission=row["permission_name"],
                state=row["state"],
                grantor=row["grantor_name"],
            )
            for row in rows
        ]

    def _extract_role_memberships(self) -> list[RoleMembership]:
        """Extract all role memberships."""
        query = """
            SELECT
                m.name AS member_name,
                r.name AS role_name,
                m.type_desc AS member_type
            FROM sys.database_role_members rm
            JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
            JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
            ORDER BY r.name, m.name
        """
        rows = self.connection.execute_dict(query)
        return [
            RoleMembership(
                member_name=row["member_name"],
                role_name=row["role_name"],
                member_type=row["member_type"],
            )
            for row in rows
        ]
