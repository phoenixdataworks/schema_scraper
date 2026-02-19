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
    Permission,
    PrimaryKey,
    Procedure,
    Role,
    RoleMembership,
    Table,
    Trigger,
    UniqueConstraint,
    User,
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
            table.unique_constraints = self._get_unique_constraints(table.schema_name, table.name)
            table.triggers = self._get_table_triggers(table.schema_name, table.name)
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
        # Get basic index info
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

        indexes = []
        for row in rows:
            # Try to get filter definition from SHOW CREATE TABLE (MySQL 8.0+)
            filter_definition = None
            try:
                create_query = f"SHOW CREATE TABLE `{schema_name}`.`{table_name}`"
                create_rows = self.connection.execute_dict(create_query)
                if create_rows:
                    create_sql = create_rows[0]["Create Table"]
                    # Look for index definition with WHERE clause
                    import re
                    index_pattern = rf'INDEX\s+`{re.escape(row["index_name"])}`\s+.*?(WHERE\s+[^,\)]+)'
                    match = re.search(index_pattern, create_sql, re.IGNORECASE | re.DOTALL)
                    if match:
                        filter_definition = match.group(1)
            except Exception:
                # SHOW CREATE TABLE may not be available or index may not have filter
                pass

            indexes.append(
                Index(
                    name=row["index_name"],
                    columns=row["columns"].split(","),
                    is_unique=bool(row["is_unique"]),
                    is_primary_key=bool(row["is_primary_key"]),
                    is_clustered=False,  # MySQL doesn't have explicit clustered indexes like SQL Server
                    index_type=row["index_type"].upper(),
                    filter_definition=filter_definition,
                )
            )
        return indexes

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

    def _get_unique_constraints(self, schema_name: str, table_name: str) -> list[UniqueConstraint]:
        """Get unique constraints for a table."""
        query = """
            SELECT
                tc.constraint_name,
                GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
            AND tc.table_schema = %s AND tc.table_name = %s
            GROUP BY tc.constraint_name
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [UniqueConstraint(name=row["constraint_name"], columns=row["columns"].split(",")) for row in rows]

    def _get_table_triggers(self, schema_name: str, table_name: str) -> list[Trigger]:
        """Get triggers for a table."""
        query = """
            SELECT
                trigger_name,
                action_timing AS trigger_type,
                event_manipulation AS event,
                action_statement AS definition
            FROM information_schema.triggers
            WHERE event_object_schema = %s AND event_object_table = %s
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        triggers = []

        for row in rows:
            triggers.append(
                Trigger(
                    schema_name=schema_name,
                    name=row["trigger_name"],
                    parent_table_schema=schema_name,
                    parent_table_name=table_name,
                    trigger_type=row["trigger_type"],
                    events=[row["event"]],
                    definition=row["definition"],
                )
            )
        return triggers

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


class SecurityExtractor(BaseExtractor):
    """Extracts security metadata from MySQL."""

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
                u.user AS user_name,
                u.host AS host,
                u.plugin AS auth_plugin,
                u.authentication_string IS NOT NULL AS has_password,
                u.account_locked = 'Y' AS is_locked,
                u.password_expired = 'Y' AS password_expired,
                u.password_last_changed AS password_change_date
            FROM mysql.user u
            ORDER BY u.user, u.host
        """
        rows = self.connection.execute_dict(query)
        return [
            User(
                name=f"{row['user_name']}@{row['host']}",
                authentication_type=row["auth_plugin"] or "UNKNOWN",
                is_disabled=bool(row["is_locked"]),
                create_date=None,
                modify_date=str(row["password_change_date"]) if row["password_change_date"] else None,
            )
            for row in rows
        ]

    def _extract_roles(self) -> list[Role]:
        """Extract all database roles (MySQL 8.0+)."""
        # MySQL roles are essentially users that cannot login
        query = """
            SELECT
                u.user AS role_name,
                u.host AS host,
                CASE
                    WHEN u.account_locked = 'Y' THEN 'DISABLED_ROLE'
                    ELSE 'DATABASE_ROLE'
                END AS role_type,
                u.account_locked = 'Y' AS is_disabled
            FROM mysql.user u
            WHERE u.account_locked = 'Y' OR u.user LIKE 'mysql.%'
            ORDER BY u.user, u.host
        """
        try:
            rows = self.connection.execute_dict(query)
            return [
                Role(
                    name=f"{row['role_name']}@{row['host']}",
                    role_type=row["role_type"],
                    is_disabled=bool(row["is_disabled"]),
                    create_date=None,
                    modify_date=None,
                )
                for row in rows
            ]
        except Exception:
            # Older MySQL versions don't have roles
            return []

    def _extract_permissions(self) -> list[Permission]:
        """Extract object-level permissions."""
        permissions = []

        # Extract table permissions from mysql.db and mysql.tables_priv
        db_query = """
            SELECT
                db AS schema_name,
                user AS grantee,
                host AS grantee_host,
                Select_priv = 'Y' AS can_select,
                Insert_priv = 'Y' AS can_insert,
                Update_priv = 'Y' AS can_update,
                Delete_priv = 'Y' AS can_delete,
                Create_priv = 'Y' AS can_create,
                Drop_priv = 'Y' AS can_drop,
                Grant_priv = 'Y' AS can_grant,
                References_priv = 'Y' AS can_reference,
                Index_priv = 'Y' AS can_index,
                Alter_priv = 'Y' AS can_alter,
                Create_tmp_table_priv = 'Y' AS can_create_tmp,
                Lock_tables_priv = 'Y' AS can_lock
            FROM mysql.db
            ORDER BY db, user, host
        """
        db_rows = self.connection.execute_dict(db_query)
        for row in db_rows:
            grantee = f"{row['grantee']}@{row['grantee_host']}"
            perms = [
                ("SELECT", row["can_select"]),
                ("INSERT", row["can_insert"]),
                ("UPDATE", row["can_update"]),
                ("DELETE", row["can_delete"]),
                ("CREATE", row["can_create"]),
                ("DROP", row["can_drop"]),
                ("GRANT", row["can_grant"]),
                ("REFERENCES", row["can_reference"]),
                ("INDEX", row["can_index"]),
                ("ALTER", row["can_alter"]),
            ]
            for perm_name, has_perm in perms:
                if has_perm:
                    permissions.append(Permission(
                        grantee=grantee,
                        grantee_type="USER",
                        object_schema=row["schema_name"],
                        object_name="",  # Database-level permission
                        object_type="DATABASE",
                        permission=perm_name,
                        state="GRANT",
                        grantor=None,
                    ))

        # Extract table-specific permissions
        table_query = """
            SELECT
                tp.db AS schema_name,
                tp.table_name AS object_name,
                tp.user AS grantee,
                tp.host AS grantee_host,
                tp.table_priv AS table_privileges,
                tp.column_priv AS column_privileges
            FROM mysql.tables_priv tp
            ORDER BY tp.db, tp.table_name, tp.user, tp.host
        """
        table_rows = self.connection.execute_dict(table_query)
        for row in table_rows:
            grantee = f"{row['grantee']}@{row['grantee_host']}"

            # Parse table privileges (comma-separated)
            if row["table_privileges"]:
                priv_list = row["table_privileges"].split(',')
                for priv in priv_list:
                    priv = priv.strip()
                    if priv:
                        permissions.append(Permission(
                            grantee=grantee,
                            grantee_type="USER",
                            object_schema=row["schema_name"],
                            object_name=row["object_name"],
                            object_type="TABLE",
                            permission=priv.upper(),
                            state="GRANT",
                            grantor=None,
                        ))

        return permissions

    def _extract_role_memberships(self) -> list[RoleMembership]:
        """Extract role memberships (MySQL 8.0+)."""
        # MySQL role memberships are stored in mysql.role_edges
        query = """
            SELECT
                from_user AS member_name,
                from_host AS member_host,
                to_user AS role_name,
                to_host AS role_host
            FROM mysql.role_edges
            ORDER BY to_user, to_host, from_user, from_host
        """
        try:
            rows = self.connection.execute_dict(query)
            return [
                RoleMembership(
                    member_name=f"{row['member_name']}@{row['member_host']}",
                    role_name=f"{row['role_name']}@{row['role_host']}",
                    member_type="USER",
                )
                for row in rows
            ]
        except Exception:
            # Older MySQL versions don't have role_edges table
            return []
