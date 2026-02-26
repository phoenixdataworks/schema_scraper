"""Snowflake schema extractors."""

import logging
from typing import Any, Optional

from ...base import BaseExtractor
from ...base.models import (
    Column,
    ForeignKey,
    Function,
    Parameter,
    Permission,
    PrimaryKey,
    Procedure,
    Role,
    RoleMembership,
    Sequence,
    Table,
    UniqueConstraint,
    User,
    View,
)

logger = logging.getLogger(__name__)


class TableExtractor(BaseExtractor):
    """Extracts table metadata from Snowflake."""

    def extract(self) -> list[Table]:
        """Extract all tables with their metadata."""
        tables = self._get_tables()
        logger.info(f"Found {len(tables)} tables")

        for table in tables:
            table.columns = self._get_columns(table.schema_name, table.name)
            table.primary_key = self._get_primary_key(table.schema_name, table.name)
            table.foreign_keys = self._get_foreign_keys(table.schema_name, table.name)
            table.unique_constraints = self._get_unique_constraints(table.schema_name, table.name)

        # Build referenced_by relationships
        self._build_references(tables)
        return tables

    def _get_tables(self) -> list[Table]:
        """Get all user tables."""
        query = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                ROW_COUNT,
                BYTES,
                COMMENT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        rows = self.connection.execute_dict(query)
        tables = []
        for row in rows:
            schema_name = row["TABLE_SCHEMA"]
            if not self._should_include_schema(schema_name):
                continue
            tables.append(Table(
                schema_name=schema_name,
                name=row["TABLE_NAME"],
                row_count=row.get("ROW_COUNT") or 0,
                total_space_kb=(row.get("BYTES") or 0) // 1024,
                description=row.get("COMMENT"),
            ))
        return tables

    def _get_columns(self, schema_name: str, table_name: str) -> list[Column]:
        """Get columns for a table."""
        query = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                IS_IDENTITY,
                ORDINAL_POSITION,
                COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        rows = self.connection.execute_dict(query, (schema_name, table_name))
        return [
            Column(
                name=row["COLUMN_NAME"],
                data_type=row["DATA_TYPE"],
                max_length=row.get("CHARACTER_MAXIMUM_LENGTH"),
                precision=row.get("NUMERIC_PRECISION"),
                scale=row.get("NUMERIC_SCALE"),
                is_nullable=row.get("IS_NULLABLE", "YES") == "YES",
                default_value=row.get("COLUMN_DEFAULT"),
                is_identity=row.get("IS_IDENTITY", "NO") == "YES",
                ordinal_position=row.get("ORDINAL_POSITION", 0),
                description=row.get("COMMENT"),
            )
            for row in rows
        ]

    def _get_primary_key(self, schema_name: str, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key for a table using SHOW PRIMARY KEYS."""
        try:
            rows = self.connection.execute_dict(
                f'SHOW PRIMARY KEYS IN TABLE "{schema_name}"."{table_name}"'
            )
            if not rows:
                return None
            # Sort by key_sequence to get correct column order
            rows.sort(key=lambda r: r.get("key_sequence", 0))
            return PrimaryKey(
                name=rows[0].get("constraint_name", "PK"),
                columns=[row["column_name"] for row in rows],
                is_clustered=False,
            )
        except Exception as e:
            logger.debug(f"Could not get primary key for {schema_name}.{table_name}: {e}")
            return None

    def _get_foreign_keys(self, schema_name: str, table_name: str) -> list[ForeignKey]:
        """Get foreign keys for a table using SHOW IMPORTED KEYS."""
        try:
            rows = self.connection.execute_dict(
                f'SHOW IMPORTED KEYS IN TABLE "{schema_name}"."{table_name}"'
            )
            if not rows:
                return []

            fk_groups: dict[str, list[dict]] = {}
            for row in rows:
                name = row.get("fk_name", "")
                if name not in fk_groups:
                    fk_groups[name] = []
                fk_groups[name].append(row)

            foreign_keys = []
            for name, fk_rows in fk_groups.items():
                fk_rows.sort(key=lambda r: r.get("key_sequence", 0))
                foreign_keys.append(ForeignKey(
                    name=name,
                    columns=[r["fk_column_name"] for r in fk_rows],
                    referenced_schema=fk_rows[0].get("pk_schema_name", ""),
                    referenced_table=fk_rows[0].get("pk_table_name", ""),
                    referenced_columns=[r["pk_column_name"] for r in fk_rows],
                    on_delete=fk_rows[0].get("delete_rule", "NO ACTION"),
                    on_update=fk_rows[0].get("update_rule", "NO ACTION"),
                ))
            return foreign_keys
        except Exception as e:
            logger.debug(f"Could not get foreign keys for {schema_name}.{table_name}: {e}")
            return []

    def _get_unique_constraints(self, schema_name: str, table_name: str) -> list[UniqueConstraint]:
        """Get unique constraints for a table using SHOW UNIQUE KEYS."""
        try:
            rows = self.connection.execute_dict(
                f'SHOW UNIQUE KEYS IN TABLE "{schema_name}"."{table_name}"'
            )
            if not rows:
                return []

            uc_groups: dict[str, list[dict]] = {}
            for row in rows:
                name = row.get("constraint_name", "")
                if name not in uc_groups:
                    uc_groups[name] = []
                uc_groups[name].append(row)

            constraints = []
            for name, uc_rows in uc_groups.items():
                uc_rows.sort(key=lambda r: r.get("key_sequence", 0))
                constraints.append(UniqueConstraint(
                    name=name,
                    columns=[r["column_name"] for r in uc_rows],
                ))
            return constraints
        except Exception as e:
            logger.debug(f"Could not get unique constraints for {schema_name}.{table_name}: {e}")
            return []

    def _build_references(self, tables: list[Table]) -> None:
        """Build referenced_by relationships across all tables."""
        table_lookup = {t.full_name: t for t in tables}
        for table in tables:
            for fk in table.foreign_keys:
                ref_name = f"{fk.referenced_schema}.{fk.referenced_table}"
                if ref_name in table_lookup:
                    table_lookup[ref_name].referenced_by.append(
                        (table.schema_name, table.name, fk.name)
                    )


class ViewExtractor(BaseExtractor):
    """Extracts view metadata from Snowflake."""

    def extract(self) -> list[View]:
        """Extract all views."""
        views = self._get_views()
        logger.info(f"Found {len(views)} views")

        for view in views:
            view.columns = self._get_columns(view.schema_name, view.name)
        return views

    def _get_views(self) -> list[View]:
        """Get all views."""
        query = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                VIEW_DEFINITION,
                COMMENT
            FROM INFORMATION_SCHEMA.VIEWS
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        rows = self.connection.execute_dict(query)
        views = []
        for row in rows:
            schema_name = row["TABLE_SCHEMA"]
            if not self._should_include_schema(schema_name):
                continue
            views.append(View(
                schema_name=schema_name,
                name=row["TABLE_NAME"],
                definition=row.get("VIEW_DEFINITION"),
                description=row.get("COMMENT"),
            ))
        return views

    def _get_columns(self, schema_name: str, view_name: str) -> list[Column]:
        """Get columns for a view."""
        query = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                ORDINAL_POSITION,
                COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        rows = self.connection.execute_dict(query, (schema_name, view_name))
        return [
            Column(
                name=row["COLUMN_NAME"],
                data_type=row["DATA_TYPE"],
                max_length=row.get("CHARACTER_MAXIMUM_LENGTH"),
                precision=row.get("NUMERIC_PRECISION"),
                scale=row.get("NUMERIC_SCALE"),
                is_nullable=row.get("IS_NULLABLE", "YES") == "YES",
                default_value=row.get("COLUMN_DEFAULT"),
                ordinal_position=row.get("ORDINAL_POSITION", 0),
                description=row.get("COMMENT"),
            )
            for row in rows
        ]


class ProcedureExtractor(BaseExtractor):
    """Extracts stored procedure metadata from Snowflake."""

    def extract(self) -> list[Procedure]:
        """Extract all stored procedures."""
        query = """
            SELECT
                PROCEDURE_SCHEMA,
                PROCEDURE_NAME,
                PROCEDURE_DEFINITION,
                COMMENT,
                ARGUMENT_SIGNATURE
            FROM INFORMATION_SCHEMA.PROCEDURES
            ORDER BY PROCEDURE_SCHEMA, PROCEDURE_NAME
        """
        rows = self.connection.execute_dict(query)
        procedures = []
        for row in rows:
            schema_name = row["PROCEDURE_SCHEMA"]
            if not self._should_include_schema(schema_name):
                continue
            proc = Procedure(
                schema_name=schema_name,
                name=row["PROCEDURE_NAME"],
                definition=row.get("PROCEDURE_DEFINITION"),
                description=row.get("COMMENT"),
                language="SQL",
            )
            proc.parameters = self._parse_arguments(row.get("ARGUMENT_SIGNATURE", ""))
            procedures.append(proc)

        logger.info(f"Found {len(procedures)} procedures")
        return procedures

    def _parse_arguments(self, signature: str) -> list[Parameter]:
        """Parse Snowflake argument signature string into Parameters."""
        if not signature or signature.strip() == "()":
            return []
        # Signature looks like: (ARG1 VARCHAR, ARG2 NUMBER)
        inner = signature.strip().strip("()")
        if not inner:
            return []
        params = []
        for i, part in enumerate(inner.split(","), 1):
            part = part.strip()
            if not part:
                continue
            tokens = part.split()
            if len(tokens) >= 2:
                params.append(Parameter(
                    name=tokens[0],
                    data_type=" ".join(tokens[1:]),
                    ordinal_position=i,
                ))
            elif len(tokens) == 1:
                params.append(Parameter(
                    name=f"arg{i}",
                    data_type=tokens[0],
                    ordinal_position=i,
                ))
        return params


class FunctionExtractor(BaseExtractor):
    """Extracts user-defined function metadata from Snowflake."""

    def extract(self) -> list[Function]:
        """Extract all user-defined functions."""
        query = """
            SELECT
                FUNCTION_SCHEMA,
                FUNCTION_NAME,
                DATA_TYPE,
                FUNCTION_DEFINITION,
                COMMENT,
                ARGUMENT_SIGNATURE
            FROM INFORMATION_SCHEMA.FUNCTIONS
            ORDER BY FUNCTION_SCHEMA, FUNCTION_NAME
        """
        rows = self.connection.execute_dict(query)
        functions = []
        for row in rows:
            schema_name = row["FUNCTION_SCHEMA"]
            if not self._should_include_schema(schema_name):
                continue

            return_type = row.get("DATA_TYPE")
            func_type = "TABLE" if return_type and "TABLE" in str(return_type).upper() else "SCALAR"

            func = Function(
                schema_name=schema_name,
                name=row["FUNCTION_NAME"],
                function_type=func_type,
                return_type=return_type,
                definition=row.get("FUNCTION_DEFINITION"),
                description=row.get("COMMENT"),
                language="SQL",
            )
            func.parameters = self._parse_arguments(row.get("ARGUMENT_SIGNATURE", ""))
            functions.append(func)

        logger.info(f"Found {len(functions)} functions")
        return functions

    def _parse_arguments(self, signature: str) -> list[Parameter]:
        """Parse Snowflake argument signature string into Parameters."""
        if not signature or signature.strip() == "()":
            return []
        inner = signature.strip().strip("()")
        if not inner:
            return []
        params = []
        for i, part in enumerate(inner.split(","), 1):
            part = part.strip()
            if not part:
                continue
            tokens = part.split()
            if len(tokens) >= 2:
                params.append(Parameter(
                    name=tokens[0],
                    data_type=" ".join(tokens[1:]),
                    ordinal_position=i,
                ))
            elif len(tokens) == 1:
                params.append(Parameter(
                    name=f"arg{i}",
                    data_type=tokens[0],
                    ordinal_position=i,
                ))
        return params


class SequenceExtractor(BaseExtractor):
    """Extracts sequence metadata from Snowflake."""

    def extract(self) -> list[Sequence]:
        """Extract all sequences."""
        query = """
            SELECT
                SEQUENCE_SCHEMA,
                SEQUENCE_NAME,
                DATA_TYPE,
                START_VALUE,
                MINIMUM_VALUE,
                MAXIMUM_VALUE,
                "INCREMENT",
                CYCLE_OPTION,
                COMMENT
            FROM INFORMATION_SCHEMA.SEQUENCES
            ORDER BY SEQUENCE_SCHEMA, SEQUENCE_NAME
        """
        rows = self.connection.execute_dict(query)
        sequences = []
        for row in rows:
            schema_name = row["SEQUENCE_SCHEMA"]
            if not self._should_include_schema(schema_name):
                continue

            # Parse numeric values safely
            def safe_int(val: Any, default: int = 0) -> int:
                if val is None:
                    return default
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return default

            sequences.append(Sequence(
                schema_name=schema_name,
                name=row["SEQUENCE_NAME"],
                data_type=row.get("DATA_TYPE", "NUMBER"),
                start_value=safe_int(row.get("START_VALUE"), 1),
                increment=safe_int(row.get("INCREMENT"), 1),
                min_value=safe_int(row.get("MINIMUM_VALUE"), 1),
                max_value=safe_int(row.get("MAXIMUM_VALUE"), 2**63 - 1),
                is_cycling=row.get("CYCLE_OPTION", "NO") == "YES",
                description=row.get("COMMENT"),
            ))

        logger.info(f"Found {len(sequences)} sequences")
        return sequences


class SecurityExtractor(BaseExtractor):
    """Extracts security metadata from Snowflake using SHOW commands."""

    def extract(self) -> dict[str, list]:
        """Extract security objects (users, roles, permissions, memberships)."""
        result = {
            "users": self._get_users(),
            "roles": self._get_roles(),
            "role_memberships": self._get_role_memberships(),
            "permissions": self._get_permissions(),
        }
        return result

    def _get_users(self) -> list[User]:
        """Get database users via SHOW USERS."""
        try:
            rows = self.connection.execute_dict("SHOW USERS")
            users = []
            for row in rows:
                users.append(User(
                    name=row.get("name", ""),
                    authentication_type="SNOWFLAKE",
                    is_disabled=row.get("disabled", "false").lower() == "true"
                    if isinstance(row.get("disabled"), str)
                    else bool(row.get("disabled", False)),
                    create_date=str(row.get("created_on", "")) if row.get("created_on") else None,
                ))
            logger.info(f"Found {len(users)} users")
            return users
        except Exception as e:
            logger.warning(f"Could not extract users (insufficient privileges?): {e}")
            return []

    def _get_roles(self) -> list[Role]:
        """Get database roles via SHOW ROLES."""
        try:
            rows = self.connection.execute_dict("SHOW ROLES")
            roles = []
            for row in rows:
                roles.append(Role(
                    name=row.get("name", ""),
                    role_type="DATABASE_ROLE",
                    create_date=str(row.get("created_on", "")) if row.get("created_on") else None,
                ))
            logger.info(f"Found {len(roles)} roles")
            return roles
        except Exception as e:
            logger.warning(f"Could not extract roles (insufficient privileges?): {e}")
            return []

    def _get_role_memberships(self) -> list[RoleMembership]:
        """Get role grants via SHOW GRANTS."""
        try:
            rows = self.connection.execute_dict("SHOW GRANTS OF ROLE " + self.config.snowflake_role
                                                if self.config.snowflake_role else "SHOW GRANTS")
            memberships = []
            for row in rows:
                grantee = row.get("grantee_name", "")
                role = row.get("role", "")
                if grantee and role:
                    memberships.append(RoleMembership(
                        member_name=grantee,
                        role_name=role,
                        member_type="ROLE" if row.get("granted_to", "") == "ROLE" else "USER",
                    ))
            return memberships
        except Exception as e:
            logger.warning(f"Could not extract role memberships: {e}")
            return []

    def _get_permissions(self) -> list[Permission]:
        """Get object permissions for the current database."""
        try:
            database = self.config.database
            rows = self.connection.execute_dict(f"SHOW GRANTS ON DATABASE {database}")
            permissions = []
            for row in rows:
                permissions.append(Permission(
                    grantee=row.get("grantee_name", ""),
                    grantee_type="ROLE",
                    object_schema="",
                    object_name=row.get("name", ""),
                    object_type=row.get("granted_on", "DATABASE"),
                    permission=row.get("privilege", ""),
                    state="GRANT",
                    grantor=row.get("granted_by"),
                ))
            return permissions
        except Exception as e:
            logger.warning(f"Could not extract permissions: {e}")
            return []
