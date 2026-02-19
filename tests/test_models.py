"""Tests for data models."""

import pytest
from schema_scraper.base.models import (
    Column,
    FunctionColumn,
    Parameter,
    Partition,
    PartitionScheme,
    Permission,
    Role,
    RoleMembership,
    TablePartitioning,
    TypeColumn,
    User,
)


class TestColumn:
    """Tests for Column model."""

    def test_full_type_varchar(self):
        """Should format varchar with length."""
        col = Column(name="test", data_type="varchar", max_length=50)
        assert col.full_type == "varchar(50)"

    def test_full_type_varchar_max(self):
        """Should format varchar(max)."""
        col = Column(name="test", data_type="varchar", max_length=-1)
        assert col.full_type == "varchar(max)"

    def test_full_type_nvarchar(self):
        """Should format nvarchar with correct length (halved)."""
        col = Column(name="test", data_type="nvarchar", max_length=100)
        assert col.full_type == "nvarchar(50)"

    def test_full_type_nvarchar_max(self):
        """Should format nvarchar(max)."""
        col = Column(name="test", data_type="nvarchar", max_length=-1)
        assert col.full_type == "nvarchar(max)"

    def test_full_type_character_varying(self):
        """Should format character varying (PostgreSQL)."""
        col = Column(name="test", data_type="character varying", max_length=255)
        assert col.full_type == "character varying(255)"

    def test_full_type_decimal(self):
        """Should format decimal with precision and scale."""
        col = Column(name="test", data_type="decimal", precision=18, scale=2)
        assert col.full_type == "decimal(18,2)"

    def test_full_type_decimal_no_scale(self):
        """Should format decimal with precision only."""
        col = Column(name="test", data_type="numeric", precision=10, scale=0)
        assert col.full_type == "numeric(10)"

    def test_full_type_datetime2_default(self):
        """Should format datetime2 without precision when default."""
        col = Column(name="test", data_type="datetime2", scale=7)
        assert col.full_type == "datetime2"

    def test_full_type_datetime2_custom(self):
        """Should format datetime2 with custom precision."""
        col = Column(name="test", data_type="datetime2", scale=3)
        assert col.full_type == "datetime2(3)"

    def test_full_type_simple(self):
        """Should return simple types as-is."""
        col = Column(name="test", data_type="int")
        assert col.full_type == "int"

        col = Column(name="test", data_type="bigint")
        assert col.full_type == "bigint"

        col = Column(name="test", data_type="datetime")
        assert col.full_type == "datetime"

        col = Column(name="test", data_type="text")
        assert col.full_type == "text"


class TestParameter:
    """Tests for Parameter model."""

    def test_full_type_varchar(self):
        """Should format varchar parameter with length."""
        param = Parameter(name="@test", data_type="varchar", max_length=100)
        assert param.full_type == "varchar(100)"

    def test_full_type_decimal(self):
        """Should format decimal parameter."""
        param = Parameter(name="@test", data_type="decimal", precision=10, scale=4)
        assert param.full_type == "decimal(10,4)"

    def test_full_type_simple(self):
        """Should return simple types as-is."""
        param = Parameter(name="@test", data_type="int")
        assert param.full_type == "int"


class TestFunctionColumn:
    """Tests for FunctionColumn model."""

    def test_full_type(self):
        """Should format function column type."""
        col = FunctionColumn(name="test", data_type="nvarchar", max_length=200)
        assert col.full_type == "nvarchar(100)"


class TestTypeColumn:
    """Tests for TypeColumn model."""

    def test_full_type(self):
        """Should format type column type."""
        col = TypeColumn(name="test", data_type="varchar", max_length=50)
        assert col.full_type == "varchar(50)"


class TestPartition:
    """Tests for Partition model."""

    def test_partition_creation(self):
        """Should create partition with basic properties."""
        partition = Partition(
            partition_number=1,
            boundary_value="2023-01-01",
            filegroup_name="PRIMARY",
            row_count=1000,
        )
        assert partition.partition_number == 1
        assert partition.boundary_value == "2023-01-01"
        assert partition.filegroup_name == "PRIMARY"
        assert partition.row_count == 1000
        assert partition.data_compression is None
        assert partition.is_readonly is False


class TestPartitionScheme:
    """Tests for PartitionScheme model."""

    def test_partition_scheme_creation(self):
        """Should create partition scheme with partitions."""
        partitions = [
            Partition(partition_number=1, boundary_value="2023-01-01"),
            Partition(partition_number=2, boundary_value="2023-07-01"),
        ]
        scheme = PartitionScheme(
            name="monthly_partition",
            partition_column="created_date",
            partition_type="RANGE",
            boundary_type="LEFT",
            partitions=partitions,
        )
        assert scheme.name == "monthly_partition"
        assert scheme.partition_column == "created_date"
        assert scheme.partition_type == "RANGE"
        assert scheme.boundary_type == "LEFT"
        assert len(scheme.partitions) == 2


class TestTablePartitioning:
    """Tests for TablePartitioning model."""

    def test_partitioned_table(self):
        """Should create partitioned table info."""
        scheme = PartitionScheme(
            name="test_scheme",
            partition_column="id",
            partition_type="RANGE",
        )
        partitioning = TablePartitioning(
            partition_scheme=scheme,
            is_partitioned=True,
        )
        assert partitioning.is_partitioned is True
        assert partitioning.partition_scheme is not None

    def test_non_partitioned_table(self):
        """Should create non-partitioned table info."""
        partitioning = TablePartitioning(is_partitioned=False)
        assert partitioning.is_partitioned is False
        assert partitioning.partition_scheme is None


class TestUser:
    """Tests for User model."""

    def test_user_creation(self):
        """Should create user with authentication info."""
        user = User(
            name="testuser",
            authentication_type="PASSWORD",
            is_disabled=False,
            default_schema="dbo",
        )
        assert user.name == "testuser"
        assert user.authentication_type == "PASSWORD"
        assert user.is_disabled is False
        assert user.default_schema == "dbo"


class TestRole:
    """Tests for Role model."""

    def test_role_creation(self):
        """Should create role with permissions."""
        role = Role(
            name="db_reader",
            role_type="DATABASE_ROLE",
            is_disabled=False,
        )
        assert role.name == "db_reader"
        assert role.role_type == "DATABASE_ROLE"
        assert role.is_disabled is False


class TestPermission:
    """Tests for Permission model."""

    def test_permission_creation(self):
        """Should create permission grant."""
        permission = Permission(
            grantee="testuser",
            grantee_type="USER",
            object_schema="dbo",
            object_name="users",
            object_type="TABLE",
            permission="SELECT",
            state="GRANT",
            grantor="dbo",
        )
        assert permission.grantee == "testuser"
        assert permission.grantee_type == "USER"
        assert permission.object_schema == "dbo"
        assert permission.object_name == "users"
        assert permission.object_type == "TABLE"
        assert permission.permission == "SELECT"
        assert permission.state == "GRANT"
        assert permission.grantor == "dbo"


class TestRoleMembership:
    """Tests for RoleMembership model."""

    def test_role_membership_creation(self):
        """Should create user-role relationship."""
        membership = RoleMembership(
            member_name="testuser",
            role_name="db_reader",
            member_type="USER",
        )
        assert membership.member_name == "testuser"
        assert membership.role_name == "db_reader"
        assert membership.member_type == "USER"
