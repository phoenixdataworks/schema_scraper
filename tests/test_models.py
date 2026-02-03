"""Tests for data models."""

import pytest
from schema_scraper.base.models import Column, FunctionColumn, Parameter, TypeColumn


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
