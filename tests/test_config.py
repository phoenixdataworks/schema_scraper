"""Tests for configuration module."""

import pytest
from schema_scraper.config import ScraperConfig
from schema_scraper.exceptions import ConfigurationError


class TestScraperConfig:
    """Tests for ScraperConfig class."""

    def test_validate_mssql_connection_string(self):
        """Config with connection string should validate."""
        config = ScraperConfig(
            db_type="mssql",
            connection_string="Driver={SQL Server};Server=localhost"
        )
        config.validate()

    def test_validate_mssql_trusted(self):
        """Config with server, database, and trusted connection should validate."""
        config = ScraperConfig(
            db_type="mssql",
            host="localhost",
            database="TestDB",
            trusted_connection=True,
        )
        config.validate()

    def test_validate_mssql_credentials(self):
        """Config with server, database, and credentials should validate."""
        config = ScraperConfig(
            db_type="mssql",
            host="localhost",
            database="TestDB",
            username="user",
            password="pass",
        )
        config.validate()

    def test_validate_postgresql(self):
        """PostgreSQL config should validate with username."""
        config = ScraperConfig(
            db_type="postgresql",
            host="localhost",
            database="testdb",
            username="postgres",
        )
        config.validate()

    def test_validate_mysql(self):
        """MySQL config should validate."""
        config = ScraperConfig(
            db_type="mysql",
            host="localhost",
            database="testdb",
            username="root",
            password="pass",
        )
        config.validate()

    def test_validate_oracle(self):
        """Oracle config should validate with service name."""
        config = ScraperConfig(
            db_type="oracle",
            host="localhost",
            database="testdb",
            username="system",
            password="pass",
            service_name="ORCL",
        )
        config.validate()

    def test_validate_oracle_missing_service(self):
        """Oracle config without service name or SID should fail."""
        config = ScraperConfig(
            db_type="oracle",
            host="localhost",
            database="testdb",
            username="system",
            password="pass",
        )
        with pytest.raises(ConfigurationError, match="Service name or SID"):
            config.validate()

    def test_validate_sqlite(self):
        """SQLite config should validate with path."""
        config = ScraperConfig(
            db_type="sqlite",
            database="/path/to/db.sqlite",
        )
        config.validate()

    def test_validate_sqlite_missing_path(self):
        """SQLite config without path should fail."""
        config = ScraperConfig(db_type="sqlite")
        with pytest.raises(ConfigurationError, match="Database path"):
            config.validate()

    def test_validate_missing_host(self):
        """Config without host should fail."""
        config = ScraperConfig(
            db_type="postgresql",
            database="TestDB",
            username="user",
        )
        with pytest.raises(ConfigurationError, match="Host is required"):
            config.validate()

    def test_validate_missing_database(self):
        """Config without database should fail."""
        config = ScraperConfig(
            db_type="postgresql",
            host="localhost",
            username="user",
        )
        with pytest.raises(ConfigurationError, match="Database is required"):
            config.validate()

    def test_validate_missing_username(self):
        """PostgreSQL config without username should fail."""
        config = ScraperConfig(
            db_type="postgresql",
            host="localhost",
            database="testdb",
        )
        with pytest.raises(ConfigurationError, match="Username is required"):
            config.validate()

    def test_default_excluded_schemas_mssql(self):
        """MSSQL should have default excluded schemas."""
        config = ScraperConfig(db_type="mssql")
        assert "sys" in config.exclude_schemas
        assert "INFORMATION_SCHEMA" in config.exclude_schemas

    def test_default_excluded_schemas_postgresql(self):
        """PostgreSQL should have default excluded schemas."""
        config = ScraperConfig(db_type="postgresql")
        assert "pg_catalog" in config.exclude_schemas
        assert "information_schema" in config.exclude_schemas

    def test_default_excluded_schemas_mysql(self):
        """MySQL should have default excluded schemas."""
        config = ScraperConfig(db_type="mysql")
        assert "information_schema" in config.exclude_schemas
        assert "mysql" in config.exclude_schemas

    def test_should_include_schema_with_include_list(self):
        """Config with include list should only include specified schemas."""
        config = ScraperConfig(include_schemas=["dbo", "Sales"])
        assert config.should_include_schema("dbo") is True
        assert config.should_include_schema("Sales") is True
        assert config.should_include_schema("HR") is False

    def test_should_extract_all(self):
        """Config with 'all' should extract all types."""
        config = ScraperConfig(object_types=["all"])
        assert config.should_extract("tables") is True
        assert config.should_extract("views") is True
        assert config.should_extract("procedures") is True

    def test_should_extract_specific(self):
        """Config with specific types should only extract those."""
        config = ScraperConfig(object_types=["tables", "views"])
        assert config.should_extract("tables") is True
        assert config.should_extract("views") is True
        assert config.should_extract("procedures") is False

    def test_default_port_postgresql(self):
        """PostgreSQL should default to port 5432."""
        config = ScraperConfig(db_type="postgresql", host="localhost")
        assert config.port == 5432

    def test_default_port_mysql(self):
        """MySQL should default to port 3306."""
        config = ScraperConfig(db_type="mysql", host="localhost")
        assert config.port == 3306

    def test_default_port_oracle(self):
        """Oracle should default to port 1521."""
        config = ScraperConfig(db_type="oracle", host="localhost")
        assert config.port == 1521

    def test_default_port_mssql(self):
        """MSSQL should default to port 1433."""
        config = ScraperConfig(db_type="mssql", host="localhost")
        assert config.port == 1433
