"""Tests for the object dependency graph."""

from pathlib import Path

from schema_scraper.base.models import (
    Database,
    Function,
    ObjectRef,
    Procedure,
    Relationship,
    Table,
    View,
)
from schema_scraper.backends.sqlite import SQLiteConnection, get_extractors
from schema_scraper.config import ScraperConfig
from schema_scraper.dependencies import (
    apply_relationships,
    attach_object_dependencies,
    dedupe_relationships,
    normalize_object_type,
    relationships_from_sql_definitions,
    relationships_from_view_base_tables,
    split_qualified_name,
    strip_sql_noise,
)


class TestNormalizeObjectType:
    def test_mssql_codes(self):
        assert normalize_object_type("U") == "table"
        assert normalize_object_type("V") == "view"
        assert normalize_object_type("P") == "procedure"
        assert normalize_object_type("FN") == "function"
        assert normalize_object_type("IF") == "function"
        assert normalize_object_type("TF") == "function"

    def test_verbose_catalog_names(self):
        assert normalize_object_type("SQL_STORED_PROCEDURE") == "procedure"
        assert normalize_object_type("MATERIALIZED VIEW") == "view"
        assert normalize_object_type("USER_TABLE") == "table"

    def test_empty(self):
        assert normalize_object_type(None) == "unknown"
        assert normalize_object_type("") == "unknown"


class TestSplitQualifiedName:
    def test_dotted(self):
        assert split_qualified_name("sales.orders") == ("sales", "orders")

    def test_bracketed(self):
        assert split_qualified_name("[dbo].[Customers]") == ("dbo", "Customers")

    def test_bare_uses_default_schema(self):
        assert split_qualified_name("customers", default_schema="main") == (
            "main",
            "customers",
        )


class TestStripSqlNoise:
    def test_strips_line_comment_and_string(self):
        sql = "SELECT * FROM customers -- ignore orders\nWHERE name = 'FROM products'"
        cleaned = strip_sql_noise(sql)
        assert "customers" in cleaned
        assert "orders" not in cleaned
        assert "products" not in cleaned


class TestRelationshipModel:
    def test_source_and_target_refs(self):
        rel = Relationship(
            from_schema="sales",
            from_name="order_summary",
            from_type="view",
            to_schema="sales",
            to_name="orders",
            to_type="table",
        )
        assert rel.source == ObjectRef("sales", "order_summary", "view")
        assert rel.target == ObjectRef("sales", "orders", "table")
        assert rel.source.full_name == "sales.order_summary"


class TestGraphBuilding:
    def test_base_tables_become_reads_from(self):
        view = View(
            schema_name="main",
            name="order_summary",
            base_tables=["main.customers", "main.orders"],
        )
        customers = Table(schema_name="main", name="customers")
        orders = Table(schema_name="main", name="orders")
        database = Database(
            name="test",
            db_type="sqlite",
            tables=[customers, orders],
            views=[view],
        )

        rels = relationships_from_view_base_tables(database)
        apply_relationships(database, rels)

        assert [r.full_name for r in view.reads_from] == [
            "main.customers",
            "main.orders",
        ]
        assert [r.full_name for r in customers.used_by] == ["main.order_summary"]
        assert [r.full_name for r in orders.used_by] == ["main.order_summary"]

    def test_sql_definition_scan_finds_from_clause(self):
        view = View(
            schema_name="main",
            name="low_stock",
            definition="CREATE VIEW low_stock AS SELECT * FROM products WHERE qty < 5",
        )
        products = Table(schema_name="main", name="products")
        database = Database(
            name="test",
            db_type="sqlite",
            tables=[products],
            views=[view],
        )

        rels = relationships_from_sql_definitions(database)
        apply_relationships(database, rels)

        assert [r.name for r in view.reads_from] == ["products"]
        assert [r.name for r in products.used_by] == ["low_stock"]

    def test_sql_scan_ignores_names_only_in_strings(self):
        view = View(
            schema_name="main",
            name="notes",
            definition="CREATE VIEW notes AS SELECT 'FROM products' AS label FROM customers",
        )
        products = Table(schema_name="main", name="products")
        customers = Table(schema_name="main", name="customers")
        database = Database(
            name="test",
            db_type="sqlite",
            tables=[products, customers],
            views=[view],
        )

        rels = relationships_from_sql_definitions(database)
        names = {rel.to_name for rel in rels}
        assert "customers" in names
        assert "products" not in names

    def test_dedupe_and_skip_self_edges(self):
        rels = dedupe_relationships(
            [
                Relationship("s", "a", "view", "s", "b", "table"),
                Relationship("s", "a", "view", "s", "b", "table"),
                Relationship("s", "a", "view", "s", "a", "view"),
            ]
        )
        assert len(rels) == 1
        assert rels[0].to_name == "b"

    def test_procedure_and_function_edges(self):
        proc = Procedure(
            schema_name="dbo",
            name="sp_refresh",
            definition="INSERT INTO fact SELECT * FROM staging",
        )
        func = Function(
            schema_name="dbo",
            name="fn_total",
            function_type="SCALAR",
            definition="SELECT SUM(amount) FROM fact",
        )
        fact = Table(schema_name="dbo", name="fact")
        staging = Table(schema_name="dbo", name="staging")
        database = Database(
            name="test",
            db_type="mssql",
            tables=[fact, staging],
            procedures=[proc],
            functions=[func],
        )

        apply_relationships(database, relationships_from_sql_definitions(database))

        assert {r.name for r in proc.reads_from} == {"fact", "staging"}
        assert {r.name for r in func.reads_from} == {"fact"}
        used = {r.name for r in fact.used_by}
        assert used == {"sp_refresh", "fn_total"}


class TestSqliteEndToEnd:
    def test_attach_from_real_sqlite_schema(self, tmp_path: Path):
        db_path = tmp_path / "shop.db"
        db_path.write_bytes(b"")
        init_sql = Path("tests/integration/sqlite/init.sql").read_text(encoding="utf-8")

        config = ScraperConfig(
            db_type="sqlite",
            database=str(db_path),
            database_path=str(db_path),
            quiet=True,
        )
        with SQLiteConnection(config) as conn:
            conn.connection.executescript(init_sql)
            extractors = get_extractors()
            database = Database(name="shop", db_type="sqlite")
            database.tables = extractors["tables"](conn, config).extract()
            database.views = extractors["views"](conn, config).extract()
            rels = attach_object_dependencies(conn, database, config)

        assert rels
        views = {view.name: view for view in database.views}
        tables = {table.name: table for table in database.tables}

        summary = views["customer_order_summary"]
        assert {ref.name for ref in summary.reads_from} >= {"customers", "orders"}

        low_stock = views["low_stock_products"]
        assert {ref.name for ref in low_stock.reads_from} >= {"products"}

        assert any(ref.name == "customer_order_summary" for ref in tables["customers"].used_by)
        assert any(ref.name == "low_stock_products" for ref in tables["products"].used_by)
