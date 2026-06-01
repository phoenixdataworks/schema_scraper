"""Tests for dependency extraction helpers."""

from schema_scraper.base.models import ObjectReference, Procedure, View
from schema_scraper.dependencies import (
    apply_dependencies,
    build_reverse_dependencies,
    classify_dependencies,
    infer_read_targets,
    infer_write_targets,
    sync_view_base_tables,
)
from schema_scraper.base.models import Database, Table


class TestInferWriteTargets:
    def test_insert_with_schema(self):
        definition = "INSERT INTO Sales.Orders (CustomerID) VALUES (@CustomerID)"
        assert infer_write_targets(definition, "dbo") == [("Sales", "Orders")]

    def test_update_without_schema(self):
        definition = "UPDATE Inventory.Products SET QuantityInStock = 0"
        assert infer_write_targets(definition, "Inventory") == [("Inventory", "Products")]


class TestClassifyDependencies:
    def test_promotes_read_to_write(self):
        refs = [
            ObjectReference("Sales", "Orders", "table", "read", "catalog"),
        ]
        definition = "INSERT INTO Sales.Orders (CustomerID) VALUES (1)"
        reads, writes, executes = classify_dependencies(refs, definition, "Sales")
        assert len(reads) == 1
        assert writes[0].access == "write"
        assert writes[0].object_name == "Orders"

    def test_inferred_reads_when_no_catalog(self):
        definition = "SELECT * FROM Sales.Customers c JOIN Sales.Orders o ON c.CustomerID = o.CustomerID"
        reads, writes, executes = classify_dependencies([], definition, "Sales")
        assert ("Sales", "Customers") in [(r.schema_name, r.object_name) for r in reads]
        assert ("Sales", "Orders") in [(r.schema_name, r.object_name) for r in reads]
        assert writes == []
        assert executes == []


class TestReverseDependencies:
    def test_table_used_by_view(self):
        table = Table(schema_name="Sales", name="Orders")
        view = View(schema_name="Sales", name="CustomerSummary")
        view.reads_from = [
            ObjectReference("Sales", "Orders", "table", "read", "catalog"),
        ]
        db = Database(name="TestDB", tables=[table], views=[view])
        build_reverse_dependencies(db)
        assert table.used_by == [("Sales", "CustomerSummary", "view")]


class TestApplyDependencies:
    def test_syncs_view_base_tables(self):
        view = View(schema_name="Sales", name="Summary")
        reads = [
            ObjectReference("Sales", "Customers", "table", "read", "catalog"),
            ObjectReference("Sales", "Orders", "table", "read", "catalog"),
        ]
        apply_dependencies(view, reads, [], [])
        assert view.base_tables == ["Sales.Customers", "Sales.Orders"]

    def test_procedure_dependencies(self):
        proc = Procedure(schema_name="Sales", name="CreateOrder")
        writes = [ObjectReference("Sales", "Orders", "table", "write", "inferred")]
        apply_dependencies(proc, [], writes, [])
        assert proc.writes_to[0].full_name == "Sales.Orders"
