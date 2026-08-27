"""Tests for markdown dependency sections."""

from pathlib import Path

from schema_scraper.base.models import Function, ObjectRef, Procedure, Table, View
from schema_scraper.config import ScraperConfig
from schema_scraper.generators.markdown import MarkdownGenerator


def _generator(tmp_path: Path) -> MarkdownGenerator:
    config = ScraperConfig(
        db_type="sqlite",
        database="test",
        output_dir=tmp_path,
        quiet=True,
    )
    gen = MarkdownGenerator(config)
    gen._create_directories()
    return gen


class TestInputsAndReferencedBy:
    def test_table_emits_inputs_and_referenced_by(self, tmp_path: Path):
        gen = _generator(tmp_path)
        table = Table(
            schema_name="main",
            name="customers",
            reads_from=[ObjectRef("main", "regions", "table")],
            used_by=[ObjectRef("main", "customer_order_summary", "view")],
        )
        text = gen._generate_table_file(table).read_text(encoding="utf-8")
        assert "## Inputs" in text
        assert "### Reads From" in text
        assert "main.regions" in text
        assert "### Referenced By" in text
        assert "main.customer_order_summary" in text

    def test_view_emits_inputs_and_referenced_by(self, tmp_path: Path):
        gen = _generator(tmp_path)
        view = View(
            schema_name="main",
            name="customer_order_summary",
            reads_from=[
                ObjectRef("main", "customers", "table"),
                ObjectRef("main", "orders", "table"),
            ],
            used_by=[ObjectRef("dbo", "sp_refresh", "procedure")],
        )
        text = gen._generate_view_file(view).read_text(encoding="utf-8")
        assert "## Inputs" in text
        assert "### Reads From" in text
        assert "main.customers" in text
        assert "main.orders" in text
        assert "### Referenced By" in text
        assert "dbo.sp_refresh" in text
        assert "## Base Tables" not in text

    def test_procedure_emits_inputs(self, tmp_path: Path):
        gen = _generator(tmp_path)
        proc = Procedure(
            schema_name="dbo",
            name="sp_Kova_UpdateHistoricCostCalcData",
            reads_from=[
                ObjectRef("dbo", "CostHistory", "table"),
                ObjectRef("dbo", "fn_latest", "function"),
            ],
        )
        text = gen._generate_procedure_file(proc).read_text(encoding="utf-8")
        assert "## Inputs" in text
        assert "### Reads From" in text
        assert "dbo.CostHistory" in text
        assert "dbo.fn_latest" in text

    def test_function_emits_inputs(self, tmp_path: Path):
        gen = _generator(tmp_path)
        func = Function(
            schema_name="dbo",
            name="fn_option_profitability",
            function_type="TABLE",
            reads_from=[ObjectRef("dbo", "Options", "table")],
        )
        text = gen._generate_function_file(func).read_text(encoding="utf-8")
        assert "## Inputs" in text
        assert "### Reads From" in text
        assert "dbo.Options" in text

    def test_view_falls_back_to_base_tables_without_reads_from(self, tmp_path: Path):
        gen = _generator(tmp_path)
        view = View(
            schema_name="main",
            name="legacy",
            base_tables=["main.products"],
        )
        text = gen._generate_view_file(view).read_text(encoding="utf-8")
        assert "## Base Tables" in text
        assert "main.products" in text
        assert "## Inputs" not in text
