"""Row counts from SQL Server allocation units."""

from schema_scraper.backends.mssql.extractors import (
    TableExtractor,
    summarize_allocation_unit_stats,
)
from schema_scraper.config import ScraperConfig


def test_second_allocation_unit_does_not_double_row_count():
    """In-row plus row-overflow or LOB repeats p.rows; count the partition once."""
    stats = summarize_allocation_unit_stats(
        [
            {
                "partition_id": 1,
                "row_count": 11_027_838,
                "total_pages": 1_000_000,
                "used_pages": 900_000,
            },
            {
                "partition_id": 1,
                "row_count": 11_027_838,
                "total_pages": 40_000,
                "used_pages": 40_000,
            },
        ]
    )

    assert stats["row_count"] == 11_027_838
    assert stats["total_space_kb"] == (1_000_000 + 40_000) * 8
    assert stats["used_space_kb"] == (900_000 + 40_000) * 8


def test_partitions_with_the_same_row_count_are_both_counted():
    """Equal partition sizes must not collapse the way SUM(DISTINCT rows) would."""
    stats = summarize_allocation_unit_stats(
        [
            {"partition_id": 1, "row_count": 100, "total_pages": 10, "used_pages": 8},
            {"partition_id": 1, "row_count": 100, "total_pages": 4, "used_pages": 4},
            {"partition_id": 2, "row_count": 100, "total_pages": 10, "used_pages": 9},
        ]
    )

    assert stats["row_count"] == 200
    assert stats["total_space_kb"] == (10 + 4 + 10) * 8
    assert stats["used_space_kb"] == (8 + 4 + 9) * 8


def test_single_allocation_unit_counts_rows_and_pages():
    stats = summarize_allocation_unit_stats(
        [
            {"partition_id": 7, "row_count": 5, "total_pages": 10, "used_pages": 8},
        ]
    )

    assert stats == {"row_count": 5, "total_space_kb": 80, "used_space_kb": 64}


def test_missing_stats_are_zero():
    assert summarize_allocation_unit_stats([]) == {
        "row_count": 0,
        "total_space_kb": 0,
        "used_space_kb": 0,
    }

    stats = summarize_allocation_unit_stats(
        [
            {
                "partition_id": 1,
                "row_count": None,
                "total_pages": None,
                "used_pages": None,
            }
        ]
    )
    assert stats == {"row_count": 0, "total_space_kb": 0, "used_space_kb": 0}


class _StatsConnection:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.calls: list[tuple[str, tuple]] = []

    def execute_dict(self, query: str, params: tuple = ()) -> list[dict]:
        self.calls.append((query, params))
        return self.rows


def test_table_stats_query_returns_each_allocation_unit():
    """The catalog query must not SUM(p.rows) across allocation units."""
    connection = _StatsConnection(
        [
            {
                "partition_id": 1,
                "row_count": 11_027_838,
                "total_pages": 10,
                "used_pages": 8,
            },
            {
                "partition_id": 1,
                "row_count": 11_027_838,
                "total_pages": 3,
                "used_pages": 3,
            },
        ]
    )
    extractor = TableExtractor(connection, ScraperConfig())

    stats = extractor._get_table_stats("dbo", "JobBOMPrts")

    assert stats["row_count"] == 11_027_838
    assert stats["total_space_kb"] == 13 * 8
    assert stats["used_space_kb"] == 11 * 8
    query, params = connection.calls[0]
    assert "SUM(p.rows)" not in query
    assert params == ("dbo", "JobBOMPrts")
