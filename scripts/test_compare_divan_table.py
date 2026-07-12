"""Tests for scripts/compare_divan_table.py."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from compare_divan_table import build_comparison_rows, compare
from format_divan_table import parse_divan_output


BEFORE_OUTPUT = """\
Timer precision: 100 ps
git_log_with_duckdb     fastest │ slowest │ median │ mean │ samples │ iters
├─ metadata_only                      │         │        │      │         │
│  ├─ libgit_t1     95 ms │ 105 ms │ 100 ms │ 100 ms │ 10 │ 10
│  │                   max alloc:
│  │                     4000 │ 4000 │ 4000 │ 4000 │         │
│  │                     400 KB │ 400 KB │ 400 KB │ 400 KB │         │
│  ├─ old_only      9 ms │ 11 ms │ 10 ms │ 10 ms │ 10 │ 10
│  │                   max alloc:
│  │                     10 │ 10 │ 10 │ 10 │         │
│  │                     1 KB │ 1 KB │ 1 KB │ 1 KB │         │
╰─ limit_10                           │         │        │      │         │
   ├─ libgit        45.6 ms │ 46.1 ms │ 45.7 ms │ 45.8 ms │ 10 │ 10
"""

AFTER_OUTPUT = """\
Timer precision: 100 ps
git_log_with_duckdb     fastest │ slowest │ median │ mean │ samples │ iters
├─ metadata_only                      │         │        │      │         │
│  ├─ libgit_t1     48 ms │ 52 ms │ 50 ms │ 50 ms │ 10 │ 10
│  │                   max alloc:
│  │                     20 │ 20 │ 20 │ 20 │         │
│  │                     100 KB │ 100 KB │ 100 KB │ 100 KB │         │
│  ├─ new_only      1.9 s │ 2.1 s │ 2 s │ 2 s │ 10 │ 10
│  │                   max alloc:
│  │                     10 │ 10 │ 10 │ 10 │         │
│  │                     1 KB │ 1 KB │ 1 KB │ 1 KB │         │
╰─ limit_10                           │         │        │      │         │
   ├─ libgit        45.6 ms │ 46.1 ms │ 45.7 ms │ 45.8 ms │ 10 │ 10
"""


class CompareDivanTableTest(unittest.TestCase):
    def rows(self) -> list[list[str]]:
        return build_comparison_rows(
            parse_divan_output(BEFORE_OUTPUT),
            parse_divan_output(AFTER_OUTPUT),
            "before",
            "after",
        )

    def rows_for(self, method: str) -> tuple[list[str], list[str]]:
        rows = self.rows()
        for i, row in enumerate(rows):
            if row[0] == method:
                return rows[i], rows[i + 1]
        raise AssertionError(f"method not found: {method}")

    def test_ratio_and_alloc_ratio(self) -> None:
        base, curr = self.rows_for("metadata_only_libgit_t1")
        self.assertEqual(base, ["metadata_only_libgit_t1", "before", "100 ms", "1.00", "4000", "400 KB", "1.00"])
        self.assertEqual(curr, ["", "after", "50 ms", "0.50", "20", "100 KB", "0.25"])

    def test_ratio_across_units(self) -> None:
        # old_only: 10 ms (before only), new_only: 2 s (after only) — but also
        # verify unit normalization via a method present in both runs.
        base, curr = self.rows_for("limit_10_libgit")
        self.assertEqual(base[2], "45.8 ms")
        self.assertEqual(curr[3], "1.00")

    def test_missing_alloc_block_gives_dash_ratio(self) -> None:
        base, curr = self.rows_for("limit_10_libgit")
        self.assertEqual(base[4:], ["-", "-", "-"])
        self.assertEqual(curr[4:], ["-", "-", "-"])

    def test_method_only_in_baseline(self) -> None:
        base, curr = self.rows_for("metadata_only_old_only")
        self.assertEqual(base[2], "10 ms")
        self.assertEqual(base[3], "-")
        self.assertEqual(curr, ["", "after", "-", "-", "-", "-", "-"])

    def test_method_only_in_current(self) -> None:
        base, curr = self.rows_for("metadata_only_new_only")
        self.assertEqual(base, ["metadata_only_new_only", "before", "-", "-", "-", "-", "-"])
        self.assertEqual(curr[2], "2 s")
        self.assertEqual(curr[3], "-")

    def test_current_order_first_then_baseline_only(self) -> None:
        methods = [row[0] for row in self.rows() if row[0]]
        self.assertEqual(
            methods,
            [
                "metadata_only_libgit_t1",
                "metadata_only_new_only",
                "limit_10_libgit",
                "metadata_only_old_only",
            ],
        )

    def test_compare_renders_markdown(self) -> None:
        table = compare(BEFORE_OUTPUT, AFTER_OUTPUT)
        lines = table.splitlines()
        self.assertIn("| Method", lines[0])
        self.assertIn("Alloc Ratio", lines[0])
        self.assertTrue(lines[1].startswith("|:"))
        self.assertIn("| metadata_only_libgit_t1 | before |", table)


if __name__ == "__main__":
    unittest.main()
