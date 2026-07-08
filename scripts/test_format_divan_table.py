"""Tests for scripts/format_divan_table.py."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from format_divan_table import convert, parse_divan_output


SAMPLE_OUTPUT = """\
Timer precision: 100 ps
git_log_with_duckdb     fastest │ slowest │ median │ mean │ samples │ iters
├─ metadata_only                      │         │        │      │         │
│  ├─ libgit_t1     12.3 ms │ 13.1 ms │ 12.5 ms │ 12.6 ms │ 10 │ 10
│  │                   max alloc:
│  │                     856 │ 900 │ 856 │ 856 │         │
│  │                    125 KB │ 130 KB │ 125 KB │ 125 KB │         │
│  ├─ libgit_t2     6.1 ms │ 6.8 ms │ 6.2 ms │ 6.3 ms │ 10 │ 10
│  │                   max alloc:
│  │                     400 │ 420 │ 400 │ 400 │         │
│  │                     64 KB │ 66 KB │ 64 KB │ 64 KB │         │
╰─ limit_10                           │         │        │      │         │
   ├─ libgit        45.6 ms │ 46.1 ms │ 45.7 ms │ 45.8 ms │ 10 │ 10
"""


class FormatDivanTableTest(unittest.TestCase):
    def test_parse_rows(self) -> None:
        rows = parse_divan_output(SAMPLE_OUTPUT)
        self.assertEqual(len(rows), 3)

        self.assertEqual(rows[0].method, "metadata_only_libgit_t1")
        self.assertEqual(rows[0].mean, "12.6 ms")
        self.assertEqual(rows[0].allocs, "856")
        self.assertEqual(rows[0].allocated, "125 KB")

        self.assertEqual(rows[1].method, "metadata_only_libgit_t2")
        self.assertEqual(rows[1].mean, "6.3 ms")
        self.assertEqual(rows[1].allocs, "400")
        self.assertEqual(rows[1].allocated, "64 KB")

        self.assertEqual(rows[2].method, "limit_10_libgit")
        self.assertEqual(rows[2].mean, "45.8 ms")
        self.assertEqual(rows[2].allocs, "-")
        self.assertEqual(rows[2].allocated, "-")

    def test_convert_to_table(self) -> None:
        table = convert(SAMPLE_OUTPUT)
        self.assertIn("| Method", table)
        self.assertIn("metadata_only_libgit_t1", table)
        self.assertIn("12.6 ms", table)
        self.assertIn("125 KB", table)
        self.assertIn("StdDev", table)

    def test_passthrough_when_no_rows(self) -> None:
        text = "compiling crate v0.1.0\n"
        self.assertEqual(convert(text), text)


if __name__ == "__main__":
    unittest.main()
