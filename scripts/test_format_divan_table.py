"""Tests for scripts/format_divan_table.py."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from format_divan_table import convert, duration_to_ns, parse_divan_output, size_to_bytes


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

    def test_duration_to_ns(self) -> None:
        self.assertEqual(duration_to_ns("76.99 ms"), 76.99e6)
        self.assertEqual(duration_to_ns("1.061 s"), 1.061e9)
        self.assertEqual(duration_to_ns("21 ns"), 21.0)
        self.assertEqual(duration_to_ns("3.5 µs"), 3.5e3)
        self.assertIsNone(duration_to_ns("-"))
        self.assertIsNone(duration_to_ns(""))

    def test_size_to_bytes(self) -> None:
        self.assertEqual(size_to_bytes("449.7 KB"), 449.7e3)
        self.assertEqual(size_to_bytes("63.54 MB"), 63.54e6)
        self.assertEqual(size_to_bytes("5.119 GB"), 5.119e9)
        self.assertEqual(size_to_bytes("8 B"), 8.0)
        self.assertIsNone(size_to_bytes("-"))
        self.assertIsNone(size_to_bytes(""))


if __name__ == "__main__":
    unittest.main()
