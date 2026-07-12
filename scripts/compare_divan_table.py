#!/usr/bin/env python3
"""Compare two divan benchmark runs as a BenchmarkDotNet-style baseline table."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from format_divan_table import (
    BenchRow,
    duration_to_ns,
    parse_divan_output,
    render_markdown_table,
    size_to_bytes,
)

HEADERS = ["Method", "Job", "Mean", "Ratio", "Allocs", "Allocated", "Alloc Ratio"]
ALIGNS = ["l", "l", "r", "r", "r", "r", "r"]


def ratio_cells(
    baseline: str | None, current: str | None, to_number
) -> tuple[str, str]:
    base_value = to_number(baseline) if baseline else None
    current_value = to_number(current) if current else None
    if base_value is None or current_value is None or base_value == 0:
        return "-", "-"
    return "1.00", f"{current_value / base_value:.2f}"


def method_order(
    baseline_rows: list[BenchRow], current_rows: list[BenchRow]
) -> list[str]:
    seen = set()
    methods = []
    for row in [*current_rows, *baseline_rows]:
        if row.method not in seen:
            seen.add(row.method)
            methods.append(row.method)
    return methods


def build_comparison_rows(
    baseline_rows: list[BenchRow],
    current_rows: list[BenchRow],
    baseline_label: str,
    current_label: str,
) -> list[list[str]]:
    baseline_map = {row.method: row for row in baseline_rows}
    current_map = {row.method: row for row in current_rows}

    table_rows: list[list[str]] = []
    for method in method_order(baseline_rows, current_rows):
        base = baseline_map.get(method)
        curr = current_map.get(method)
        base_ratio, curr_ratio = ratio_cells(
            base.mean if base else None,
            curr.mean if curr else None,
            duration_to_ns,
        )
        base_alloc_ratio, curr_alloc_ratio = ratio_cells(
            base.allocated if base else None,
            curr.allocated if curr else None,
            size_to_bytes,
        )
        table_rows.append([
            method,
            baseline_label,
            (base.mean or "-") if base else "-",
            base_ratio,
            base.allocs if base else "-",
            base.allocated if base else "-",
            base_alloc_ratio,
        ])
        table_rows.append([
            "",
            current_label,
            (curr.mean or "-") if curr else "-",
            curr_ratio,
            curr.allocs if curr else "-",
            curr.allocated if curr else "-",
            curr_alloc_ratio,
        ])
    return table_rows


def compare(
    baseline_text: str,
    current_text: str,
    baseline_label: str = "before",
    current_label: str = "after",
) -> str:
    baseline_rows = parse_divan_output(baseline_text)
    current_rows = parse_divan_output(current_text)
    table_rows = build_comparison_rows(
        baseline_rows, current_rows, baseline_label, current_label
    )
    return render_markdown_table(HEADERS, table_rows, ALIGNS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="raw divan output of the baseline run")
    parser.add_argument("current", type=Path, help="raw divan output of the current run")
    parser.add_argument("--baseline-label", default="before")
    parser.add_argument("--current-label", default="after")
    args = parser.parse_args()

    baseline_text = args.baseline.read_text()
    current_text = args.current.read_text()
    if not parse_divan_output(baseline_text):
        print(f"error: no benchmark rows found in {args.baseline}", file=sys.stderr)
        return 1
    if not parse_divan_output(current_text):
        print(f"error: no benchmark rows found in {args.current}", file=sys.stderr)
        return 1

    print(compare(baseline_text, current_text, args.baseline_label, args.current_label))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
