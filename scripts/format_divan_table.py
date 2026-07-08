#!/usr/bin/env python3
"""Convert divan tree benchmark output into a BenchmarkDotNet-style flat table."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field

DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(ns|µs|us|ms|s)\s*$")
BRANCH_RE = re.compile(r"^[├╰│\s─]+")
COLUMN_SPLIT_RE = re.compile(r"\s+│\s+")


@dataclass
class BenchRow:
    method: str
    mean: str = ""
    allocs: str = "-"
    allocated: str = "-"


@dataclass
class ParserState:
    name_stack: list[str] = field(default_factory=list)
    rows: list[BenchRow] = field(default_factory=list)
    pending: BenchRow | None = None
    awaiting_max_alloc: int = 0


def tree_depth(line: str) -> int:
    depth = 0
    i = 0
    while i < len(line):
        if line.startswith("│  ", i) or line.startswith("   ", i):
            depth += 1
            i += 3
        else:
            break
    return depth


def strip_tree_prefix(text: str) -> str:
    return BRANCH_RE.sub("", text).strip()


def extract_duration(text: str) -> tuple[str, str]:
    stripped = text.strip()
    match = DURATION_RE.search(stripped)
    if not match:
        return stripped, ""
    duration = f"{match.group(1)} {match.group(2)}"
    label = stripped[: match.start()].strip()
    return label, duration


def split_columns(line: str) -> list[str]:
    if " │ " not in line:
        return []
    return [part.strip() for part in COLUMN_SPLIT_RE.split(line)]


def is_stats_row(columns: list[str]) -> bool:
    if len(columns) < 4:
        return False
    return any(DURATION_RE.search(col) for col in columns[:4])


def mean_column(columns: list[str]) -> str:
    if len(columns) < 4:
        return ""
    _, duration = extract_duration(columns[3])
    return duration


def numeric_mean_column(columns: list[str]) -> str:
    if len(columns) < 4:
        return ""
    value = columns[3].strip()
    if not value or value == "0":
        return ""
    return value


def size_mean_column(columns: list[str]) -> str:
    if len(columns) < 4:
        return ""
    value = columns[3].strip()
    if not value or value in {"0 B", "0"}:
        return ""
    return value


def set_stack_depth(stack: list[str], depth: int, name: str) -> None:
    if depth < len(stack):
        del stack[depth:]
    if depth == len(stack):
        stack.append(name)
    else:
        stack[depth] = name


def method_name(stack: list[str], leaf_label: str) -> str:
    return "_".join([*stack, leaf_label])


def branch_label(line: str) -> str | None:
    head = line.split(" │ ", 1)[0]
    match = re.search(r"[├╰]─\s+(.+)$", head)
    if match is None:
        return None
    return match.group(1).strip()


def parse_line(line: str, state: ParserState) -> None:
    stripped = line.rstrip("\n")
    if not stripped.strip():
        return

    if stripped.startswith("Timer precision:"):
        return

    columns = split_columns(stripped)
    depth = tree_depth(stripped)

    if "max alloc:" in stripped:
        state.awaiting_max_alloc = 2
        return

    if state.awaiting_max_alloc > 0 and columns:
        if state.pending is None:
            state.awaiting_max_alloc = 0
            return

        if state.awaiting_max_alloc == 2:
            allocs = numeric_mean_column(columns)
            if allocs:
                state.pending.allocs = allocs
            state.awaiting_max_alloc = 1
            return

        if state.awaiting_max_alloc == 1:
            allocated = size_mean_column(columns)
            if allocated:
                state.pending.allocated = allocated
            state.rows.append(state.pending)
            state.pending = None
            state.awaiting_max_alloc = 0
        return

    label = branch_label(stripped)
    if label is None:
        return

    if label == "(ignored)":
        return

    if is_stats_row(columns):
        leaf_label = strip_tree_prefix(extract_duration(columns[0])[0])
        if not leaf_label:
            leaf_label = strip_tree_prefix(label.split("│", 1)[0])
            leaf_label = extract_duration(leaf_label)[0]
        if not leaf_label:
            return
        state.pending = BenchRow(
            method=method_name(state.name_stack[:depth], leaf_label),
            mean=mean_column(columns),
        )
        return

    # Branch-only line (group header).
    name = label
    if " fastest" in name:
        name = name.split(" fastest", 1)[0].strip()
    name = extract_duration(name)[0]
    if name:
        set_stack_depth(state.name_stack, depth, name)


def parse_divan_output(text: str) -> list[BenchRow]:
    state = ParserState()
    for line in text.splitlines():
        parse_line(line, state)

    if state.pending is not None:
        state.rows.append(state.pending)
        state.pending = None

    return state.rows


def format_table(rows: list[BenchRow]) -> str:
    headers = ["Method", "Mean", "StdDev", "Allocs", "Allocated"]
    table_rows = [
        [
            row.method,
            row.mean or "-",
            "-",
            row.allocs,
            row.allocated,
        ]
        for row in rows
    ]

    widths = [len(header) for header in headers]
    for row in table_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(cells: list[str], aligns: list[str]) -> str:
        parts: list[str] = []
        for i, cell in enumerate(cells):
            if aligns[i] == "r":
                parts.append(cell.rjust(widths[i]))
            else:
                parts.append(cell.ljust(widths[i]))
        return "| " + " | ".join(parts) + " |"

    aligns = ["l", "r", "r", "r", "r"]
    header_line = fmt_row(headers, aligns)
    separator_parts: list[str] = []
    for i, width in enumerate(widths):
        dash = "-" * max(width, 3)
        separator_parts.append(f"{dash}:" if aligns[i] == "r" else f":{dash}")
    separator_line = "|" + "|".join(separator_parts) + "|"

    lines = [header_line, separator_line]
    lines.extend(fmt_row(row, aligns) for row in table_rows)
    return "\n".join(lines)


def convert(text: str) -> str:
    rows = parse_divan_output(text)
    if not rows:
        return text
    return format_table(rows)


def main() -> int:
    text = sys.stdin.read()
    sys.stdout.write(convert(text))
    if text and not text.endswith("\n") and parse_divan_output(text):
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
