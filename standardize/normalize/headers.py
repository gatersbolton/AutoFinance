from __future__ import annotations

from typing import Dict, List


def build_header_paths(grid, header_row_count: int) -> Dict[int, List[str]]:
    """Build multi-row header paths from the top rows of a logical subtable."""

    if not grid:
        return {}

    column_count = len(grid[0])
    paths: Dict[int, List[str]] = {}
    for col_idx in range(column_count):
        path: List[str] = []
        seen = set()
        for row_idx in range(min(header_row_count, len(grid))):
            text = grid[row_idx][col_idx].text_clean
            if text and text not in seen:
                path.append(text)
                seen.add(text)
        paths[col_idx] = path
    return paths


def joined_header_path(path: List[str]) -> str:
    return " / ".join(item for item in path if item)
