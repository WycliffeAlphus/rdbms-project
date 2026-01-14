"""
Row manipulation utilities to avoid code duplication.

These functions are reused across executor operations (SELECT, UPDATE, DELETE, JOIN).
Single source of truth for row operations.
"""

from typing import Dict, Any, List


def project_columns(row: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
    """
    Extract specified columns from a row.

    Used by SELECT to return only requested columns.

    Args:
        row: Full row dict
        columns: List of column names to extract

    Returns:
        New dict with only specified columns

    Example:
        row = {'id': 1, 'name': 'Alice', 'age': 30}
        project_columns(row, ['id', 'name']) -> {'id': 1, 'name': 'Alice'}
    """
    return {col: row[col] for col in columns if col in row}


def combine_rows(
    left_row: Dict[str, Any],
    right_row: Dict[str, Any],
    left_table: str,
    right_table: str
) -> Dict[str, Any]:
    """
    Merge two rows from different tables with table-qualified column names.

    Used by JOIN operations to combine rows from two tables.

    Args:
        left_row: Row from left table
        right_row: Row from right table
        left_table: Name of left table (for prefixing)
        right_table: Name of right table (for prefixing)

    Returns:
        Combined dict with qualified column names (table.column)

    Example:
        left = {'id': 1, 'name': 'Alice'}
        right = {'id': 10, 'title': 'Post'}
        combine_rows(left, right, 'users', 'posts')
        -> {'users.id': 1, 'users.name': 'Alice', 'posts.id': 10, 'posts.title': 'Post'}
    """
    combined = {}

    # Add left table columns with prefix
    for col_name, value in left_row.items():
        combined[f"{left_table}.{col_name}"] = value

    # Add right table columns with prefix
    for col_name, value in right_row.items():
        combined[f"{right_table}.{col_name}"] = value

    return combined


def get_column_value(row: Dict[str, Any], column_name: str, table_name: str = None) -> Any:
    """
    Get a column value from a row, handling qualified names.

    Used by condition evaluator and JOIN operations.

    Args:
        row: Row dict (may have qualified or unqualified column names)
        column_name: Column name to retrieve
        table_name: Optional table name qualifier

    Returns:
        Column value

    Raises:
        KeyError: If column not found
    """
    # Try qualified name first if table name provided
    if table_name:
        qualified = f"{table_name}.{column_name}"
        if qualified in row:
            return row[qualified]

    # Try unqualified name
    if column_name in row:
        return row[column_name]

    # Not found
    raise KeyError(f"Column '{column_name}' not found in row")


def has_column(row: Dict[str, Any], column_name: str, table_name: str = None) -> bool:
    """
    Check if a row has a column, handling qualified names.

    Args:
        row: Row dict
        column_name: Column name to check
        table_name: Optional table name qualifier

    Returns:
        True if column exists in row
    """
    if table_name:
        qualified = f"{table_name}.{column_name}"
        if qualified in row:
            return True

    return column_name in row
