"""
Result formatter for displaying query results.

Separates presentation logic from execution logic.
"""

from typing import List, Dict, Any
from tabulate import tabulate


def format_select_result(rows: List[Dict[str, Any]]) -> str:
    """
    Format SELECT query results as an ASCII table.

    Args:
        rows: List of row dicts

    Returns:
        Formatted string with table
    """
    if not rows:
        return "(0 rows)"

    # Extract column names from first row
    columns = list(rows[0].keys())

    # Extract values in column order
    values = []
    for row in rows:
        values.append([row.get(col) for col in columns])

    # Use tabulate for pretty printing
    table = tabulate(values, headers=columns, tablefmt='grid')
    row_count = f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})"

    return table + row_count


def format_modify_result(count: int, operation: str) -> str:
    """
    Format INSERT/UPDATE/DELETE result.

    Args:
        count: Number of affected rows
        operation: Operation name ("INSERT", "UPDATE", "DELETE")

    Returns:
        Formatted string
    """
    return f"{operation} OK, {count} row{'s' if count != 1 else ''} affected"


def format_ddl_result(operation: str, object_name: str) -> str:
    """
    Format DDL statement result (CREATE TABLE, DROP TABLE, etc.).

    Args:
        operation: Operation name
        object_name: Name of object created/dropped

    Returns:
        Formatted string
    """
    return f"{operation} OK: {object_name}"
