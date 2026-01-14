"""
Query planner for optimizing query execution.

Single source of truth for deciding how to execute queries:
- Use index for fast lookup
- Fall back to table scan

This logic is reused by SELECT, UPDATE, DELETE to avoid duplication.
"""

from typing import List, Dict, Any, Optional
from ..storage.table import Table
from ..parser.ast import Condition, Comparison, ComparisonOp, ColumnRef, Literal
from .evaluator import ConditionEvaluator


class QueryPlanner:
    """
    Plans query execution strategy.

    Decides whether to use indexes or table scans based on:
    - Available indexes
    - WHERE clause structure
    - Column references
    """

    def __init__(self):
        self.evaluator = ConditionEvaluator()

    def get_matching_rows(
        self,
        table: Table,
        where: Optional[Condition]
    ) -> List[Dict[str, Any]]:
        """
        Get rows that match WHERE condition.

        This is the SINGLE place where we decide to use indexes or table scans.
        Reused by SELECT, UPDATE, DELETE.

        Args:
            table: Table to query
            where: WHERE condition (None = all rows)

        Returns:
            List of matching rows (without internal _row_id)
        """
        # No WHERE clause = return all rows
        if where is None:
            return list(table.scan())

        # Try to use index for simple equality comparisons
        rows_to_check = self._get_candidate_rows(table, where)

        # If we got candidates from index, filter them
        # If not, we got all rows from table scan - filter them
        matching_rows = []
        for row in rows_to_check:
            if self.evaluator.evaluate(where, row):
                matching_rows.append(row)

        return matching_rows

    def _get_candidate_rows(
        self,
        table: Table,
        condition: Condition
    ) -> List[Dict[str, Any]]:
        """
        Get candidate rows, using index if possible.

        Attempts to use index for simple cases:
        - column = value
        - column = value AND other_conditions

        Otherwise falls back to full table scan.

        Args:
            table: Table to query
            condition: WHERE condition

        Returns:
            List of candidate rows to check
        """
        # Try to extract simple equality condition we can use an index for
        index_column, index_value = self._find_index_opportunity(table, condition)

        if index_column and index_value is not None:
            # Use index lookup
            index = table.get_index(index_column)
            if index:
                row_ids = index.search(index_value)
                # Get actual rows
                rows = []
                for row_id in row_ids:
                    row = table.get_row(row_id)
                    if row is not None:
                        rows.append(row)
                return rows

        # Fall back to full table scan
        return list(table.scan())

    def _find_index_opportunity(
        self,
        table: Table,
        condition: Condition
    ) -> tuple[Optional[str], Optional[Any]]:
        """
        Look for a simple equality comparison on an indexed column.

        Checks if the condition (or part of it) is:
        - column = literal
        - Where column has an index

        Returns:
            (column_name, value) if found, (None, None) otherwise
        """
        if isinstance(condition, Comparison):
            # Check if this is column = value where column is indexed
            if condition.op == ComparisonOp.EQ:
                left = condition.left
                right = condition.right

                # Case 1: column = literal
                if isinstance(left, ColumnRef) and isinstance(right, Literal):
                    if table.has_index(left.column_name):
                        return (left.column_name, right.value)

                # Case 2: literal = column
                if isinstance(left, Literal) and isinstance(right, ColumnRef):
                    if table.has_index(right.column_name):
                        return (right.column_name, left.value)

        # For complex conditions (AND, OR), we could be smarter,
        # but for simplicity we'll just do table scans
        # Future optimization: extract indexed condition from AND clause

        return (None, None)

    def can_use_index(self, table: Table, condition: Optional[Condition]) -> bool:
        """
        Check if an index can be used for this condition.

        Useful for query analysis and testing.

        Args:
            table: Table to query
            condition: WHERE condition

        Returns:
            True if index can be used
        """
        if condition is None:
            return False

        column, value = self._find_index_opportunity(table, condition)
        return column is not None and value is not None
