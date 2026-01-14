"""
Condition evaluator for WHERE clauses.

Single source of truth for evaluating conditions against rows.
Used by SELECT, UPDATE, DELETE to filter rows.
"""

from typing import Dict, Any
from ..parser.ast import (
    Condition, Comparison, LogicalCondition,
    ComparisonOp, LogicalOp,
    ColumnRef, Literal
)
from ..utils.row_utils import get_column_value, has_column
from ..utils.exceptions import ColumnNotFoundError


class ConditionEvaluator:
    """
    Evaluates WHERE clause conditions against rows.

    This is the single implementation used everywhere conditions are evaluated,
    preventing duplication across SELECT, UPDATE, DELETE operations.
    """

    def evaluate(self, condition: Condition, row: Dict[str, Any]) -> bool:
        """
        Evaluate a condition against a row.

        Args:
            condition: AST condition node (Comparison or LogicalCondition)
            row: Row dict to evaluate against

        Returns:
            True if condition is satisfied, False otherwise

        Raises:
            ColumnNotFoundError: If referenced column doesn't exist
        """
        if isinstance(condition, Comparison):
            return self._evaluate_comparison(condition, row)
        elif isinstance(condition, LogicalCondition):
            return self._evaluate_logical(condition, row)
        else:
            raise ValueError(f"Unknown condition type: {type(condition)}")

    def _evaluate_comparison(self, comp: Comparison, row: Dict[str, Any]) -> bool:
        """
        Evaluate a binary comparison: left op right.

        Args:
            comp: Comparison AST node
            row: Row to evaluate against

        Returns:
            Boolean result of comparison
        """
        left_value = self._get_value(comp.left, row)
        right_value = self._get_value(comp.right, row)

        # Handle NULL comparisons
        if left_value is None or right_value is None:
            # In SQL, NULL comparisons are special
            # NULL = NULL is NULL (false in boolean context)
            # For simplicity, we'll say NULL != anything (including NULL)
            if comp.op == ComparisonOp.NE:
                return left_value != right_value
            else:
                return False

        # Perform comparison
        if comp.op == ComparisonOp.EQ:
            return left_value == right_value
        elif comp.op == ComparisonOp.NE:
            return left_value != right_value
        elif comp.op == ComparisonOp.LT:
            return left_value < right_value
        elif comp.op == ComparisonOp.GT:
            return left_value > right_value
        elif comp.op == ComparisonOp.LTE:
            return left_value <= right_value
        elif comp.op == ComparisonOp.GTE:
            return left_value >= right_value
        else:
            raise ValueError(f"Unknown comparison operator: {comp.op}")

    def _evaluate_logical(self, logic: LogicalCondition, row: Dict[str, Any]) -> bool:
        """
        Evaluate a logical condition: left AND/OR right.

        Args:
            logic: LogicalCondition AST node
            row: Row to evaluate against

        Returns:
            Boolean result of logical operation
        """
        left_result = self.evaluate(logic.left, row)

        # Short-circuit evaluation
        if logic.op == LogicalOp.AND:
            if not left_result:
                return False  # No need to evaluate right
            return self.evaluate(logic.right, row)
        elif logic.op == LogicalOp.OR:
            if left_result:
                return True  # No need to evaluate right
            return self.evaluate(logic.right, row)
        else:
            raise ValueError(f"Unknown logical operator: {logic.op}")

    def _get_value(self, expr, row: Dict[str, Any]) -> Any:
        """
        Extract value from an expression (ColumnRef or Literal).

        Args:
            expr: Expression AST node
            row: Row dict

        Returns:
            The value (from column or literal)

        Raises:
            ColumnNotFoundError: If column doesn't exist in row
        """
        if isinstance(expr, ColumnRef):
            # Get value from row
            try:
                return get_column_value(row, expr.column_name, expr.table_name)
            except KeyError:
                raise ColumnNotFoundError(expr.column_name)

        elif isinstance(expr, Literal):
            # Return literal value directly
            return expr.value

        else:
            raise ValueError(f"Unknown expression type: {type(expr)}")


# Convenience function for common use case
def evaluate_condition(condition: Condition, row: Dict[str, Any]) -> bool:
    """
    Convenience function to evaluate a condition without creating an evaluator.

    Args:
        condition: Condition AST node
        row: Row to evaluate

    Returns:
        True if condition satisfied
    """
    evaluator = ConditionEvaluator()
    return evaluator.evaluate(condition, row)
