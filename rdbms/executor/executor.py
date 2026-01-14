"""
Query executor - executes AST nodes against the database.

Separates execution logic from:
- Parsing (parser layer)
- Storage (storage layer)
- User interaction (REPL)
"""

from typing import List, Dict, Any, Union
from ..storage.database import Database
from ..storage.table import Table
from ..storage.types import Column, ColumnConstraint, DataType
from ..parser import ast
from ..utils.exceptions import ColumnNotFoundError, TableNotFoundError
from ..utils.row_utils import project_columns, combine_rows, get_column_value
from .planner import QueryPlanner
from .evaluator import ConditionEvaluator


# Result types
QueryResult = Union[List[Dict[str, Any]], int, None]


class QueryExecutor:
    """
    Executes parsed SQL statements against a database.

    Uses composition:
    - QueryPlanner for optimization
    - ConditionEvaluator for WHERE clauses
    - Row utilities for projections and joins
    """

    def __init__(self, database: Database):
        """
        Initialize executor for a database.

        Args:
            database: Database instance to execute queries against
        """
        self.database = database
        self.planner = QueryPlanner()
        self.evaluator = ConditionEvaluator()

    def execute(self, statement: ast.Statement) -> QueryResult:
        """
        Execute an AST statement node.

        Dispatches to appropriate handler based on statement type.

        Args:
            statement: Parsed AST statement

        Returns:
            Query result:
            - List of rows for SELECT
            - Number of affected rows for INSERT/UPDATE/DELETE
            - None for DDL statements

        Raises:
            Various exceptions depending on statement execution
        """
        if isinstance(statement, ast.CreateTableStmt):
            return self._execute_create_table(statement)
        elif isinstance(statement, ast.DropTableStmt):
            return self._execute_drop_table(statement)
        elif isinstance(statement, ast.CreateIndexStmt):
            return self._execute_create_index(statement)
        elif isinstance(statement, ast.InsertStmt):
            return self._execute_insert(statement)
        elif isinstance(statement, ast.SelectStmt):
            return self._execute_select(statement)
        elif isinstance(statement, ast.UpdateStmt):
            return self._execute_update(statement)
        elif isinstance(statement, ast.DeleteStmt):
            return self._execute_delete(statement)
        else:
            raise ValueError(f"Unknown statement type: {type(statement)}")

    # ----- DDL Statements -----

    def _execute_create_table(self, stmt: ast.CreateTableStmt) -> None:
        """Execute CREATE TABLE statement."""
        # Convert AST column defs to storage Column objects
        columns = []
        for col_def in stmt.columns:
            # Parse data type
            data_type = DataType.from_string(col_def.data_type)

            # Parse constraints
            constraints = set()
            for constraint_str in col_def.constraints:
                if constraint_str == "PRIMARY KEY":
                    constraints.add(ColumnConstraint.PRIMARY_KEY)
                elif constraint_str == "UNIQUE":
                    constraints.add(ColumnConstraint.UNIQUE)
                elif constraint_str == "NOT NULL":
                    constraints.add(ColumnConstraint.NOT_NULL)

            column = Column(
                name=col_def.name,
                data_type=data_type,
                max_length=col_def.max_length,
                constraints=constraints
            )
            columns.append(column)

        # Create table in database
        self.database.create_table(stmt.table_name, columns)
        return None

    def _execute_drop_table(self, stmt: ast.DropTableStmt) -> None:
        """Execute DROP TABLE statement."""
        self.database.drop_table(stmt.table_name)
        return None

    def _execute_create_index(self, stmt: ast.CreateIndexStmt) -> None:
        """Execute CREATE INDEX statement."""
        table = self.database.get_table(stmt.table_name)
        table.create_index(stmt.column_name)
        return None

    # ----- DML Statements -----

    def _execute_insert(self, stmt: ast.InsertStmt) -> int:
        """
        Execute INSERT statement.

        Returns:
            Number of rows inserted (always 1)
        """
        table = self.database.get_table(stmt.table_name)

        # Build row dict from columns and values
        if len(stmt.columns) != len(stmt.values):
            raise ValueError("Number of columns and values must match")

        # Extract values from Literal nodes
        row_data = {}
        for col_name, value_node in zip(stmt.columns, stmt.values):
            if isinstance(value_node, ast.Literal):
                row_data[col_name] = value_node.value
            else:
                row_data[col_name] = value_node

        # Insert row (Table handles validation and constraints)
        table.insert_row(row_data)
        return 1

    def _execute_select(self, stmt: ast.SelectStmt) -> List[Dict[str, Any]]:
        """
        Execute SELECT statement.

        Handles:
        - Column projection
        - WHERE filtering (using planner for optimization)
        - INNER JOIN

        Returns:
            List of result rows
        """
        # Handle JOIN separately
        if stmt.join:
            return self._execute_select_with_join(stmt)

        # Simple SELECT from single table
        table = self.database.get_table(stmt.table_name)

        # Get matching rows (planner decides index vs scan)
        matching_rows = self.planner.get_matching_rows(table, stmt.where)

        # Project columns
        if stmt.columns == "*":
            # Return all columns
            return matching_rows
        else:
            # Project specific columns
            result = []
            for row in matching_rows:
                try:
                    projected = project_columns(row, stmt.columns)
                    result.append(projected)
                except KeyError as e:
                    raise ColumnNotFoundError(str(e), stmt.table_name)
            return result

    def _execute_select_with_join(self, stmt: ast.SelectStmt) -> List[Dict[str, Any]]:
        """
        Execute SELECT with INNER JOIN.

        Uses nested loop join algorithm (simple but works).
        """
        # Get both tables
        left_table = self.database.get_table(stmt.table_name)
        right_table = self.database.get_table(stmt.join.table_name)

        # Get join columns
        left_join_col = stmt.join.left_column
        right_join_col = stmt.join.right_column

        # Nested loop join
        result_rows = []
        for left_row in left_table.scan():
            for right_row in right_table.scan():
                # Check join condition
                try:
                    left_value = get_column_value(
                        left_row,
                        left_join_col.column_name,
                        left_join_col.table_name
                    )
                    right_value = get_column_value(
                        right_row,
                        right_join_col.column_name,
                        right_join_col.table_name
                    )

                    if left_value == right_value:
                        # Combine rows with qualified names
                        combined = combine_rows(
                            left_row,
                            right_row,
                            stmt.table_name,
                            stmt.join.table_name
                        )

                        # Apply WHERE clause if present
                        if stmt.where is None or self.evaluator.evaluate(stmt.where, combined):
                            result_rows.append(combined)

                except KeyError:
                    # Column not found - skip this combination
                    continue

        # Project columns
        if stmt.columns == "*":
            return result_rows
        else:
            # Project specific columns (handle qualified names)
            projected_results = []
            for row in result_rows:
                projected = {}
                for col_spec in stmt.columns:
                    # Handle both qualified (table.column) and unqualified names
                    if col_spec in row:
                        projected[col_spec] = row[col_spec]
                    else:
                        # Try to find it with any table prefix
                        found = False
                        for key in row:
                            if key.endswith(f".{col_spec}"):
                                projected[key] = row[key]
                                found = True
                                break
                        if not found:
                            raise ColumnNotFoundError(col_spec)
                projected_results.append(projected)
            return projected_results

    def _execute_update(self, stmt: ast.UpdateStmt) -> int:
        """
        Execute UPDATE statement.

        Uses planner to find rows efficiently.

        Returns:
            Number of rows updated
        """
        table = self.database.get_table(stmt.table_name)

        # Get rows to update (planner decides index vs scan)
        matching_rows = self.planner.get_matching_rows(table, stmt.where)

        # Update each row
        # Note: matching_rows don't have _row_id, so we need to find them again
        # This is inefficient but simple - could be optimized
        updated_count = 0
        for match_row in matching_rows:
            # Find row_id by scanning (not ideal but works for demo)
            for row in table._rows:
                if row.get('_row_id') not in table._deleted_row_ids:
                    # Check if this is our row by comparing all columns
                    is_match = all(
                        row.get(k) == v
                        for k, v in match_row.items()
                    )
                    if is_match:
                        # Update this row
                        table.update_row(row['_row_id'], stmt.assignments)
                        updated_count += 1
                        break

        return updated_count

    def _execute_delete(self, stmt: ast.DeleteStmt) -> int:
        """
        Execute DELETE statement.

        Uses planner to find rows efficiently.

        Returns:
            Number of rows deleted
        """
        table = self.database.get_table(stmt.table_name)

        # Get rows to delete
        matching_rows = self.planner.get_matching_rows(table, stmt.where)

        # Delete each row (same inefficiency as UPDATE)
        deleted_count = 0
        for match_row in matching_rows:
            for row in table._rows:
                if row.get('_row_id') not in table._deleted_row_ids:
                    is_match = all(
                        row.get(k) == v
                        for k, v in match_row.items()
                    )
                    if is_match:
                        table.delete_row(row['_row_id'])
                        deleted_count += 1
                        break

        return deleted_count
