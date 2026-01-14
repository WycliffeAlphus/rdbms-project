"""
SQL Parser using Lark.

Parses SQL strings into AST nodes for execution.
Separates parsing concerns from execution logic.
"""

import os
from pathlib import Path
from lark import Lark, Transformer, Token
from lark.exceptions import LarkError

from . import ast
from ..utils.exceptions import SQLSyntaxError


class ASTBuilder(Transformer):
    """
    Transforms Lark parse tree into our AST nodes.

    Each method corresponds to a rule in the grammar and returns
    the appropriate AST node.
    """

    # ----- Top-level -----

    def statement(self, args):
        """Unwrap statement node - just return the child."""
        return args[0]

    # ----- Data Types -----

    def type_integer(self, args):
        return "INTEGER"

    def type_int(self, args):
        return "INTEGER"

    def type_float(self, args):
        return "FLOAT"

    def type_real(self, args):
        return "FLOAT"

    def type_varchar(self, args):
        max_length = int(args[0])
        return ("VARCHAR", max_length)

    def type_text(self, args):
        return "TEXT"

    def type_boolean(self, args):
        return "BOOLEAN"

    def type_bool(self, args):
        return "BOOLEAN"

    # ----- Constraints -----

    def constraint_primary_key(self, args):
        return "PRIMARY KEY"

    def constraint_unique(self, args):
        return "UNIQUE"

    def constraint_not_null(self, args):
        return "NOT NULL"

    # ----- Column Definition -----

    def column_def(self, args):
        name = str(args[0])
        data_type_info = args[1]

        # Handle VARCHAR with length
        if isinstance(data_type_info, tuple):
            data_type, max_length = data_type_info
        else:
            data_type = data_type_info
            max_length = None

        constraints = args[2:] if len(args) > 2 else []

        return ast.ColumnDef(
            name=name,
            data_type=data_type,
            max_length=max_length,
            constraints=list(constraints)
        )

    # ----- CREATE TABLE -----

    def create_table_stmt(self, args):
        table_name = str(args[0])
        columns = args[1:]
        return ast.CreateTableStmt(table_name=table_name, columns=list(columns))

    # ----- DROP TABLE -----

    def drop_table_stmt(self, args):
        table_name = str(args[0])
        return ast.DropTableStmt(table_name=table_name)

    # ----- CREATE INDEX -----

    def create_index_stmt(self, args):
        index_name = str(args[0])
        table_name = str(args[1])
        column_name = str(args[2])
        return ast.CreateIndexStmt(
            index_name=index_name,
            table_name=table_name,
            column_name=column_name
        )

    # ----- INSERT -----

    def name_list(self, args):
        return [str(arg) for arg in args]

    def value_list(self, args):
        return list(args)

    def insert_stmt(self, args):
        table_name = str(args[0])
        columns = args[1]
        values = args[2]
        return ast.InsertStmt(
            table_name=table_name,
            columns=columns,
            values=values
        )

    # ----- SELECT -----

    def select_all(self, args):
        return "*"

    def select_columns(self, args):
        return list(args)

    def column_name(self, args):
        """Handle both qualified names and simple names in SELECT."""
        arg = args[0]
        if isinstance(arg, ast.ColumnRef):
            # Qualified name: users.id → "users.id"
            if arg.table_name:
                return f"{arg.table_name}.{arg.column_name}"
            return arg.column_name
        else:
            # Simple NAME token
            return str(arg)

    def select_stmt(self, args):
        columns = args[0]
        table_name = str(args[1])
        # With new grammar: FROM table [JOIN ...] [WHERE ...]
        # args[2] could be JOIN or WHERE (or neither)
        # args[3] could be WHERE (if JOIN present)
        join = None
        where = None

        if len(args) > 2:
            if isinstance(args[2], ast.JoinClause):
                join = args[2]
                where = args[3] if len(args) > 3 else None
            else:
                # args[2] is WHERE (no JOIN)
                where = args[2]

        return ast.SelectStmt(
            columns=columns,
            table_name=table_name,
            where=where,
            join=join
        )

    # ----- WHERE Clause -----

    def where_clause(self, args):
        return args[0]

    def condition(self, args):
        """Pass through - unwrap condition node."""
        return args[0]

    def or_term(self, args):
        """Pass through or_term node."""
        return args[0]

    def and_term(self, args):
        """Pass through and_term node."""
        return args[0]

    def primary(self, args):
        """Pass through primary node."""
        return args[0]

    def condition_and(self, args):
        return ast.LogicalCondition(
            left=args[0],
            op=ast.LogicalOp.AND,
            right=args[1]
        )

    def condition_or(self, args):
        return ast.LogicalCondition(
            left=args[0],
            op=ast.LogicalOp.OR,
            right=args[1]
        )

    def condition_parens(self, args):
        return args[0]

    def comparison(self, args):
        left = args[0]
        op = args[1]
        right = args[2]
        return ast.Comparison(left=left, op=op, right=right)

    # Comparison operators
    def op_eq(self, args):
        return ast.ComparisonOp.EQ

    def op_ne(self, args):
        return ast.ComparisonOp.NE

    def op_ne2(self, args):
        return ast.ComparisonOp.NE

    def op_lt(self, args):
        return ast.ComparisonOp.LT

    def op_gt(self, args):
        return ast.ComparisonOp.GT

    def op_lte(self, args):
        return ast.ComparisonOp.LTE

    def op_gte(self, args):
        return ast.ComparisonOp.GTE

    # ----- Expressions -----

    def expr_column(self, args):
        column_name = str(args[0])
        return ast.ColumnRef(column_name=column_name)

    def expr_qualified_column(self, args):
        return args[0]  # Already a ColumnRef from qualified_name

    def qualified_name(self, args):
        table_name = str(args[0])
        column_name = str(args[1])
        return ast.ColumnRef(column_name=column_name, table_name=table_name)

    def expr_literal(self, args):
        return args[0]

    # ----- Literals -----

    def lit_number(self, args):
        value_str = str(args[0])
        # Convert to int or float
        if '.' in value_str or 'e' in value_str.lower():
            value = float(value_str)
        else:
            value = int(value_str)
        return ast.Literal(value=value, type="NUMBER")

    def lit_string(self, args):
        # Remove quotes
        value_str = str(args[0])[1:-1]
        # Handle escape sequences
        value_str = value_str.replace("\\'", "'").replace('\\"', '"')
        return ast.Literal(value=value_str, type="STRING")

    def lit_true(self, args):
        return ast.Literal(value=True, type="BOOLEAN")

    def lit_false(self, args):
        return ast.Literal(value=False, type="BOOLEAN")

    def lit_null(self, args):
        return ast.Literal(value=None, type="NULL")

    # ----- JOIN -----

    def join_clause(self, args):
        table_name = str(args[0])
        # join_condition returns a list, so unpack it
        join_cond = args[1]
        left_column = join_cond[0]
        right_column = join_cond[1]
        return ast.JoinClause(
            table_name=table_name,
            left_column=left_column,
            right_column=right_column
        )

    def join_condition(self, args):
        # Return both qualified names as a list
        return args

    # ----- UPDATE -----

    def assignment(self, args):
        column_name = str(args[0])
        value = args[1].value  # Extract value from Literal
        return (column_name, value)

    def assignment_list(self, args):
        return dict(args)

    def update_stmt(self, args):
        table_name = str(args[0])
        assignments = args[1]
        where = args[2] if len(args) > 2 else None
        return ast.UpdateStmt(
            table_name=table_name,
            assignments=assignments,
            where=where
        )

    # ----- DELETE -----

    def delete_stmt(self, args):
        table_name = str(args[0])
        where = args[1] if len(args) > 1 else None
        return ast.DeleteStmt(table_name=table_name, where=where)


class SQLParser:
    """
    SQL Parser facade.

    Provides simple interface for parsing SQL strings into AST nodes.
    """

    def __init__(self):
        """Initialize parser with grammar."""
        grammar_path = Path(__file__).parent / "grammar.lark"
        with open(grammar_path, 'r') as f:
            grammar = f.read()

        self._parser = Lark(
            grammar,
            start='start',
            parser='lalr'  # Fast LALR parser
        )
        self._transformer = ASTBuilder()

    def parse(self, sql: str) -> ast.Statement:
        """
        Parse SQL string into AST node.

        Args:
            sql: SQL statement string

        Returns:
            AST node representing the statement

        Raises:
            SQLSyntaxError: If SQL syntax is invalid
        """
        try:
            # Strip trailing semicolon if present
            sql = sql.strip().rstrip(';')
            # Parse and transform
            tree = self._parser.parse(sql)
            return self._transformer.transform(tree)
        except LarkError as e:
            raise SQLSyntaxError(str(e), sql)
