"""
Abstract Syntax Tree (AST) node definitions.

These dataclasses represent parsed SQL statements in a structured form,
decoupling the parser from the executor. The executor works with these
AST nodes rather than raw parse trees.
"""

from dataclasses import dataclass
from typing import List, Optional, Any
from enum import Enum


# ----- Enums -----

class ComparisonOp(Enum):
    """Comparison operators for WHERE clauses."""
    EQ = "="
    NE = "!="
    LT = "<"
    GT = ">"
    LTE = "<="
    GTE = ">="


class LogicalOp(Enum):
    """Logical operators for combining conditions."""
    AND = "AND"
    OR = "OR"


# ----- Column Definition Nodes -----

@dataclass
class ColumnDef:
    """Column definition in CREATE TABLE."""
    name: str
    data_type: str  # "INTEGER", "VARCHAR", etc.
    max_length: Optional[int] = None  # For VARCHAR(n)
    constraints: List[str] = None  # ["PRIMARY KEY", "UNIQUE", "NOT NULL"]

    def __post_init__(self):
        if self.constraints is None:
            self.constraints = []


# ----- Expression Nodes -----

@dataclass
class ColumnRef:
    """Reference to a column, optionally qualified with table name."""
    column_name: str
    table_name: Optional[str] = None

    def __str__(self):
        if self.table_name:
            return f"{self.table_name}.{self.column_name}"
        return self.column_name


@dataclass
class Literal:
    """Literal value (number, string, boolean, null)."""
    value: Any
    type: str  # "NUMBER", "STRING", "BOOLEAN", "NULL"


@dataclass
class Comparison:
    """Binary comparison: left op right."""
    left: 'Expr'
    op: ComparisonOp
    right: 'Expr'


@dataclass
class LogicalCondition:
    """Logical combination of conditions: left AND/OR right."""
    left: 'Condition'
    op: LogicalOp
    right: 'Condition'


# Type aliases for clarity
Expr = ColumnRef | Literal
Condition = Comparison | LogicalCondition


# ----- Statement Nodes -----

@dataclass
class CreateTableStmt:
    """CREATE TABLE statement."""
    table_name: str
    columns: List[ColumnDef]


@dataclass
class DropTableStmt:
    """DROP TABLE statement."""
    table_name: str


@dataclass
class CreateIndexStmt:
    """CREATE INDEX statement."""
    index_name: str
    table_name: str
    column_name: str


@dataclass
class InsertStmt:
    """INSERT INTO statement."""
    table_name: str
    columns: List[str]
    values: List[Any]


@dataclass
class SelectStmt:
    """SELECT statement."""
    columns: List[str] | str  # List of column names or "*"
    table_name: str
    where: Optional[Condition] = None
    join: Optional['JoinClause'] = None


@dataclass
class JoinClause:
    """INNER JOIN clause."""
    table_name: str
    left_column: ColumnRef  # Qualified column from left table
    right_column: ColumnRef  # Qualified column from right table


@dataclass
class UpdateStmt:
    """UPDATE statement."""
    table_name: str
    assignments: dict[str, Any]  # column_name -> new_value
    where: Optional[Condition] = None


@dataclass
class DeleteStmt:
    """DELETE FROM statement."""
    table_name: str
    where: Optional[Condition] = None


# Type alias for any statement
Statement = (
    CreateTableStmt | DropTableStmt | CreateIndexStmt |
    InsertStmt | SelectStmt | UpdateStmt | DeleteStmt
)
