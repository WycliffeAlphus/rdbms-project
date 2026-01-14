"""
Column data type definitions and validation.

The Column class is the single source of truth for column metadata
and type validation, used throughout storage and execution layers.
"""

from typing import Any, Optional, Set
from enum import Enum

from ..utils.validators import validate_identifier, validate_value_for_type
from ..utils.exceptions import TypeValidationError, ConstraintViolationError


class DataType(Enum):
    """Supported column data types."""
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"

    @classmethod
    def from_string(cls, type_str: str) -> 'DataType':
        """Convert string representation to DataType enum."""
        type_str = type_str.upper()
        # Handle aliases
        if type_str == 'INT':
            return cls.INTEGER
        elif type_str == 'REAL':
            return cls.FLOAT
        elif type_str == 'BOOL':
            return cls.BOOLEAN
        else:
            return cls[type_str]


class ColumnConstraint(Enum):
    """Supported column constraints."""
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT NULL"


class Column:
    """
    Represents a table column with type and constraints.

    This class is the single source of truth for:
    - Column metadata (name, type, constraints)
    - Value validation logic
    - Constraint checking
    """

    def __init__(
        self,
        name: str,
        data_type: DataType,
        max_length: Optional[int] = None,
        constraints: Optional[Set[ColumnConstraint]] = None
    ):
        """
        Initialize a column.

        Args:
            name: Column name (validated)
            data_type: Data type enum
            max_length: Maximum length for VARCHAR types
            constraints: Set of column constraints
        """
        validate_identifier(name)
        self.name = name
        self.data_type = data_type
        self.max_length = max_length
        self.constraints = constraints or set()

    @property
    def is_primary_key(self) -> bool:
        """Check if this column is a primary key."""
        return ColumnConstraint.PRIMARY_KEY in self.constraints

    @property
    def is_unique(self) -> bool:
        """Check if this column has UNIQUE constraint."""
        return ColumnConstraint.UNIQUE in self.constraints or self.is_primary_key

    @property
    def is_not_null(self) -> bool:
        """Check if this column has NOT NULL constraint."""
        return ColumnConstraint.NOT_NULL in self.constraints or self.is_primary_key

    def validate(self, value: Any, column_name_for_error: str = None) -> None:
        """
        Validate a value against this column's type and constraints.

        This is the SINGLE SOURCE OF TRUTH for validation logic.
        Used by Table, Executor, and anywhere else values are checked.

        Args:
            value: The value to validate
            column_name_for_error: Optional column name for error messages

        Raises:
            TypeValidationError: If type doesn't match
            ConstraintViolationError: If constraints are violated
        """
        col_name = column_name_for_error or self.name

        # Check NOT NULL constraint
        if value is None:
            if self.is_not_null:
                raise ConstraintViolationError(
                    f"Column '{col_name}' cannot be NULL"
                )
            return  # NULL is valid if not NOT NULL

        # Validate type using centralized validator
        try:
            validate_value_for_type(
                value,
                self.data_type.value,
                self.max_length
            )
        except TypeValidationError as e:
            # Add column name to error
            raise TypeValidationError(col_name, self.data_type.value, value)

    def to_dict(self) -> dict:
        """
        Serialize column to dictionary.

        Used for schema introspection and persistence.
        """
        return {
            'name': self.name,
            'data_type': self.data_type.value,
            'max_length': self.max_length,
            'constraints': [c.value for c in self.constraints]
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Column':
        """Deserialize column from dictionary."""
        return cls(
            name=data['name'],
            data_type=DataType[data['data_type']],
            max_length=data.get('max_length'),
            constraints={ColumnConstraint(c) for c in data.get('constraints', [])}
        )

    def __repr__(self) -> str:
        constraints_str = ', '.join(c.value for c in self.constraints)
        type_str = self.data_type.value
        if self.max_length:
            type_str += f"({self.max_length})"
        if constraints_str:
            return f"Column({self.name}, {type_str}, {constraints_str})"
        return f"Column({self.name}, {type_str})"
