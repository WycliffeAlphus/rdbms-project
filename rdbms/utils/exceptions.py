"""
Centralized exception hierarchy for the RDBMS.

All custom exceptions inherit from RDBMSError to provide a single base
for catching database-specific errors. This ensures consistent error
handling across all modules.
"""


class RDBMSError(Exception):
    """Base exception for all RDBMS errors."""
    pass


class TableNotFoundError(RDBMSError):
    """Raised when attempting to access a non-existent table."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        super().__init__(f"Table '{table_name}' does not exist")


class TableAlreadyExistsError(RDBMSError):
    """Raised when attempting to create a table that already exists."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        super().__init__(f"Table '{table_name}' already exists")


class ColumnNotFoundError(RDBMSError):
    """Raised when referencing a non-existent column."""

    def __init__(self, column_name: str, table_name: str = None):
        self.column_name = column_name
        self.table_name = table_name
        msg = f"Column '{column_name}' does not exist"
        if table_name:
            msg += f" in table '{table_name}'"
        super().__init__(msg)


class DuplicateKeyError(RDBMSError):
    """Raised when inserting a duplicate value into a PRIMARY KEY or UNIQUE column."""

    def __init__(self, column_name: str, value):
        self.column_name = column_name
        self.value = value
        super().__init__(
            f"Duplicate value '{value}' for column '{column_name}'"
        )


class ConstraintViolationError(RDBMSError):
    """Raised when a constraint is violated (e.g., NOT NULL, CHECK)."""

    def __init__(self, message: str):
        super().__init__(message)


class TypeValidationError(RDBMSError):
    """Raised when a value doesn't match the expected column type."""

    def __init__(self, column_name: str, expected_type: str, actual_value):
        self.column_name = column_name
        self.expected_type = expected_type
        self.actual_value = actual_value
        super().__init__(
            f"Type mismatch for column '{column_name}': "
            f"expected {expected_type}, got {type(actual_value).__name__} ({actual_value})"
        )


class SQLSyntaxError(RDBMSError):
    """Raised when SQL syntax is invalid."""

    def __init__(self, message: str, sql: str = None):
        self.sql = sql
        msg = f"SQL syntax error: {message}"
        if sql:
            msg += f"\nSQL: {sql}"
        super().__init__(msg)


class IndexNotFoundError(RDBMSError):
    """Raised when referencing a non-existent index."""

    def __init__(self, index_name: str):
        self.index_name = index_name
        super().__init__(f"Index '{index_name}' does not exist")


class InvalidIdentifierError(RDBMSError):
    """Raised when a table/column name is invalid."""

    def __init__(self, identifier: str, reason: str):
        self.identifier = identifier
        self.reason = reason
        super().__init__(f"Invalid identifier '{identifier}': {reason}")
