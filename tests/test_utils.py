"""
Unit tests for utility modules (validators, row_utils, exceptions).
"""

import pytest
from rdbms.utils.validators import (
    validate_identifier,
    validate_value_for_type,
    coerce_value_to_type
)
from rdbms.utils.row_utils import (
    project_columns,
    combine_rows,
    get_column_value,
    has_column
)
from rdbms.utils.exceptions import (
    RDBMSError,
    InvalidIdentifierError,
    TypeValidationError
)


class TestValidators:
    """Test validator functions."""

    def test_validate_identifier_valid(self):
        """Test valid identifiers."""
        assert validate_identifier("user") is True
        assert validate_identifier("user_table") is True
        assert validate_identifier("User123") is True
        assert validate_identifier("_private") is True

    def test_validate_identifier_empty(self):
        """Test empty identifier raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_identifier("")

    def test_validate_identifier_too_long(self):
        """Test identifier over 64 characters raises error."""
        long_name = "a" * 65
        with pytest.raises(InvalidIdentifierError):
            validate_identifier(long_name)

    def test_validate_identifier_reserved_word(self):
        """Test reserved SQL keywords raise error."""
        reserved = ["SELECT", "FROM", "WHERE", "INSERT", "TABLE"]
        for word in reserved:
            with pytest.raises(InvalidIdentifierError):
                validate_identifier(word)

    def test_validate_identifier_invalid_chars(self):
        """Test invalid characters raise error."""
        invalid = ["user-table", "user@table", "123user", "user table"]
        for name in invalid:
            with pytest.raises(InvalidIdentifierError):
                validate_identifier(name)

    def test_validate_value_integer(self):
        """Test integer validation."""
        assert validate_value_for_type(42, "INTEGER") is True
        assert validate_value_for_type(0, "INT") is True
        assert validate_value_for_type(-100, "INTEGER") is True

        with pytest.raises(TypeValidationError):
            validate_value_for_type("not a number", "INTEGER")

        with pytest.raises(TypeValidationError):
            validate_value_for_type(3.14, "INTEGER")

    def test_validate_value_float(self):
        """Test float validation."""
        assert validate_value_for_type(3.14, "FLOAT") is True
        assert validate_value_for_type(42, "FLOAT") is True  # Integers acceptable
        assert validate_value_for_type(0.0, "REAL") is True

        with pytest.raises(TypeValidationError):
            validate_value_for_type("not a number", "FLOAT")

    def test_validate_value_boolean(self):
        """Test boolean validation."""
        assert validate_value_for_type(True, "BOOLEAN") is True
        assert validate_value_for_type(False, "BOOL") is True

        with pytest.raises(TypeValidationError):
            validate_value_for_type(1, "BOOLEAN")  # Not a bool

        with pytest.raises(TypeValidationError):
            validate_value_for_type("true", "BOOLEAN")

    def test_validate_value_varchar(self):
        """Test VARCHAR validation."""
        assert validate_value_for_type("hello", "VARCHAR", max_length=10) is True

        with pytest.raises(TypeValidationError):
            validate_value_for_type("too long string", "VARCHAR", max_length=5)

        with pytest.raises(TypeValidationError):
            validate_value_for_type(123, "VARCHAR")

    def test_validate_value_text(self):
        """Test TEXT validation."""
        assert validate_value_for_type("short", "TEXT") is True
        assert validate_value_for_type("very " * 1000 + "long", "TEXT") is True

        with pytest.raises(TypeValidationError):
            validate_value_for_type(123, "TEXT")

    def test_validate_value_null(self):
        """Test NULL values are accepted."""
        assert validate_value_for_type(None, "INTEGER") is True
        assert validate_value_for_type(None, "VARCHAR") is True
        assert validate_value_for_type(None, "BOOLEAN") is True

    def test_coerce_value_integer(self):
        """Test coercing values to INTEGER."""
        assert coerce_value_to_type("42", "INTEGER") == 42
        assert coerce_value_to_type(42, "INT") == 42

        with pytest.raises(TypeValidationError):
            coerce_value_to_type("not a number", "INTEGER")

    def test_coerce_value_float(self):
        """Test coercing values to FLOAT."""
        assert coerce_value_to_type("3.14", "FLOAT") == 3.14
        assert coerce_value_to_type(3, "REAL") == 3.0

    def test_coerce_value_boolean(self):
        """Test coercing values to BOOLEAN."""
        assert coerce_value_to_type("true", "BOOLEAN") is True
        assert coerce_value_to_type("TRUE", "BOOL") is True
        assert coerce_value_to_type("false", "BOOLEAN") is False
        assert coerce_value_to_type("1", "BOOLEAN") is True
        assert coerce_value_to_type("0", "BOOLEAN") is False

        with pytest.raises(TypeValidationError):
            coerce_value_to_type("maybe", "BOOLEAN")

    def test_coerce_value_string(self):
        """Test coercing values to VARCHAR/TEXT."""
        assert coerce_value_to_type(123, "VARCHAR") == "123"
        assert coerce_value_to_type("hello", "TEXT") == "hello"


class TestRowUtils:
    """Test row utility functions."""

    def test_project_columns(self):
        """Test projecting specific columns from a row."""
        row = {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"}
        projected = project_columns(row, ["name", "age"])

        assert projected == {"name": "Alice", "age": 30}
        assert "id" not in projected
        assert "email" not in projected

    def test_project_columns_missing(self):
        """Test projecting columns that don't exist."""
        row = {"id": 1, "name": "Alice"}
        projected = project_columns(row, ["name", "nonexistent"])

        assert projected == {"name": "Alice"}
        assert "nonexistent" not in projected

    def test_combine_rows(self):
        """Test combining rows from JOIN."""
        left_row = {"id": 1, "name": "Alice"}
        right_row = {"id": 10, "title": "Post Title"}

        combined = combine_rows(left_row, right_row, "users", "posts")

        assert combined["users.id"] == 1
        assert combined["users.name"] == "Alice"
        assert combined["posts.id"] == 10
        assert combined["posts.title"] == "Post Title"

    def test_get_column_value_unqualified(self):
        """Test getting column value without table qualifier."""
        row = {"id": 1, "name": "Alice"}

        assert get_column_value(row, "name") == "Alice"
        assert get_column_value(row, "id") == 1

    def test_get_column_value_qualified(self):
        """Test getting column value with table qualifier."""
        row = {"users.id": 1, "users.name": "Alice"}

        assert get_column_value(row, "name", "users") == "Alice"
        assert get_column_value(row, "id", "users") == 1

    def test_get_column_value_missing(self):
        """Test getting non-existent column raises error."""
        row = {"id": 1}

        with pytest.raises(KeyError):
            get_column_value(row, "nonexistent")

    def test_has_column(self):
        """Test checking if row has column."""
        row = {"id": 1, "name": "Alice"}

        assert has_column(row, "name") is True
        assert has_column(row, "id") is True
        assert has_column(row, "nonexistent") is False

    def test_has_column_qualified(self):
        """Test checking qualified column names."""
        row = {"users.id": 1, "users.name": "Alice"}

        assert has_column(row, "name", "users") is True
        assert has_column(row, "id", "users") is True
        assert has_column(row, "email", "users") is False


class TestExceptions:
    """Test exception hierarchy."""

    def test_rdbms_error_base(self):
        """Test that all custom exceptions inherit from RDBMSError."""
        from rdbms.utils.exceptions import (
            TableNotFoundError,
            DuplicateKeyError,
            ConstraintViolationError,
            TypeValidationError,
            SQLSyntaxError,
            ColumnNotFoundError
        )

        assert issubclass(TableNotFoundError, RDBMSError)
        assert issubclass(DuplicateKeyError, RDBMSError)
        assert issubclass(ConstraintViolationError, RDBMSError)
        assert issubclass(TypeValidationError, RDBMSError)
        assert issubclass(SQLSyntaxError, RDBMSError)
        assert issubclass(ColumnNotFoundError, RDBMSError)

    def test_exception_messages(self):
        """Test exception messages are informative."""
        from rdbms.utils.exceptions import (
            TableNotFoundError,
            DuplicateKeyError,
            TypeValidationError
        )

        err = TableNotFoundError("users")
        assert "users" in str(err)

        err = DuplicateKeyError("email", "test@example.com")
        assert "email" in str(err)
        assert "test@example.com" in str(err)

        err = TypeValidationError("age", "INTEGER", "not a number")
        assert "age" in str(err)
        assert "INTEGER" in str(err)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
