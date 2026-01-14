"""
Reusable validation functions used across the RDBMS.

These validators provide single sources of truth for validation logic,
preventing duplication across storage, parser, and executor modules.
"""

import re
from typing import Any
from .exceptions import InvalidIdentifierError


def validate_identifier(name: str) -> bool:
    """
    Validates table/column/index names.

    Rules:
    - Must start with a letter or underscore
    - Can contain letters, numbers, and underscores
    - Must be between 1 and 64 characters
    - Cannot be a SQL reserved word

    Args:
        name: The identifier to validate

    Returns:
        True if valid

    Raises:
        InvalidIdentifierError: If the identifier is invalid
    """
    # Reserved SQL keywords (basic subset)
    RESERVED_WORDS = {
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE',
        'CREATE', 'DROP', 'TABLE', 'INDEX', 'INTO', 'VALUES',
        'SET', 'AND', 'OR', 'NOT', 'NULL', 'PRIMARY', 'KEY',
        'UNIQUE', 'INNER', 'JOIN', 'ON', 'AS', 'INTEGER', 'INT',
        'VARCHAR', 'TEXT', 'BOOLEAN', 'FLOAT', 'REAL', 'TRUE', 'FALSE'
    }

    if not name:
        raise InvalidIdentifierError(name, "Identifier cannot be empty")

    if len(name) > 64:
        raise InvalidIdentifierError(name, "Identifier too long (max 64 characters)")

    if name.upper() in RESERVED_WORDS:
        raise InvalidIdentifierError(name, "Cannot use SQL reserved word")

    # Must start with letter or underscore, then letters/numbers/underscores
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise InvalidIdentifierError(
            name,
            "Must start with letter or underscore, and contain only letters, numbers, and underscores"
        )

    return True


def validate_value_for_type(value: Any, data_type: str, max_length: int = None) -> bool:
    """
    Validates that a value matches the expected data type.

    This is the single source of truth for type checking, used by:
    - Column.validate() in storage layer
    - Type checking in executor
    - Constraint validation

    Args:
        value: The value to validate
        data_type: The expected type ('INTEGER', 'VARCHAR', 'TEXT', 'BOOLEAN', 'FLOAT')
        max_length: Maximum length for VARCHAR types

    Returns:
        True if valid

    Raises:
        TypeValidationError: If value doesn't match the expected type
    """
    from .exceptions import TypeValidationError

    if value is None:
        # NULL handling is done separately via NOT NULL constraint
        return True

    data_type = data_type.upper()

    if data_type in ('INTEGER', 'INT'):
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeValidationError('', data_type, value)
        return True

    elif data_type in ('FLOAT', 'REAL'):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise TypeValidationError('', data_type, value)
        return True

    elif data_type in ('BOOLEAN', 'BOOL'):
        if not isinstance(value, bool):
            raise TypeValidationError('', data_type, value)
        return True

    elif data_type == 'VARCHAR':
        if not isinstance(value, str):
            raise TypeValidationError('', data_type, value)
        if max_length and len(value) > max_length:
            raise TypeValidationError(
                '',
                f'VARCHAR({max_length})',
                f'string too long ({len(value)} > {max_length})'
            )
        return True

    elif data_type == 'TEXT':
        if not isinstance(value, str):
            raise TypeValidationError('', data_type, value)
        return True

    else:
        raise ValueError(f"Unknown data type: {data_type}")


def coerce_value_to_type(value: Any, data_type: str) -> Any:
    """
    Attempts to coerce a value to the specified type.

    Used by the parser to convert string literals from SQL
    to appropriate Python types.

    Args:
        value: The value to coerce (typically a string from SQL parsing)
        data_type: Target type

    Returns:
        The coerced value

    Raises:
        TypeValidationError: If coercion fails
    """
    from .exceptions import TypeValidationError

    data_type = data_type.upper()

    try:
        if data_type in ('INTEGER', 'INT'):
            return int(value)
        elif data_type in ('FLOAT', 'REAL'):
            return float(value)
        elif data_type in ('BOOLEAN', 'BOOL'):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                value_lower = value.lower()
                if value_lower in ('true', '1', 'yes'):
                    return True
                elif value_lower in ('false', '0', 'no'):
                    return False
            raise ValueError()
        elif data_type in ('VARCHAR', 'TEXT'):
            return str(value)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    except (ValueError, TypeError) as e:
        raise TypeValidationError('', data_type, value)
