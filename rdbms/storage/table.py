"""
Table implementation for storing rows with constraints and indexes.

The Table class is responsible for:
- Storing and managing rows
- Enforcing constraints (PRIMARY KEY, UNIQUE, NOT NULL)
- Maintaining indexes automatically
- Providing row access methods

It does NOT handle:
- SQL parsing
- Query execution logic
- Result formatting
"""

from typing import List, Dict, Any, Optional, Iterator
from .types import Column, ColumnConstraint
from .index import Index, HashIndex
from ..utils.exceptions import (
    ColumnNotFoundError,
    DuplicateKeyError,
    ConstraintViolationError
)


class Table:
    """
    Represents a database table with rows, columns, and indexes.

    Uses composition pattern:
    - Column objects for metadata and validation
    - Index objects for fast lookups
    - Simple list for row storage
    """

    def __init__(self, name: str, columns: List[Column]):
        """
        Create a new table.

        Args:
            name: Table name
            columns: List of Column objects defining the schema

        Raises:
            ValueError: If no columns or multiple primary keys
        """
        if not columns:
            raise ValueError("Table must have at least one column")

        # Validate single primary key
        pk_columns = [c for c in columns if c.is_primary_key]
        if len(pk_columns) > 1:
            raise ValueError("Table can have at most one PRIMARY KEY")

        self.name = name
        self.columns = {col.name: col for col in columns}
        self.column_order = [col.name for col in columns]  # Preserve insertion order

        # Primary key column (if any)
        self.primary_key = pk_columns[0].name if pk_columns else None

        # Row storage: list of dicts
        # Each row has an internal _row_id for index tracking
        self._rows: List[Dict[str, Any]] = []
        self._next_row_id = 0
        self._deleted_row_ids = set()  # Track deleted rows for ID reuse

        # Indexes: column_name -> Index object
        self._indexes: Dict[str, Index] = {}

        # Automatically create indexes for PRIMARY KEY and UNIQUE columns
        for col in columns:
            if col.is_primary_key or col.is_unique:
                self.create_index(col.name)

    def create_index(self, column_name: str) -> None:
        """
        Create an index on a column.

        Indexes are automatically created for PRIMARY KEY and UNIQUE columns.
        Can also be created manually via CREATE INDEX.

        Args:
            column_name: Name of column to index

        Raises:
            ColumnNotFoundError: If column doesn't exist
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(column_name, self.name)

        if column_name in self._indexes:
            return  # Index already exists

        # Create hash index and populate with existing rows
        index = HashIndex(column_name)
        for row in self._rows:
            if '_row_id' in row and row['_row_id'] not in self._deleted_row_ids:
                index.insert(row[column_name], row['_row_id'])

        self._indexes[column_name] = index

    def drop_index(self, column_name: str) -> None:
        """
        Remove an index from a column.

        Cannot drop indexes on PRIMARY KEY or UNIQUE columns.

        Args:
            column_name: Column to remove index from
        """
        col = self.columns.get(column_name)
        if col and (col.is_primary_key or col.is_unique):
            raise ValueError(f"Cannot drop index on PRIMARY KEY or UNIQUE column '{column_name}'")

        self._indexes.pop(column_name, None)

    def insert_row(self, row_data: Dict[str, Any]) -> int:
        """
        Insert a new row into the table.

        Validates all constraints and updates indexes.

        Args:
            row_data: Dictionary mapping column names to values

        Returns:
            Internal row ID of inserted row

        Raises:
            ColumnNotFoundError: If unknown column in row_data
            TypeValidationError: If value doesn't match column type
            DuplicateKeyError: If PRIMARY KEY or UNIQUE constraint violated
            ConstraintViolationError: If NOT NULL constraint violated
        """
        # Validate columns
        for col_name in row_data:
            if col_name not in self.columns:
                raise ColumnNotFoundError(col_name, self.name)

        # Build complete row with defaults for missing columns
        complete_row = {}
        for col_name, column in self.columns.items():
            value = row_data.get(col_name)

            # Validate type and constraints using Column (single source of truth)
            column.validate(value, col_name)

            complete_row[col_name] = value

        # Check PRIMARY KEY uniqueness
        if self.primary_key:
            pk_value = complete_row[self.primary_key]
            if pk_value is not None:
                existing = self._find_by_index(self.primary_key, pk_value)
                if existing:
                    raise DuplicateKeyError(self.primary_key, pk_value)

        # Check UNIQUE constraints
        for col_name, column in self.columns.items():
            if column.is_unique and not column.is_primary_key:
                value = complete_row[col_name]
                if value is not None:
                    existing = self._find_by_index(col_name, value)
                    if existing:
                        raise DuplicateKeyError(col_name, value)

        # Assign internal row ID
        row_id = self._next_row_id
        self._next_row_id += 1
        complete_row['_row_id'] = row_id

        # Insert row
        self._rows.append(complete_row)

        # Update all indexes
        for col_name, index in self._indexes.items():
            index.insert(complete_row[col_name], row_id)

        return row_id

    def get_row(self, row_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a row by its internal ID.

        Args:
            row_id: Internal row ID

        Returns:
            Row dict without _row_id, or None if not found
        """
        for row in self._rows:
            if row.get('_row_id') == row_id and row_id not in self._deleted_row_ids:
                return self._strip_internal_id(row)
        return None

    def update_row(self, row_id: int, updates: Dict[str, Any]) -> bool:
        """
        Update a row with new values.

        Validates constraints and updates indexes.

        Args:
            row_id: Internal row ID
            updates: Dict of column_name -> new_value

        Returns:
            True if row was updated, False if not found

        Raises:
            ColumnNotFoundError: If unknown column in updates
            TypeValidationError: If value doesn't match column type
            DuplicateKeyError: If UNIQUE constraint violated
            ConstraintViolationError: If constraint violated
        """
        # Find the row
        row_index = None
        for i, row in enumerate(self._rows):
            if row.get('_row_id') == row_id and row_id not in self._deleted_row_ids:
                row_index = i
                break

        if row_index is None:
            return False

        old_row = self._rows[row_index]

        # Validate updates
        for col_name, new_value in updates.items():
            if col_name not in self.columns:
                raise ColumnNotFoundError(col_name, self.name)

            column = self.columns[col_name]

            # Cannot update PRIMARY KEY
            if column.is_primary_key:
                raise ConstraintViolationError(f"Cannot update PRIMARY KEY column '{col_name}'")

            # Validate new value
            column.validate(new_value, col_name)

            # Check UNIQUE constraint
            if column.is_unique and new_value is not None:
                existing = self._find_by_index(col_name, new_value)
                # Allow update if the existing row is this row
                if existing and existing[0] != row_id:
                    raise DuplicateKeyError(col_name, new_value)

        # Apply updates
        new_row = old_row.copy()
        for col_name, new_value in updates.items():
            old_value = old_row[col_name]
            new_row[col_name] = new_value

            # Update indexes
            if col_name in self._indexes:
                self._indexes[col_name].delete(old_value, row_id)
                self._indexes[col_name].insert(new_value, row_id)

        self._rows[row_index] = new_row
        return True

    def delete_row(self, row_id: int) -> bool:
        """
        Delete a row by its internal ID.

        Updates all indexes.

        Args:
            row_id: Internal row ID

        Returns:
            True if row was deleted, False if not found
        """
        for i, row in enumerate(self._rows):
            if row.get('_row_id') == row_id and row_id not in self._deleted_row_ids:
                # Mark as deleted
                self._deleted_row_ids.add(row_id)

                # Update indexes
                for col_name, index in self._indexes.items():
                    index.delete(row[col_name], row_id)

                return True

        return False

    def scan(self) -> Iterator[Dict[str, Any]]:
        """
        Iterate over all rows in the table (full table scan).

        Yields:
            Row dicts without internal _row_id
        """
        for row in self._rows:
            if row.get('_row_id') not in self._deleted_row_ids:
                yield self._strip_internal_id(row)

    def get_column(self, column_name: str) -> Column:
        """
        Get Column object by name.

        Args:
            column_name: Name of column

        Returns:
            Column object

        Raises:
            ColumnNotFoundError: If column doesn't exist
        """
        if column_name not in self.columns:
            raise ColumnNotFoundError(column_name, self.name)
        return self.columns[column_name]

    def has_index(self, column_name: str) -> bool:
        """Check if column has an index."""
        return column_name in self._indexes

    def get_index(self, column_name: str) -> Optional[Index]:
        """Get index for column, or None if no index exists."""
        return self._indexes.get(column_name)

    def _find_by_index(self, column_name: str, value: Any) -> Optional[List[int]]:
        """
        Internal helper to find rows using index.

        Returns list of row_ids or None if no index.
        """
        if column_name not in self._indexes:
            return None
        return self._indexes[column_name].search(value)

    def _strip_internal_id(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Remove internal _row_id from row dict."""
        return {k: v for k, v in row.items() if k != '_row_id'}

    def row_count(self) -> int:
        """Return number of non-deleted rows."""
        return len(self._rows) - len(self._deleted_row_ids)

    def get_schema(self) -> List[Dict[str, Any]]:
        """
        Get table schema as list of column definitions.

        Used for introspection and persistence.
        """
        return [self.columns[name].to_dict() for name in self.column_order]

    def __repr__(self) -> str:
        return f"Table({self.name}, {self.row_count()} rows, {len(self.columns)} columns)"
