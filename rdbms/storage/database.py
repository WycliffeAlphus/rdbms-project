"""
Database class that manages multiple tables.

The Database is the top-level container for tables, providing:
- Table creation and deletion
- Table lookup
- Database-wide operations
"""

from typing import Dict, List, Any
from .table import Table
from .types import Column
from ..utils.exceptions import TableNotFoundError, TableAlreadyExistsError
from ..utils.validators import validate_identifier


class Database:
    """
    Represents a database containing multiple tables.

    Responsibilities:
    - Manage table lifecycle (create, drop, lookup)
    - Provide database-wide introspection

    Does NOT:
    - Parse SQL
    - Execute queries
    - Format results
    """

    def __init__(self, name: str = "default"):
        """
        Initialize an empty database.

        Args:
            name: Database name
        """
        self.name = name
        self._tables: Dict[str, Table] = {}

    def create_table(self, table_name: str, columns: List[Column]) -> Table:
        """
        Create a new table in the database.

        Args:
            table_name: Name for the new table
            columns: List of Column objects defining the schema

        Returns:
            The created Table object

        Raises:
            TableAlreadyExistsError: If table already exists
            InvalidIdentifierError: If table name is invalid
        """
        validate_identifier(table_name)

        if table_name in self._tables:
            raise TableAlreadyExistsError(table_name)

        table = Table(table_name, columns)
        self._tables[table_name] = table
        return table

    def drop_table(self, table_name: str) -> None:
        """
        Remove a table from the database.

        Args:
            table_name: Name of table to drop

        Raises:
            TableNotFoundError: If table doesn't exist
        """
        if table_name not in self._tables:
            raise TableNotFoundError(table_name)

        del self._tables[table_name]

    def get_table(self, table_name: str) -> Table:
        """
        Get a table by name.

        Args:
            table_name: Name of table

        Returns:
            Table object

        Raises:
            TableNotFoundError: If table doesn't exist
        """
        if table_name not in self._tables:
            raise TableNotFoundError(table_name)

        return self._tables[table_name]

    def has_table(self, table_name: str) -> bool:
        """Check if a table exists."""
        return table_name in self._tables

    def list_tables(self) -> List[str]:
        """Get list of all table names."""
        return list(self._tables.keys())

    def table_count(self) -> int:
        """Return number of tables in database."""
        return len(self._tables)

    def clear(self) -> None:
        """Remove all tables from database."""
        self._tables.clear()

    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dict with database statistics
        """
        return {
            'name': self.name,
            'table_count': len(self._tables),
            'tables': {
                name: {
                    'row_count': table.row_count(),
                    'column_count': len(table.columns),
                    'index_count': len(table._indexes)
                }
                for name, table in self._tables.items()
            }
        }

    def __repr__(self) -> str:
        return f"Database({self.name}, {len(self._tables)} tables)"
