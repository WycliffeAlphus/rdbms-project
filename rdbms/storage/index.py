"""
Index implementations for fast row lookups.

Uses abstract base class pattern for extensibility:
- Easy to add new index types (BTreeIndex, etc.)
- Table class doesn't depend on specific index implementation
- Clear contract for what an index must provide
"""

from abc import ABC, abstractmethod
from typing import List, Any, Optional
from collections import defaultdict


class Index(ABC):
    """
    Abstract base class for all index implementations.

    This defines the interface that all indexes must implement,
    allowing the Table class to work with any index type without
    knowing the specific implementation.
    """

    def __init__(self, column_name: str):
        """
        Initialize index for a specific column.

        Args:
            column_name: Name of the indexed column
        """
        self.column_name = column_name

    @abstractmethod
    def insert(self, key: Any, row_id: int) -> None:
        """
        Add a key-to-row mapping to the index.

        Args:
            key: The column value to index
            row_id: Internal row ID in the table
        """
        pass

    @abstractmethod
    def search(self, key: Any) -> List[int]:
        """
        Find all row IDs with the given key value.

        Args:
            key: The value to search for

        Returns:
            List of row IDs (may be empty)
        """
        pass

    @abstractmethod
    def delete(self, key: Any, row_id: int) -> None:
        """
        Remove a key-to-row mapping from the index.

        Args:
            key: The column value
            row_id: The row ID to remove
        """
        pass

    @abstractmethod
    def get_all_keys(self) -> List[Any]:
        """
        Get all unique keys in the index.

        Used for unique constraint checking.

        Returns:
            List of all indexed keys
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """Remove all entries from the index."""
        pass


class HashIndex(Index):
    """
    Hash-based index implementation using Python dict.

    Provides O(1) average-case lookups for equality comparisons.
    Suitable for PRIMARY KEY, UNIQUE, and general indexes.

    Trade-offs:
    - Very fast equality lookups
    - No support for range queries
    - Simple to implement and understand
    """

    def __init__(self, column_name: str):
        """Initialize an empty hash index."""
        super().__init__(column_name)
        # Maps key -> list of row_ids
        # Using defaultdict avoids duplicate key-checking logic
        self._index: defaultdict[Any, List[int]] = defaultdict(list)

    def insert(self, key: Any, row_id: int) -> None:
        """
        Add key-to-row mapping.

        Multiple rows can have the same key (unless enforced by Table).
        """
        # Handle None keys (for optional indexed columns)
        if key is not None:
            self._index[key].append(row_id)

    def search(self, key: Any) -> List[int]:
        """
        O(1) lookup for rows with given key.

        Returns empty list if key not found.
        """
        if key is None:
            return []
        return self._index.get(key, []).copy()  # Return copy to prevent external modification

    def delete(self, key: Any, row_id: int) -> None:
        """
        Remove specific row_id from key's list.

        If no more rows have this key, remove the key entirely.
        """
        if key is not None and key in self._index:
            try:
                self._index[key].remove(row_id)
                # Clean up empty entries
                if not self._index[key]:
                    del self._index[key]
            except ValueError:
                # row_id not in list - this is OK, might have been deleted already
                pass

    def get_all_keys(self) -> List[Any]:
        """Return all indexed keys."""
        return list(self._index.keys())

    def clear(self) -> None:
        """Remove all index entries."""
        self._index.clear()

    def __len__(self) -> int:
        """Return number of unique keys in index."""
        return len(self._index)

    def __repr__(self) -> str:
        return f"HashIndex({self.column_name}, {len(self)} keys)"


# Future: BTreeIndex for range queries (stretch goal)
# class BTreeIndex(Index):
#     """
#     B-tree based index for range queries.
#
#     Would support:
#     - Range queries (BETWEEN, <, >, etc.)
#     - Ordered traversal
#     - Still good point lookups
#
#     Implementation: Could use sortedcontainers.SortedDict
#     """
#     pass
