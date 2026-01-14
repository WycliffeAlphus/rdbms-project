"""
Unit tests for storage layer (Database, Table, Index).
"""

import pytest
from rdbms.storage.database import Database
from rdbms.storage.table import Table
from rdbms.storage.types import Column, DataType, ColumnConstraint
from rdbms.storage.index import HashIndex
from rdbms.utils.exceptions import (
    TableNotFoundError,
    TableAlreadyExistsError,
    DuplicateKeyError,
    ConstraintViolationError,
    ColumnNotFoundError,
    TypeValidationError
)


class TestDatabase:
    """Test Database class."""

    def test_create_database(self):
        """Test database creation."""
        db = Database("test_db")
        assert db.name == "test_db"
        assert db.table_count() == 0

    def test_create_table(self):
        """Test table creation."""
        db = Database()
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50)
        ]
        table = db.create_table("users", columns)

        assert db.has_table("users")
        assert db.table_count() == 1
        assert isinstance(table, Table)

    def test_create_duplicate_table(self):
        """Test creating table with duplicate name raises error."""
        db = Database()
        columns = [Column("id", DataType.INTEGER)]
        db.create_table("users", columns)

        with pytest.raises(TableAlreadyExistsError):
            db.create_table("users", columns)

    def test_drop_table(self):
        """Test dropping a table."""
        db = Database()
        columns = [Column("id", DataType.INTEGER)]
        db.create_table("users", columns)

        db.drop_table("users")
        assert not db.has_table("users")
        assert db.table_count() == 0

    def test_drop_nonexistent_table(self):
        """Test dropping non-existent table raises error."""
        db = Database()
        with pytest.raises(TableNotFoundError):
            db.drop_table("nonexistent")

    def test_get_table(self):
        """Test retrieving a table."""
        db = Database()
        columns = [Column("id", DataType.INTEGER)]
        created_table = db.create_table("users", columns)

        retrieved_table = db.get_table("users")
        assert retrieved_table is created_table

    def test_list_tables(self):
        """Test listing all tables."""
        db = Database()
        columns = [Column("id", DataType.INTEGER)]

        db.create_table("users", columns)
        db.create_table("posts", columns)

        tables = db.list_tables()
        assert len(tables) == 2
        assert "users" in tables
        assert "posts" in tables


class TestTable:
    """Test Table class."""

    def test_create_table(self):
        """Test table creation."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50)
        ]
        table = Table("users", columns)

        assert table.name == "users"
        assert len(table.columns) == 2
        assert table.row_count() == 0
        assert table.primary_key == "id"

    def test_insert_row(self):
        """Test inserting a row."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50),
            Column("age", DataType.INTEGER)
        ]
        table = Table("users", columns)

        row_id = table.insert_row({"id": 1, "name": "Alice", "age": 30})
        assert row_id == 0  # First row gets ID 0
        assert table.row_count() == 1

    def test_insert_duplicate_primary_key(self):
        """Test inserting duplicate primary key raises error."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50)
        ]
        table = Table("users", columns)

        table.insert_row({"id": 1, "name": "Alice"})

        with pytest.raises(DuplicateKeyError):
            table.insert_row({"id": 1, "name": "Bob"})

    def test_insert_duplicate_unique(self):
        """Test inserting duplicate unique value raises error."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("email", DataType.VARCHAR, max_length=100, constraints={ColumnConstraint.UNIQUE})
        ]
        table = Table("users", columns)

        table.insert_row({"id": 1, "email": "alice@example.com"})

        with pytest.raises(DuplicateKeyError):
            table.insert_row({"id": 2, "email": "alice@example.com"})

    def test_insert_null_not_null_column(self):
        """Test inserting NULL into NOT NULL column raises error."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50, constraints={ColumnConstraint.NOT_NULL})
        ]
        table = Table("users", columns)

        with pytest.raises(ConstraintViolationError):
            table.insert_row({"id": 1, "name": None})

    def test_insert_wrong_type(self):
        """Test inserting wrong type raises error."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("age", DataType.INTEGER)
        ]
        table = Table("users", columns)

        with pytest.raises(TypeValidationError):
            table.insert_row({"id": 1, "age": "not a number"})

    def test_select_all_rows(self):
        """Test selecting all rows."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50)
        ]
        table = Table("users", columns)

        table.insert_row({"id": 1, "name": "Alice"})
        table.insert_row({"id": 2, "name": "Bob"})

        rows = list(table.scan())
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

    def test_update_row(self):
        """Test updating a row."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50),
            Column("age", DataType.INTEGER)
        ]
        table = Table("users", columns)

        row_id = table.insert_row({"id": 1, "name": "Alice", "age": 30})

        success = table.update_row(row_id, {"age": 31})
        assert success is True

        row = table.get_row(row_id)
        assert row["age"] == 31

    def test_update_primary_key_fails(self):
        """Test updating primary key raises error."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50)
        ]
        table = Table("users", columns)

        row_id = table.insert_row({"id": 1, "name": "Alice"})

        with pytest.raises(ConstraintViolationError):
            table.update_row(row_id, {"id": 2})

    def test_delete_row(self):
        """Test deleting a row."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50)
        ]
        table = Table("users", columns)

        row_id = table.insert_row({"id": 1, "name": "Alice"})
        assert table.row_count() == 1

        success = table.delete_row(row_id)
        assert success is True
        assert table.row_count() == 0

    def test_delete_nonexistent_row(self):
        """Test deleting non-existent row returns False."""
        columns = [Column("id", DataType.INTEGER)]
        table = Table("users", columns)

        success = table.delete_row(999)
        assert success is False


class TestIndex:
    """Test Index classes."""

    def test_hash_index_insert_and_search(self):
        """Test inserting and searching in hash index."""
        index = HashIndex("id")

        index.insert(1, 100)
        index.insert(2, 200)
        index.insert(1, 101)  # Same key, different row

        results = index.search(1)
        assert len(results) == 2
        assert 100 in results
        assert 101 in results

    def test_hash_index_search_nonexistent(self):
        """Test searching for non-existent key returns empty list."""
        index = HashIndex("id")

        results = index.search(999)
        assert results == []

    def test_hash_index_delete(self):
        """Test deleting from hash index."""
        index = HashIndex("id")

        index.insert(1, 100)
        index.insert(1, 101)

        index.delete(1, 100)

        results = index.search(1)
        assert len(results) == 1
        assert 101 in results

    def test_hash_index_get_all_keys(self):
        """Test getting all keys from index."""
        index = HashIndex("id")

        index.insert(1, 100)
        index.insert(2, 200)
        index.insert(3, 300)

        keys = index.get_all_keys()
        assert len(keys) == 3
        assert 1 in keys
        assert 2 in keys
        assert 3 in keys

    def test_hash_index_clear(self):
        """Test clearing index."""
        index = HashIndex("id")

        index.insert(1, 100)
        index.insert(2, 200)

        index.clear()

        assert len(index) == 0
        assert index.search(1) == []


class TestTableWithIndex:
    """Test Table with index operations."""

    def test_automatic_primary_key_index(self):
        """Test that primary key automatically gets an index."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("name", DataType.VARCHAR, max_length=50)
        ]
        table = Table("users", columns)

        assert table.has_index("id")

    def test_automatic_unique_index(self):
        """Test that unique columns automatically get indexes."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("email", DataType.VARCHAR, max_length=100, constraints={ColumnConstraint.UNIQUE})
        ]
        table = Table("users", columns)

        assert table.has_index("email")

    def test_manual_index_creation(self):
        """Test manually creating an index."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("age", DataType.INTEGER)
        ]
        table = Table("users", columns)

        table.create_index("age")
        assert table.has_index("age")

    def test_index_updated_on_insert(self):
        """Test that index is updated when row is inserted."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("age", DataType.INTEGER)
        ]
        table = Table("users", columns)
        table.create_index("age")

        table.insert_row({"id": 1, "age": 30})
        table.insert_row({"id": 2, "age": 25})
        table.insert_row({"id": 3, "age": 30})

        index = table.get_index("age")
        results = index.search(30)
        assert len(results) == 2

    def test_index_updated_on_delete(self):
        """Test that index is updated when row is deleted."""
        columns = [
            Column("id", DataType.INTEGER, constraints={ColumnConstraint.PRIMARY_KEY}),
            Column("age", DataType.INTEGER)
        ]
        table = Table("users", columns)
        table.create_index("age")

        row_id = table.insert_row({"id": 1, "age": 30})

        table.delete_row(row_id)

        index = table.get_index("age")
        results = index.search(30)
        assert len(results) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
