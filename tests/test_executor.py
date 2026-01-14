"""
Unit tests for query executor.
"""

import pytest
from rdbms.storage.database import Database
from rdbms.parser.parser import SQLParser
from rdbms.executor.executor import QueryExecutor
from rdbms.utils.exceptions import (
    TableNotFoundError,
    DuplicateKeyError,
    ColumnNotFoundError,
    ConstraintViolationError
)


class TestExecutor:
    """Test query executor."""

    def setup_method(self):
        """Set up database, parser, and executor for each test."""
        self.db = Database("test")
        self.parser = SQLParser()
        self.executor = QueryExecutor(self.db)

    def test_execute_create_table(self):
        """Test executing CREATE TABLE."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
        stmt = self.parser.parse(sql)
        result = self.executor.execute(stmt)

        assert result is None
        assert self.db.has_table("users")

    def test_execute_drop_table(self):
        """Test executing DROP TABLE."""
        sql = "CREATE TABLE users (id INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        sql = "DROP TABLE users"
        result = self.executor.execute(self.parser.parse(sql))

        assert result is None
        assert not self.db.has_table("users")

    def test_execute_create_index(self):
        """Test executing CREATE INDEX."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(100))"
        self.executor.execute(self.parser.parse(sql))

        sql = "CREATE INDEX idx_email ON users(email)"
        result = self.executor.execute(self.parser.parse(sql))

        assert result is None
        table = self.db.get_table("users")
        assert table.has_index("email")

    def test_execute_insert(self):
        """Test executing INSERT."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        result = self.executor.execute(self.parser.parse(sql))

        assert result == 1  # 1 row affected
        table = self.db.get_table("users")
        assert table.row_count() == 1

    def test_execute_insert_duplicate_key(self):
        """Test inserting duplicate primary key raises error."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name) VALUES (1, 'Bob')"
        with pytest.raises(DuplicateKeyError):
            self.executor.execute(self.parser.parse(sql))

    def test_execute_select_all(self):
        """Test executing SELECT *."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, name) VALUES (2, 'Bob')"
        self.executor.execute(self.parser.parse(sql))

        sql = "SELECT * FROM users"
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Bob"

    def test_execute_select_columns(self):
        """Test executing SELECT with specific columns."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        self.executor.execute(self.parser.parse(sql))

        sql = "SELECT name, age FROM users"
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 1
        assert "name" in result[0]
        assert "age" in result[0]
        assert "id" not in result[0]

    def test_execute_select_with_where(self):
        """Test executing SELECT with WHERE clause."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"
        self.executor.execute(self.parser.parse(sql))

        sql = "SELECT name FROM users WHERE age > 25"
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 2
        names = [r["name"] for r in result]
        assert "Alice" in names
        assert "Charlie" in names
        assert "Bob" not in names

    def test_execute_select_with_and(self):
        """Test executing SELECT with AND in WHERE."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER, active BOOLEAN)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, age, active) VALUES (1, 30, TRUE)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, age, active) VALUES (2, 25, TRUE)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, age, active) VALUES (3, 35, FALSE)"
        self.executor.execute(self.parser.parse(sql))

        sql = "SELECT id FROM users WHERE age > 25 AND active = TRUE"
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 1
        assert result[0]["id"] == 1

    def test_execute_select_with_or(self):
        """Test executing SELECT with OR in WHERE."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, age) VALUES (1, 20)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, age) VALUES (2, 30)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, age) VALUES (3, 65)"
        self.executor.execute(self.parser.parse(sql))

        sql = "SELECT id FROM users WHERE age < 25 OR age > 60"
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 2
        ids = [r["id"] for r in result]
        assert 1 in ids
        assert 3 in ids

    def test_execute_update(self):
        """Test executing UPDATE."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        self.executor.execute(self.parser.parse(sql))

        sql = "UPDATE users SET age = 31 WHERE id = 1"
        result = self.executor.execute(self.parser.parse(sql))

        assert result == 1  # 1 row updated

        sql = "SELECT age FROM users WHERE id = 1"
        result = self.executor.execute(self.parser.parse(sql))
        assert result[0]["age"] == 31

    def test_execute_update_multiple_rows(self):
        """Test updating multiple rows."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER, active BOOLEAN)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, age, active) VALUES (1, 30, TRUE)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, age, active) VALUES (2, 25, TRUE)"
        self.executor.execute(self.parser.parse(sql))

        sql = "UPDATE users SET active = FALSE WHERE age < 28"
        result = self.executor.execute(self.parser.parse(sql))

        assert result == 1  # Only Bob matches

    def test_execute_delete(self):
        """Test executing DELETE."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, name) VALUES (2, 'Bob')"
        self.executor.execute(self.parser.parse(sql))

        sql = "DELETE FROM users WHERE id = 1"
        result = self.executor.execute(self.parser.parse(sql))

        assert result == 1  # 1 row deleted

        sql = "SELECT * FROM users"
        result = self.executor.execute(self.parser.parse(sql))
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_execute_join(self):
        """Test executing INNER JOIN."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
        self.executor.execute(self.parser.parse(sql))

        sql = "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title VARCHAR(100))"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO users (id, name) VALUES (2, 'Bob')"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'First Post')"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO posts (id, user_id, title) VALUES (2, 1, 'Second Post')"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO posts (id, user_id, title) VALUES (3, 2, 'Bob Post')"
        self.executor.execute(self.parser.parse(sql))

        sql = """
        SELECT users.name, posts.title
        FROM users
        INNER JOIN posts ON users.id = posts.user_id
        """
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 3
        # Check qualified column names
        assert "users.name" in result[0]
        assert "posts.title" in result[0]

    def test_execute_join_with_where(self):
        """Test executing JOIN with WHERE clause."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
        self.executor.execute(self.parser.parse(sql))

        sql = "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, views INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
        self.executor.execute(self.parser.parse(sql))

        sql = "INSERT INTO posts (id, user_id, views) VALUES (1, 1, 100)"
        self.executor.execute(self.parser.parse(sql))
        sql = "INSERT INTO posts (id, user_id, views) VALUES (2, 1, 50)"
        self.executor.execute(self.parser.parse(sql))

        sql = """
        SELECT posts.views
        FROM users
        INNER JOIN posts ON users.id = posts.user_id
        WHERE posts.views > 75
        """
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 1
        assert result[0]["posts.views"] == 100


class TestExecutorWithIndex:
    """Test executor utilizing indexes."""

    def setup_method(self):
        """Set up database with indexed table."""
        self.db = Database("test")
        self.parser = SQLParser()
        self.executor = QueryExecutor(self.db)

        # Create table with index
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(100) UNIQUE, age INTEGER)"
        self.executor.execute(self.parser.parse(sql))

        # Add test data
        for i in range(100):
            sql = f"INSERT INTO users (id, email, age) VALUES ({i}, 'user{i}@example.com', {20 + i % 50})"
            self.executor.execute(self.parser.parse(sql))

    def test_select_uses_primary_key_index(self):
        """Test that SELECT uses primary key index."""
        table = self.db.get_table("users")

        # Verify index exists
        assert table.has_index("id")

        # Query using indexed column
        sql = "SELECT email FROM users WHERE id = 50"
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 1
        assert result[0]["email"] == "user50@example.com"

    def test_select_uses_unique_index(self):
        """Test that SELECT uses unique index."""
        table = self.db.get_table("users")

        # Verify index exists
        assert table.has_index("email")

        # Query using indexed column
        sql = "SELECT id FROM users WHERE email = 'user25@example.com'"
        result = self.executor.execute(self.parser.parse(sql))

        assert len(result) == 1
        assert result[0]["id"] == 25


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
