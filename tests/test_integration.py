"""
Integration tests for the complete RDBMS system.

Tests end-to-end functionality from SQL parsing through execution.
"""

import pytest
from rdbms.storage.database import Database
from rdbms.parser.parser import SQLParser
from rdbms.executor.executor import QueryExecutor


class TestIntegration:
    """Integration tests for complete RDBMS functionality."""

    def setup_method(self):
        """Set up fresh database for each test."""
        self.db = Database("integration_test")
        self.parser = SQLParser()
        self.executor = QueryExecutor(self.db)

    def execute_sql(self, sql):
        """Helper to parse and execute SQL."""
        stmt = self.parser.parse(sql)
        return self.executor.execute(stmt)

    def test_complete_user_workflow(self):
        """Test complete workflow: create, insert, select, update, delete."""
        # Create table
        self.execute_sql("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(50) UNIQUE,
                email VARCHAR(100),
                age INTEGER,
                active BOOLEAN
            )
        """)

        # Insert users
        self.execute_sql("INSERT INTO users (id, username, email, age, active) VALUES (1, 'alice', 'alice@example.com', 30, TRUE)")
        self.execute_sql("INSERT INTO users (id, username, email, age, active) VALUES (2, 'bob', 'bob@example.com', 25, TRUE)")
        self.execute_sql("INSERT INTO users (id, username, email, age, active) VALUES (3, 'charlie', 'charlie@example.com', 35, FALSE)")

        # Select all
        result = self.execute_sql("SELECT * FROM users")
        assert len(result) == 3

        # Select with WHERE
        result = self.execute_sql("SELECT username FROM users WHERE age > 28")
        assert len(result) == 2
        usernames = [r["username"] for r in result]
        assert "alice" in usernames
        assert "charlie" in usernames

        # Update
        count = self.execute_sql("UPDATE users SET active = FALSE WHERE username = 'bob'")
        assert count == 1

        # Verify update
        result = self.execute_sql("SELECT active FROM users WHERE username = 'bob'")
        assert result[0]["active"] is False

        # Delete
        count = self.execute_sql("DELETE FROM users WHERE active = FALSE")
        assert count == 2  # bob and charlie

        # Verify delete
        result = self.execute_sql("SELECT * FROM users")
        assert len(result) == 1
        assert result[0]["username"] == "alice"

    def test_blog_system_with_join(self):
        """Test a blog system with users and posts using JOIN."""
        # Create tables
        self.execute_sql("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(50) UNIQUE
            )
        """)

        self.execute_sql("""
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                title VARCHAR(200),
                content TEXT,
                published BOOLEAN
            )
        """)

        # Create index on foreign key
        self.execute_sql("CREATE INDEX idx_user_id ON posts(user_id)")

        # Insert users
        self.execute_sql("INSERT INTO users (id, username) VALUES (1, 'alice')")
        self.execute_sql("INSERT INTO users (id, username) VALUES (2, 'bob')")

        # Insert posts
        self.execute_sql("INSERT INTO posts (id, user_id, title, content, published) VALUES (1, 1, 'First Post', 'Content 1', TRUE)")
        self.execute_sql("INSERT INTO posts (id, user_id, title, content, published) VALUES (2, 1, 'Second Post', 'Content 2', TRUE)")
        self.execute_sql("INSERT INTO posts (id, user_id, title, content, published) VALUES (3, 2, 'Bob Post', 'Content 3', TRUE)")
        self.execute_sql("INSERT INTO posts (id, user_id, title, content, published) VALUES (4, 1, 'Draft', 'Content 4', FALSE)")

        # JOIN to get all posts with usernames
        result = self.execute_sql("""
            SELECT users.username, posts.title, posts.published
            FROM users
            INNER JOIN posts ON users.id = posts.user_id
        """)

        assert len(result) == 4

        # JOIN with WHERE to get only published posts
        result = self.execute_sql("""
            SELECT users.username, posts.title
            FROM users
            INNER JOIN posts ON users.id = posts.user_id
            WHERE posts.published = TRUE
        """)

        assert len(result) == 3

        # Count Alice's published posts
        result = self.execute_sql("""
            SELECT posts.title
            FROM users
            INNER JOIN posts ON users.id = posts.user_id
            WHERE users.username = 'alice' AND posts.published = TRUE
        """)

        assert len(result) == 2

    def test_ecommerce_system(self):
        """Test e-commerce system with products, categories, and orders."""
        # Create tables
        self.execute_sql("""
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) UNIQUE
            )
        """)

        self.execute_sql("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                category_id INTEGER,
                name VARCHAR(100) UNIQUE,
                price FLOAT,
                in_stock BOOLEAN
            )
        """)

        self.execute_sql("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                quantity INTEGER,
                total FLOAT
            )
        """)

        # Insert categories
        self.execute_sql("INSERT INTO categories (id, name) VALUES (1, 'Electronics')")
        self.execute_sql("INSERT INTO categories (id, name) VALUES (2, 'Books')")

        # Insert products
        self.execute_sql("INSERT INTO products (id, category_id, name, price, in_stock) VALUES (1, 1, 'Laptop', 999.99, TRUE)")
        self.execute_sql("INSERT INTO products (id, category_id, name, price, in_stock) VALUES (2, 1, 'Mouse', 29.99, TRUE)")
        self.execute_sql("INSERT INTO products (id, category_id, name, price, in_stock) VALUES (3, 2, 'Python Book', 49.99, TRUE)")
        self.execute_sql("INSERT INTO products (id, category_id, name, price, in_stock) VALUES (4, 1, 'Keyboard', 79.99, FALSE)")

        # Insert orders
        self.execute_sql("INSERT INTO orders (id, product_id, quantity, total) VALUES (1, 1, 2, 1999.98)")
        self.execute_sql("INSERT INTO orders (id, product_id, quantity, total) VALUES (2, 2, 5, 149.95)")

        # Get in-stock electronics
        result = self.execute_sql("""
            SELECT products.name, products.price
            FROM products
            INNER JOIN categories ON products.category_id = categories.id
            WHERE categories.name = 'Electronics' AND products.in_stock = TRUE
        """)

        assert len(result) == 2
        names = [r["products.name"] for r in result]
        assert "Laptop" in names
        assert "Mouse" in names

        # Get order details
        result = self.execute_sql("""
            SELECT products.name, orders.quantity, orders.total
            FROM orders
            INNER JOIN products ON orders.product_id = products.id
        """)

        assert len(result) == 2

        # Update prices
        count = self.execute_sql("UPDATE products SET price = 899.99 WHERE name = 'Laptop'")
        assert count == 1

        # Mark out-of-stock items
        count = self.execute_sql("UPDATE products SET in_stock = FALSE WHERE price < 30")
        assert count == 1  # Mouse

    def test_complex_where_clauses(self):
        """Test complex WHERE conditions with AND, OR, parentheses."""
        self.execute_sql("""
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                age INTEGER,
                salary FLOAT,
                department VARCHAR(50)
            )
        """)

        # Insert test data
        employees = [
            (1, 'Alice', 30, 75000.0, 'Engineering'),
            (2, 'Bob', 25, 60000.0, 'Marketing'),
            (3, 'Charlie', 36, 80000.0, 'Engineering'),
            (4, 'Diana', 28, 70000.0, 'Marketing'),
            (5, 'Eve', 40, 90000.0, 'Management')
        ]

        for emp in employees:
            self.execute_sql(f"INSERT INTO employees (id, name, age, salary, department) VALUES ({emp[0]}, '{emp[1]}', {emp[2]}, {emp[3]}, '{emp[4]}')")

        # Test AND
        result = self.execute_sql("SELECT name FROM employees WHERE age > 30 AND salary > 75000")
        assert len(result) == 2  # Charlie and Eve

        # Test OR
        result = self.execute_sql("SELECT name FROM employees WHERE age < 27 OR salary > 85000")
        assert len(result) == 2  # Bob and Eve

        # Test combined AND/OR
        result = self.execute_sql("""
            SELECT name FROM employees
            WHERE (age < 30 AND department = 'Marketing')
            OR (age > 35 AND department = 'Engineering')
        """)
        assert len(result) == 3  # Bob, Diana, and Charlie

    def test_constraint_enforcement(self):
        """Test that constraints are properly enforced."""
        self.execute_sql("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                unique_col VARCHAR(50) UNIQUE,
                not_null_col VARCHAR(50)
            )
        """)

        # Test PRIMARY KEY uniqueness
        self.execute_sql("INSERT INTO test (id, unique_col, not_null_col) VALUES (1, 'A', 'X')")

        with pytest.raises(Exception):  # DuplicateKeyError
            self.execute_sql("INSERT INTO test (id, unique_col, not_null_col) VALUES (1, 'B', 'Y')")

        # Test UNIQUE constraint
        with pytest.raises(Exception):  # DuplicateKeyError
            self.execute_sql("INSERT INTO test (id, unique_col, not_null_col) VALUES (2, 'A', 'Z')")

        # Test NOT NULL constraint (implicit from PRIMARY KEY)
        with pytest.raises(Exception):  # ConstraintViolationError
            self.execute_sql("INSERT INTO test (id, unique_col, not_null_col) VALUES (NULL, 'C', 'W')")

    def test_index_performance(self):
        """Test that indexes improve query performance."""
        # Create table
        self.execute_sql("""
            CREATE TABLE large_table (
                id INTEGER PRIMARY KEY,
                indexed_col INTEGER,
                data VARCHAR(100)
            )
        """)

        # Insert many rows
        for i in range(100):
            self.execute_sql(f"INSERT INTO large_table (id, indexed_col, data) VALUES ({i}, {i % 10}, 'data{i}')")

        # Create index
        self.execute_sql("CREATE INDEX idx_indexed_col ON large_table(indexed_col)")

        # Query should use index
        table = self.db.get_table("large_table")
        assert table.has_index("indexed_col")

        # Verify results
        result = self.execute_sql("SELECT data FROM large_table WHERE indexed_col = 5")
        assert len(result) == 10  # 10 rows with indexed_col = 5

    def test_null_handling(self):
        """Test NULL value handling in queries."""
        self.execute_sql("""
            CREATE TABLE nullable (
                id INTEGER PRIMARY KEY,
                optional_field VARCHAR(50)
            )
        """)

        self.execute_sql("INSERT INTO nullable (id, optional_field) VALUES (1, 'value')")
        self.execute_sql("INSERT INTO nullable (id, optional_field) VALUES (2, NULL)")

        # Select all
        result = self.execute_sql("SELECT * FROM nullable")
        assert len(result) == 2
        assert result[1]["optional_field"] is None

        # NULL comparison behavior
        result = self.execute_sql("SELECT id FROM nullable WHERE optional_field = 'value'")
        assert len(result) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
