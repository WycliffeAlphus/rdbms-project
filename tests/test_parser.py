"""
Unit tests for SQL parser.
"""

import pytest
from rdbms.parser.parser import SQLParser
from rdbms.parser import ast
from rdbms.utils.exceptions import SQLSyntaxError


class TestParser:
    """Test SQL parser."""

    def setup_method(self):
        """Set up parser for each test."""
        self.parser = SQLParser()

    def test_parse_create_table_simple(self):
        """Test parsing simple CREATE TABLE."""
        sql = "CREATE TABLE users (id INTEGER, name VARCHAR(50))"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.CreateTableStmt)
        assert stmt.table_name == "users"
        assert len(stmt.columns) == 2
        assert stmt.columns[0].name == "id"
        assert stmt.columns[0].data_type == "INTEGER"
        assert stmt.columns[1].name == "name"
        assert stmt.columns[1].data_type == "VARCHAR"
        assert stmt.columns[1].max_length == 50

    def test_parse_create_table_with_constraints(self):
        """Test parsing CREATE TABLE with constraints."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(100) UNIQUE)"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.CreateTableStmt)
        assert "PRIMARY KEY" in stmt.columns[0].constraints
        assert "UNIQUE" in stmt.columns[1].constraints

    def test_parse_drop_table(self):
        """Test parsing DROP TABLE."""
        sql = "DROP TABLE users"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.DropTableStmt)
        assert stmt.table_name == "users"

    def test_parse_create_index(self):
        """Test parsing CREATE INDEX."""
        sql = "CREATE INDEX idx_email ON users(email)"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.CreateIndexStmt)
        assert stmt.index_name == "idx_email"
        assert stmt.table_name == "users"
        assert stmt.column_name == "email"

    def test_parse_insert(self):
        """Test parsing INSERT."""
        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.InsertStmt)
        assert stmt.table_name == "users"
        assert stmt.columns == ["id", "name", "age"]
        assert len(stmt.values) == 3
        assert stmt.values[0].value == 1
        assert stmt.values[1].value == "Alice"
        assert stmt.values[2].value == 30

    def test_parse_insert_boolean(self):
        """Test parsing INSERT with boolean."""
        sql = "INSERT INTO users (id, active) VALUES (1, TRUE)"
        stmt = self.parser.parse(sql)

        assert stmt.values[1].value is True

    def test_parse_insert_null(self):
        """Test parsing INSERT with NULL."""
        sql = "INSERT INTO users (id, name) VALUES (1, NULL)"
        stmt = self.parser.parse(sql)

        assert stmt.values[1].value is None

    def test_parse_select_all(self):
        """Test parsing SELECT *."""
        sql = "SELECT * FROM users"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.SelectStmt)
        assert stmt.columns == "*"
        assert stmt.table_name == "users"
        assert stmt.where is None

    def test_parse_select_columns(self):
        """Test parsing SELECT with specific columns."""
        sql = "SELECT id, name, age FROM users"
        stmt = self.parser.parse(sql)

        assert stmt.columns == ["id", "name", "age"]

    def test_parse_select_with_where(self):
        """Test parsing SELECT with WHERE clause."""
        sql = "SELECT * FROM users WHERE age > 25"
        stmt = self.parser.parse(sql)

        assert stmt.where is not None
        assert isinstance(stmt.where, ast.Comparison)
        assert stmt.where.op == ast.ComparisonOp.GT

    def test_parse_where_and(self):
        """Test parsing WHERE with AND."""
        sql = "SELECT * FROM users WHERE age > 25 AND active = TRUE"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt.where, ast.LogicalCondition)
        assert stmt.where.op == ast.LogicalOp.AND

    def test_parse_where_or(self):
        """Test parsing WHERE with OR."""
        sql = "SELECT * FROM users WHERE age < 20 OR age > 60"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt.where, ast.LogicalCondition)
        assert stmt.where.op == ast.LogicalOp.OR

    def test_parse_where_parentheses(self):
        """Test parsing WHERE with parentheses."""
        sql = "SELECT * FROM users WHERE (age > 20 AND age < 30) OR active = TRUE"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt.where, ast.LogicalCondition)
        assert stmt.where.op == ast.LogicalOp.OR

    def test_parse_select_with_join(self):
        """Test parsing SELECT with INNER JOIN."""
        sql = """
        SELECT users.name, posts.title
        FROM users
        INNER JOIN posts ON users.id = posts.user_id
        """
        stmt = self.parser.parse(sql)

        assert stmt.join is not None
        assert isinstance(stmt.join, ast.JoinClause)
        assert stmt.join.table_name == "posts"
        assert stmt.join.left_column.table_name == "users"
        assert stmt.join.left_column.column_name == "id"
        assert stmt.join.right_column.table_name == "posts"
        assert stmt.join.right_column.column_name == "user_id"

    def test_parse_update(self):
        """Test parsing UPDATE."""
        sql = "UPDATE users SET age = 31, active = FALSE WHERE id = 1"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.UpdateStmt)
        assert stmt.table_name == "users"
        assert stmt.assignments == {"age": 31, "active": False}
        assert stmt.where is not None

    def test_parse_update_no_where(self):
        """Test parsing UPDATE without WHERE."""
        sql = "UPDATE users SET age = 30"
        stmt = self.parser.parse(sql)

        assert stmt.where is None

    def test_parse_delete(self):
        """Test parsing DELETE."""
        sql = "DELETE FROM users WHERE id = 1"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.DeleteStmt)
        assert stmt.table_name == "users"
        assert stmt.where is not None

    def test_parse_delete_no_where(self):
        """Test parsing DELETE without WHERE."""
        sql = "DELETE FROM users"
        stmt = self.parser.parse(sql)

        assert stmt.where is None

    def test_parse_comparison_operators(self):
        """Test parsing all comparison operators."""
        operators = [
            ("=", ast.ComparisonOp.EQ),
            ("!=", ast.ComparisonOp.NE),
            ("<>", ast.ComparisonOp.NE),
            ("<", ast.ComparisonOp.LT),
            (">", ast.ComparisonOp.GT),
            ("<=", ast.ComparisonOp.LTE),
            (">=", ast.ComparisonOp.GTE)
        ]

        for op_str, op_enum in operators:
            sql = f"SELECT * FROM users WHERE age {op_str} 25"
            stmt = self.parser.parse(sql)
            assert stmt.where.op == op_enum

    def test_parse_data_types(self):
        """Test parsing all data types."""
        sql = """
        CREATE TABLE test (
            col1 INTEGER,
            col2 INT,
            col3 FLOAT,
            col4 REAL,
            col5 VARCHAR(100),
            col6 TEXT,
            col7 BOOLEAN,
            col8 BOOL
        )
        """
        stmt = self.parser.parse(sql)

        assert stmt.columns[0].data_type == "INTEGER"
        assert stmt.columns[1].data_type == "INTEGER"
        assert stmt.columns[2].data_type == "FLOAT"
        assert stmt.columns[3].data_type == "FLOAT"
        assert stmt.columns[4].data_type == "VARCHAR"
        assert stmt.columns[5].data_type == "TEXT"
        assert stmt.columns[6].data_type == "BOOLEAN"
        assert stmt.columns[7].data_type == "BOOLEAN"

    def test_parse_case_insensitive(self):
        """Test that SQL keywords are case-insensitive."""
        sql = "select * from users where age > 25"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.SelectStmt)

    def test_parse_with_semicolon(self):
        """Test parsing with trailing semicolon."""
        sql = "SELECT * FROM users;"
        stmt = self.parser.parse(sql)

        assert isinstance(stmt, ast.SelectStmt)

    def test_parse_invalid_syntax(self):
        """Test that invalid syntax raises error."""
        sql = "SELECT FROM WHERE"

        with pytest.raises(SQLSyntaxError):
            self.parser.parse(sql)

    def test_parse_number_types(self):
        """Test parsing different number formats."""
        sql = "INSERT INTO test (a, b, c) VALUES (42, 3.14, 1.5e10)"
        stmt = self.parser.parse(sql)

        assert stmt.values[0].value == 42
        assert stmt.values[1].value == 3.14
        assert stmt.values[2].value == 1.5e10

    def test_parse_string_escaping(self):
        """Test parsing strings with quotes."""
        sql = "INSERT INTO test (name) VALUES ('O\\'Brien')"
        stmt = self.parser.parse(sql)

        assert "'" in stmt.values[0].value


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
