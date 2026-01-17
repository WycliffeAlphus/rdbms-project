# RDBMS (Relational Database Management System)

A relational database management system built from scratch in Python, featuring SQL parsing, query execution, indexing, and JOIN operations.

## Features

### Core SQL Support
- **DDL**: `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`
- **DML**: `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- **WHERE Clauses**: Comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`), logical operators (`AND`, `OR`)
- **INNER JOIN**: Two-table joins with qualified column names
- **Data Types**: `INTEGER`, `FLOAT`, `VARCHAR(n)`, `TEXT`, `BOOLEAN`
- **Constraints**: `PRIMARY KEY`, `UNIQUE`, `NOT NULL`
- **Indexing**: Automatic indexes on PRIMARY KEY and UNIQUE columns, manual index creation

## Project Structure

```
rdbms-project/
├── rdbms/
│   ├── storage/         # Data storage layer
│   │   ├── database.py      # Database container
│   │   ├── table.py         # Table with CRUD operations
│   │   ├── index.py         # Abstract Index + HashIndex
│   │   └── types.py         # Column types and validation
│   ├── parser/          # SQL parsing layer
│   │   ├── grammar.lark     # SQL grammar definition
│   │   ├── ast.py           # AST node definitions
│   │   └── parser.py        # Parser implementation
│   ├── executor/        # Query execution layer
│   │   ├── executor.py      # Main query executor
│   │   ├── evaluator.py     # WHERE clause evaluator
│   │   └── planner.py       # Query optimizer
│   ├── utils/           # Shared utilities (DRY)
│   │   ├── exceptions.py    # Centralized exceptions
│   │   ├── validators.py    # Reusable validators
│   │   └── row_utils.py     # Row manipulation helpers
│   ├── repl.py          # Interactive SQL shell
│   └── formatter.py     # Result formatting
├── webapp/              # Demo web application
│   ├── app.py           # Flask backend
│   ├── templates/       # HTML templates
│   └── static/          # CSS and JavaScript
└── tests/               # Unit and integration tests
```

## Setup Instructions

### Prerequisites

1. **Install `python3-distutils`** (Ubuntu/Debian):
   ```bash
   sudo apt update
   sudo apt install python3-distutils
   ```

2. **Install Poetry** (Official Method - Recommended):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

3. **Add Poetry to PATH**:
   ```bash
   export PATH="$HOME/.local/bin:$PATH"
   ```

   Add this line to your `~/.bashrc` or `~/.zshrc` to make it permanent.

4. **Verify Installation**:
   ```bash
   poetry --version
   ```

> **Why Poetry?** The official Poetry installer avoids dependency conflicts with system packages that can occur when using `pip install poetry`.

### Installation

```bash
# Clone or navigate to project directory
cd rdbms-project

# Install dependencies
poetry install

# Verify installation
poetry run python -c "from rdbms.storage.database import Database; print('✓ Installation successful!')"
```

## Usage

### Interactive REPL

```bash
poetry run python -m rdbms.repl
```

**Example Session:**
```sql
rdbms> CREATE TABLE users (
         id INTEGER PRIMARY KEY,
         name VARCHAR(50),
         age INTEGER
       );
CREATE TABLE OK: users

rdbms> INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
INSERT OK, 1 row affected

rdbms> INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
INSERT OK, 1 row affected

rdbms> SELECT * FROM users WHERE age > 25;
+------+-------+-------+
|   id | name  |   age |
+======+=======+=======+
|    1 | Alice |    30 |
+------+-------+-------+
(1 row)

rdbms> UPDATE users SET age = 31 WHERE id = 1;
UPDATE OK, 1 row affected

rdbms> .tables

Tables:
  - users (2 rows)

rdbms> .exit
Goodbye!
```

**Multiline Input:** The REPL supports multiline SQL statements. Continue typing until you end with a semicolon (`;`).

**Special REPL Commands:**
- `.help` - Show available commands
- `.tables` - List all tables
- `.schema TABLE` - Show table schema
- `.stats` - Database statistics
- `.exit` / `.quit` - Exit REPL

### Web Application

```bash
poetry run python webapp/app.py
```

Then open http://localhost:5000 in your browser.

**Features:**
- Task management with categories
- Demonstrates INNER JOIN (tasks + categories)
- Full CRUD operations
- Real-time updates
- Category filtering

## Architecture & Design

### Layered Architecture

```
┌─────────────────────────────────────────┐
│        REPL / Web Application           │  ← User Interface Layer
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│          SQL Parser (Lark)              │  ← Parsing Layer
│  SQL String → AST Nodes                 │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│        Query Executor                   │  ← Execution Layer
│  - Planner (optimization)               │
│  - Evaluator (WHERE clauses)            │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│        Storage Engine                   │  ← Storage Layer
│  - Database → Tables → Rows             │
│  - Indexes (HashIndex)                  │
│  - Constraint enforcement               │
└─────────────────────────────────────────┘
```

### Key Design Decisions

**1. In-Memory Storage**
- Focuses effort on core database logic
- Simplifies implementation
- Excellent performance for demonstration
- Persistence can be added as an extension (JSON serialization ready)

**2. Hash-Based Indexing**
- O(1) average-case lookups for equality comparisons
- Simpler than B-tree but still demonstrates indexing concepts
- Automatically created for PRIMARY KEY and UNIQUE columns
- Extensible via abstract Index base class

**3. Nested Loop Join**
- Simple to implement and understand
- Works correctly for demonstration purposes
- Could be upgraded to hash join or sort-merge join

**4. Lark Parser**
- Declarative grammar definition (easier to maintain)
- LALR parser for performance
- Clean separation of parsing and execution

**5. Modular, DRY Architecture**
- **Utils Module**: Centralized exceptions, validators, row utilities
- **Single Responsibility**: Each class has one clear purpose
- **No Duplication**: WHERE evaluation, type validation, row filtering all reused
- **Abstract Interfaces**: Index ABC allows new index types without modifying Table

### Code Quality Principles

**DRY (Don't Repeat Yourself):**
- `ConditionEvaluator` - used by SELECT, UPDATE, DELETE
- `QueryPlanner.get_matching_rows()` - shared row filtering logic
- `Column.validate()` - single source of truth for type checking
- `row_utils.py` - reusable row operations for projections and joins

**Single Responsibility:**
- `Table` - manages rows, constraints, indexes (NOT parsing or formatting)
- `Parser` - converts SQL to AST (NOT execution)
- `Executor` - executes AST (NOT storage details)
- `REPL` - user interaction (NOT core logic)

## Supported SQL Examples

### Table Creation
```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) UNIQUE,
    price FLOAT,
    in_stock BOOLEAN
);
```

### Data Manipulation
```sql
-- Insert
INSERT INTO products (id, name, price, in_stock)
VALUES (1, 'Laptop', 999.99, TRUE);

-- Select with WHERE
SELECT name, price FROM products
WHERE in_stock = TRUE AND price < 1000;

-- Update
UPDATE products SET price = 899.99 WHERE id = 1;

-- Delete
DELETE FROM products WHERE in_stock = FALSE;
```

### Joins
```sql
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    product_id INTEGER,
    quantity INTEGER
);

-- Inner join
SELECT products.name, orders.quantity
FROM products
INNER JOIN orders ON products.id = orders.product_id
WHERE orders.quantity > 1;
```

### Indexes
```sql
-- Create manual index
CREATE INDEX idx_price ON products(price);

-- Indexes automatically created for PRIMARY KEY and UNIQUE
```

## Performance Characteristics

| Operation | Without Index | With Index |
|-----------|--------------|------------|
| `SELECT WHERE col = val` | O(n) full scan | O(1) hash lookup |
| `INSERT` | O(1) | O(1) + index updates |
| `UPDATE` | O(n) to find rows | O(1) with indexed WHERE |
| `DELETE` | O(n) to find rows | O(1) with indexed WHERE |
| `JOIN` | O(n × m) nested loop | O(n × m) (could optimize) |

## Testing

```bash
# Run all tests
poetry run pytest

# Run with verbose output
poetry run pytest -v

# Run specific test file
poetry run pytest tests/test_executor.py
```


## Credits & References

- **[Lark Parser](https://github.com/lark-parser/lark)** - Python parsing library for SQL grammar
- **[Flask](https://flask.palletsprojects.com/)** - Web framework for demo application
- **[Tabulate](https://github.com/astanin/python-tabulate)** - ASCII table formatting in REPL
- **Database Concepts** - General RDBMS principles (indexing, query execution, constraints)

## License

This project is for educational and demonstration purposes.

---

**Built with:** Python 3.9+ | Poetry | Flask | Lark
**Demonstrates:** SQL parsing, query execution, indexing, joins, clean modular architecture
