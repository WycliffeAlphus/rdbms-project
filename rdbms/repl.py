"""
REPL (Read-Eval-Print Loop) for interactive SQL queries.

Provides command-line interface to the database.
"""

import sys
import select
from .storage.database import Database
from .parser.parser import SQLParser
from .executor.executor import QueryExecutor
from .formatter import format_select_result, format_modify_result, format_ddl_result
from .parser import ast
from .utils.exceptions import RDBMSError


def has_pending_input():
    """Check if there's input waiting in stdin (indicates paste)."""
    if not sys.stdin.isatty():
        return True
    try:
        r, _, _ = select.select([sys.stdin], [], [], 0)
        return bool(r)
    except (ValueError, OSError, TypeError):
        return False


def read_line_raw(prompt, suppress_if_pending=False):
    """
    Read a line using raw stdin to avoid readline interference.

    This prevents issues with paste operations where prompts
    can get mixed into the input buffer.
    """
    # Suppress prompt during paste operations
    if suppress_if_pending and has_pending_input():
        prompt = ""
    sys.stdout.write(prompt)
    sys.stdout.flush()
    line = sys.stdin.readline()
    if not line:  # EOF
        raise EOFError()
    return line.rstrip('\n\r')


def print_banner():
    """Print welcome banner."""
    print("=" * 60)
    print("  Simple RDBMS - Interactive SQL Shell")
    print("=" * 60)
    print("Type SQL commands or special commands:")
    print("  Multi-line input supported - end with semicolon (;)")
    print("  .help     - Show help")
    print("  .tables   - List all tables")
    print("  .schema TABLE - Show table schema")
    print("  .exit or .quit - Exit REPL")
    print("=" * 60)
    print()


def print_help():
    """Print help message."""
    print("\n--- Help ---")
    print("SQL Commands:")
    print("  CREATE TABLE name (col TYPE constraints, ...)")
    print("  DROP TABLE name")
    print("  CREATE INDEX name ON table(column)")
    print("  INSERT INTO table (cols...) VALUES (vals...)")
    print("  SELECT cols FROM table [WHERE condition] [JOIN...]")
    print("  UPDATE table SET col=val [WHERE condition]")
    print("  DELETE FROM table [WHERE condition]")
    print("\nSpecial Commands:")
    print("  .help     - Show this help")
    print("  .tables   - List all tables")
    print("  .schema TABLE - Show schema for TABLE")
    print("  .stats    - Show database statistics")
    print("  .exit / .quit - Exit REPL")
    print()


def handle_special_command(command: str, database: Database) -> bool:
    """
    Handle special REPL commands (starting with .).

    Args:
        command: Command string
        database: Database instance

    Returns:
        True if should continue REPL, False to exit
    """
    command = command.strip().lower()

    if command in ['.exit', '.quit']:
        print("Goodbye!")
        return False

    elif command == '.help':
        print_help()

    elif command == '.tables':
        tables = database.list_tables()
        if tables:
            print("\nTables:")
            for table in tables:
                row_count = database.get_table(table).row_count()
                print(f"  - {table} ({row_count} rows)")
        else:
            print("\nNo tables.")
        print()

    elif command.startswith('.schema'):
        parts = command.split()
        if len(parts) < 2:
            print("Usage: .schema TABLE_NAME")
        else:
            table_name = parts[1]
            try:
                table = database.get_table(table_name)
                print(f"\nSchema for table '{table_name}':")
                for col_name in table.column_order:
                    col = table.columns[col_name]
                    constraints = ', '.join(c.value for c in col.constraints)
                    type_str = col.data_type.value
                    if col.max_length:
                        type_str += f"({col.max_length})"
                    if constraints:
                        print(f"  {col_name}: {type_str} [{constraints}]")
                    else:
                        print(f"  {col_name}: {type_str}")
                print()
            except Exception as e:
                print(f"Error: {e}\n")

    elif command == '.stats':
        stats = database.get_stats()
        print(f"\nDatabase Statistics:")
        print(f"  Name: {stats['name']}")
        print(f"  Tables: {stats['table_count']}")
        for table_name, table_stats in stats['tables'].items():
            print(f"    - {table_name}:")
            print(f"        Rows: {table_stats['row_count']}")
            print(f"        Columns: {table_stats['column_count']}")
            print(f"        Indexes: {table_stats['index_count']}")
        print()

    else:
        print(f"Unknown command: {command}")
        print("Type .help for available commands\n")

    return True


def repl():
    """
    Run the interactive REPL.

    Reads SQL commands, executes them, and displays results.
    Supports multi-line input - continues reading until semicolon is found.
    """
    print_banner()

    # Initialize components
    database = Database("interactive")
    parser = SQLParser()
    executor = QueryExecutor(database)

    # Main loop
    while True:
        try:
            # Read command (possibly multi-line)
            sql_lines = []
            is_continuation = False

            while True:
                try:
                    if is_continuation:
                        # Suppress continuation prompt during paste
                        line = read_line_raw("    -> ", suppress_if_pending=True).strip()
                    else:
                        line = read_line_raw("rdbms> ").strip()
                except EOFError:
                    print("\nGoodbye!")
                    return

                # Accumulate non-empty lines
                if line:
                    sql_lines.append(line)

                    # Check if this is a special command
                    if line.startswith('.'):
                        break

                    # Check if statement is complete (ends with semicolon)
                    if line.endswith(';'):
                        break

                    # Continue reading
                    is_continuation = True
                else:
                    # Empty line with no accumulated content
                    if not sql_lines:
                        break
                    # Empty line in middle of statement, continue
                    is_continuation = True

            # Join all lines into complete SQL statement
            sql = ' '.join(sql_lines)

            if not sql:
                continue

            # Handle special commands
            if sql.startswith('.'):
                if not handle_special_command(sql, database):
                    break
                continue

            # Parse SQL
            try:
                statement = parser.parse(sql)
            except RDBMSError as e:
                print(f"Syntax Error: {e}\n")
                continue

            # Execute statement
            try:
                result = executor.execute(statement)

                # Format and display result
                if isinstance(statement, ast.SelectStmt):
                    print(format_select_result(result))
                elif isinstance(statement, ast.InsertStmt):
                    print(format_modify_result(result, "INSERT"))
                elif isinstance(statement, ast.UpdateStmt):
                    print(format_modify_result(result, "UPDATE"))
                elif isinstance(statement, ast.DeleteStmt):
                    print(format_modify_result(result, "DELETE"))
                elif isinstance(statement, ast.CreateTableStmt):
                    print(format_ddl_result("CREATE TABLE", statement.table_name))
                elif isinstance(statement, ast.DropTableStmt):
                    print(format_ddl_result("DROP TABLE", statement.table_name))
                elif isinstance(statement, ast.CreateIndexStmt):
                    print(format_ddl_result("CREATE INDEX", statement.index_name))

                print()

            except RDBMSError as e:
                print(f"Error: {e}\n")
                continue

        except KeyboardInterrupt:
            print("\n\nInterrupted. Type .exit to quit.\n")
            continue
        except Exception as e:
            print(f"Unexpected error: {e}\n")
            import traceback
            traceback.print_exc()


# Entry point for running as module
if __name__ == "__main__":
    repl()
