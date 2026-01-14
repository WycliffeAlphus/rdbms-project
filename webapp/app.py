"""
Flask web application demonstrating the RDBMS.

Task Manager with Categories - demonstrates:
- CREATE TABLE
- INSERT, SELECT, UPDATE, DELETE
- INNER JOIN
- WHERE clauses
- PRIMARY KEY and UNIQUE constraints
- Indexing
"""

from flask import Flask, render_template, request, jsonify
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rdbms.storage.database import Database
from rdbms.parser.parser import SQLParser
from rdbms.executor.executor import QueryExecutor
from rdbms.utils.exceptions import RDBMSError

app = Flask(__name__)

# Initialize database
db = Database("taskmanager")
parser = SQLParser()
executor = QueryExecutor(db)

def init_database():
    """Initialize database schema."""
    try:
        # Create categories table
        sql = """
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) UNIQUE,
            color VARCHAR(7)
        )
        """
        executor.execute(parser.parse(sql))

        # Create tasks table
        sql = """
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            title VARCHAR(200),
            description TEXT,
            category_id INTEGER,
            completed BOOLEAN
        )
        """
        executor.execute(parser.parse(sql))

        # Create index on category_id for faster joins
        sql = "CREATE INDEX idx_category_id ON tasks(category_id)"
        executor.execute(parser.parse(sql))

        # Insert sample categories
        categories = [
            (1, 'Work', '#3498db'),
            (2, 'Personal', '#2ecc71'),
            (3, 'Shopping', '#e74c3c')
        ]
        for cat_id, name, color in categories:
            sql = f"INSERT INTO categories (id, name, color) VALUES ({cat_id}, '{name}', '{color}')"
            executor.execute(parser.parse(sql))

        # Insert sample tasks
        tasks = [
            (1, 'Finish RDBMS project', 'Complete implementation and tests', 1, False),
            (2, 'Write documentation', 'README and code comments', 1, False),
            (3, 'Buy groceries', 'Milk, eggs, bread', 3, False),
            (4, 'Call mom', 'Weekly check-in', 2, True)
        ]
        for task_id, title, desc, cat_id, completed in tasks:
            completed_str = 'TRUE' if completed else 'FALSE'
            sql = f"INSERT INTO tasks (id, title, description, category_id, completed) VALUES ({task_id}, '{title}', '{desc}', {cat_id}, {completed_str})"
            executor.execute(parser.parse(sql))

        print("✓ Database initialized with sample data")
    except Exception as e:
        print(f"Database already initialized or error: {e}")


@app.route('/')
def index():
    """Main page."""
    return render_template('index.html')


@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    """Get all tasks with category info (demonstrates JOIN)."""
    try:
        # Use JOIN to get tasks with category names
        sql = """
        SELECT tasks.id, tasks.title, tasks.description, tasks.completed,
               categories.name, categories.color
        FROM tasks
        INNER JOIN categories ON tasks.category_id = categories.id
        """

        result = executor.execute(parser.parse(sql))

        # Format for JSON (handle qualified column names)
        tasks = []
        for row in result:
            tasks.append({
                'id': row.get('tasks.id'),
                'title': row.get('tasks.title'),
                'description': row.get('tasks.description'),
                'completed': row.get('tasks.completed'),
                'category': row.get('categories.name'),
                'color': row.get('categories.color')
            })

        return jsonify(tasks)
    except RDBMSError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/tasks', methods=['POST'])
def create_task():
    """Create a new task."""
    try:
        data = request.json

        # Get next ID
        result = executor.execute(parser.parse("SELECT id FROM tasks"))
        next_id = max([r['id'] for r in result], default=0) + 1

        # Escape single quotes in strings
        title = data['title'].replace("'", "''")
        description = data.get('description', '').replace("'", "''")
        category_id = data['category_id']
        completed = 'TRUE' if data.get('completed', False) else 'FALSE'

        sql = f"""
        INSERT INTO tasks (id, title, description, category_id, completed)
        VALUES ({next_id}, '{title}', '{description}', {category_id}, {completed})
        """

        executor.execute(parser.parse(sql))

        return jsonify({'id': next_id, 'message': 'Task created'}), 201
    except RDBMSError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/tasks/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    """Update a task."""
    try:
        data = request.json

        updates = []
        if 'title' in data:
            title = data['title'].replace("'", "''")
            updates.append(f"title = '{title}'")
        if 'description' in data:
            desc = data['description'].replace("'", "''")
            updates.append(f"description = '{desc}'")
        if 'completed' in data:
            completed = 'TRUE' if data['completed'] else 'FALSE'
            updates.append(f"completed = {completed}")
        if 'category_id' in data:
            updates.append(f"category_id = {data['category_id']}")

        if updates:
            sql = f"UPDATE tasks SET {', '.join(updates)} WHERE id = {task_id}"
            count = executor.execute(parser.parse(sql))

            if count == 0:
                return jsonify({'error': 'Task not found'}), 404

            return jsonify({'message': 'Task updated'})

        return jsonify({'message': 'No updates provided'}), 400
    except RDBMSError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    """Delete a task."""
    try:
        sql = f"DELETE FROM tasks WHERE id = {task_id}"
        count = executor.execute(parser.parse(sql))

        if count == 0:
            return jsonify({'error': 'Task not found'}), 404

        return jsonify({'message': 'Task deleted'})
    except RDBMSError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get all categories."""
    try:
        sql = "SELECT * FROM categories"
        result = executor.execute(parser.parse(sql))
        return jsonify(result)
    except RDBMSError as e:
        return jsonify({'error': str(e)}), 400


if __name__ == '__main__':
    init_database()
    print("\n" + "="*60)
    print("Task Manager Web App Running!")
    print("Open http://localhost:5000 in your browser")
    print("="*60 + "\n")
    app.run(debug=True, port=5000)
