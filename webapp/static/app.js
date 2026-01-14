// Task Manager App - Frontend JavaScript

let tasks = [];
let categories = [];

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    loadCategories();
    loadTasks();

    document.getElementById('addTaskBtn').addEventListener('click', openModal);
    document.getElementById('addTaskForm').addEventListener('submit', handleAddTask);
    document.getElementById('filterCategory').addEventListener('change', filterTasks);
    document.getElementById('refreshBtn').addEventListener('click', () => {
        loadTasks();
        showNotification('Refreshed!');
    });
});

// Load categories from API
async function loadCategories() {
    try {
        const response = await fetch('/api/categories');
        categories = await response.json();

        // Populate filter dropdown
        const filterSelect = document.getElementById('filterCategory');
        filterSelect.innerHTML = '<option value="">All Categories</option>';
        categories.forEach(cat => {
            const option = document.createElement('option');
            option.value = cat.id;
            option.textContent = cat.name;
            filterSelect.appendChild(option);
        });

        // Populate category dropdown in add form
        const categorySelect = document.getElementById('taskCategory');
        categorySelect.innerHTML = '';
        categories.forEach(cat => {
            const option = document.createElement('option');
            option.value = cat.id;
            option.textContent = cat.name;
            categorySelect.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading categories:', error);
    }
}

// Load tasks from API (demonstrates INNER JOIN)
async function loadTasks() {
    try {
        const response = await fetch('/api/tasks');
        tasks = await response.json();
        renderTasks();
        updateStats();
    } catch (error) {
        console.error('Error loading tasks:', error);
        showNotification('Error loading tasks', 'error');
    }
}

// Render tasks to DOM
function renderTasks() {
    const taskList = document.getElementById('taskList');
    const filter = document.getElementById('filterCategory').value;

    const filteredTasks = filter
        ? tasks.filter(t => getCategoryIdByName(t.category) == filter)
        : tasks;

    if (filteredTasks.length === 0) {
        taskList.innerHTML = '<p style="text-align: center; color: #999; padding: 40px;">No tasks found. Add your first task!</p>';
        return;
    }

    taskList.innerHTML = filteredTasks.map(task => `
        <div class="task-card ${task.completed ? 'completed' : ''}" style="border-left-color: ${task.color}">
            <div class="task-header">
                <div>
                    <div class="task-title">${escapeHtml(task.title)}</div>
                    <span class="task-badge" style="background-color: ${task.color}">
                        ${escapeHtml(task.category)}
                    </span>
                </div>
            </div>
            <div class="task-description">${escapeHtml(task.description || 'No description')}</div>
            <div class="task-actions">
                <label class="checkbox-container">
                    <input type="checkbox"
                           ${task.completed ? 'checked' : ''}
                           onchange="toggleComplete(${task.id}, this.checked)">
                    <span>${task.completed ? 'Completed' : 'Mark Complete'}</span>
                </label>
                <button class="btn btn-danger" onclick="deleteTask(${task.id})">Delete</button>
            </div>
        </div>
    `).join('');
}

// Update statistics
function updateStats() {
    document.getElementById('totalTasks').textContent = tasks.length;
    document.getElementById('completedTasks').textContent =
        tasks.filter(t => t.completed).length;
}

// Open add task modal
function openModal() {
    document.getElementById('addTaskModal').classList.add('active');
    document.getElementById('addTaskForm').reset();
}

// Close modal
function closeModal() {
    document.getElementById('addTaskModal').classList.remove('active');
}

// Handle add task form submission
async function handleAddTask(e) {
    e.preventDefault();

    const title = document.getElementById('taskTitle').value;
    const description = document.getElementById('taskDescription').value;
    const category_id = parseInt(document.getElementById('taskCategory').value);

    try {
        const response = await fetch('/api/tasks', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                title,
                description,
                category_id,
                completed: false
            })
        });

        if (response.ok) {
            closeModal();
            await loadTasks();
            showNotification('Task created successfully!');
        } else {
            const error = await response.json();
            showNotification('Error: ' + error.error, 'error');
        }
    } catch (error) {
        console.error('Error creating task:', error);
        showNotification('Error creating task', 'error');
    }
}

// Toggle task completion
async function toggleComplete(taskId, completed) {
    try {
        const response = await fetch(`/api/tasks/${taskId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ completed })
        });

        if (response.ok) {
            await loadTasks();
            showNotification(completed ? 'Task completed!' : 'Task reopened');
        } else {
            showNotification('Error updating task', 'error');
        }
    } catch (error) {
        console.error('Error:', error);
        showNotification('Error updating task', 'error');
    }
}

// Delete task
async function deleteTask(taskId) {
    if (!confirm('Are you sure you want to delete this task?')) {
        return;
    }

    try {
        const response = await fetch(`/api/tasks/${taskId}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            await loadTasks();
            showNotification('Task deleted');
        } else {
            showNotification('Error deleting task', 'error');
        }
    } catch (error) {
        console.error('Error:', error);
        showNotification('Error deleting task', 'error');
    }
}

// Filter tasks by category
function filterTasks() {
    renderTasks();
}

// Helper functions
function getCategoryIdByName(name) {
    const cat = categories.find(c => c.name === name);
    return cat ? cat.id : null;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showNotification(message, type = 'success') {
    // Simple notification (could be enhanced with a library)
    const color = type === 'error' ? '#e74c3c' : '#2ecc71';
    const notification = document.createElement('div');
    notification.textContent = message;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: ${color};
        color: white;
        padding: 15px 20px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        z-index: 10000;
        animation: slideIn 0.3s ease;
    `;

    document.body.appendChild(notification);

    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

// Close modal when clicking outside
document.addEventListener('click', (e) => {
    const modal = document.getElementById('addTaskModal');
    if (e.target === modal) {
        closeModal();
    }
});
