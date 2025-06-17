# Unthread Data Extractor

A command-line tool to extract data from the Unthread API and store it in a DuckDB database.

## Installation

```bash
pip install unthread-data-extractor
```

## Configuration

Set your Unthread API key as an environment variable:

```bash
export UNTHREAD_API_KEY=your_api_key_here
```

## Usage

The tool provides several commands to extract different types of data:

### Extract Users

```bash
unthread-extractor users
```

This will download all users from the Unthread API and store them in the database.

### Extract Conversations

```bash
# Extract all conversations
unthread-extractor conversations

# Extract a specific conversation
unthread-extractor conversations --conversation-id <id>

# Extract conversations within a date range
unthread-extractor conversations --start-date 2024-01-01 --end-date 2024-03-15
```

### Extract Messages

```bash
# Extract messages for a specific conversation
unthread-extractor messages --conversation-id <id>

# Extract messages within a date range
unthread-extractor messages --start-date 2024-01-01 --end-date 2024-03-15
```

### Extract All Data

```bash
unthread-extractor all
```

This will download all users, conversations, and messages.

## Database

The data is stored in a DuckDB database at `data/unthread_data.duckdb`. You can query this database using any SQL client that supports DuckDB.

Example queries:

```sql
-- Get all users
SELECT * FROM users;

-- Get all conversations
SELECT * FROM conversations;

-- Get all messages for a conversation
SELECT * FROM messages WHERE conversation_id = '<id>';

-- Get messages with user information
SELECT m.*, u.data as user_data
FROM messages m
JOIN users u ON m.data->>'user' = u.id
WHERE m.conversation_id = '<id>';
```

## Development

1. Clone the repository:
```bash
git clone https://github.com/yourusername/unthread-data-extractor.git
cd unthread-data-extractor
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e .
```

4. Run tests:
```bash
pytest tests/
```

## License

MIT

