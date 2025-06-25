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

### Update Conversations

```bash
# Update conversations with classifications from database (default batch size: 50)
unthread-extractor update

# Update with custom batch size
unthread-extractor update --batch-size 25
```

This command will:
1. Extract classification data (category and resolution) from the `conversation_classifications` table
2. Update conversations via the Unthread API using PATCH `/conversations/:conversationId`
3. Set the `category` and `resolution` fields in the `ticketTypeFields` JSON object
4. Process conversations in batches to avoid overwhelming the API
5. Track progress with detailed logging
6. Mark successfully updated conversations with an `updated_time` timestamp in the database

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

### Conversation Classifications

The `conversation_classifications` table stores classification results:

```sql
-- Get all classifications
SELECT * FROM conversation_classifications;

-- Get classifications that need updating
SELECT * FROM conversation_classifications 
WHERE category IS NOT NULL 
  AND resolution IS NOT NULL 
  AND (updated_time IS NULL OR updated_time < created_at);

-- Get update statistics
SELECT 
    COUNT(*) as total_classifications,
    COUNT(updated_time) as updated_count,
    COUNT(*) - COUNT(updated_time) as pending_updates
FROM conversation_classifications;
```

The table includes:
- `conversation_id`: Primary key linking to conversations
- `category`: The assigned category
- `sub_category`: The assigned sub-category  
- `reasoning`: The reasoning for the classification
- `resolution`: The resolution status
- `created_at`: When the classification was created
- `updated_time`: When the conversation was successfully updated via API (NULL if not yet updated)
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

