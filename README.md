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

# Extract conversations modified within a date range
unthread-extractor conversations --start-date 2024-01-01 --end-date 2024-03-15

# Extract conversations with parallel processing for faster downloads
unthread-extractor conversations --start-date 2024-01-01 --end-date 2024-03-15 --parallel

# Extract conversations with custom parallel processing settings
unthread-extractor conversations --start-date 2024-01-01 --end-date 2024-03-15 --parallel --max-workers 10 --batch-size 20
```

### Extract Messages

```bash
# Extract messages for a specific conversation
unthread-extractor messages --conversation-id <id>
```

### Extract All Data

```bash
# Extract all data sequentially
unthread-extractor all

# Extract all data with parallel processing for faster downloads
unthread-extractor all --parallel

# Extract all data with custom parallel processing settings
unthread-extractor all --parallel --max-workers 10 --batch-size 20
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

### Migrate Ticket Categories

```bash
# Migrate all ticket categories (default batch size: 50)
unthread-extractor migrate-categories

# Migrate with custom batch size
unthread-extractor migrate-categories --batch-size 25

# Limit the number of tickets to process
unthread-extractor migrate-categories --max-tickets 100

# Migrate specific tickets by ID
unthread-extractor migrate-categories --ticket-ids 00024725-496d-40fd-b7eb-0accc6e4badb 00121463-07ef-41d9-8432-14d2d4d0753b

# Dry run to see what would be migrated without making API calls
unthread-extractor migrate-categories --dry-run
```

This command will:
1. List all tickets from the local DuckDB instance with pagination (or specific tickets by ID)
2. Read the category and sub-category fields from the `ticketTypeFields` JSON of each ticket
3. Create a new `migration_category` field by combining category and sub-category values
4. Save the new custom field back into Unthread via an API call (preserving existing fields)
5. Process tickets in batches to avoid overwhelming the API
6. Track progress with detailed logging and error reporting

**Field Mappings:**
- Category Field ID: `1a6900f6-36d2-4380-ad06-790b0b05c4b3`
- Sub-Category Field ID: `05492140-551c-49ea-a8a2-4caeec8cda4d`
- Migration Category Field ID: `0598cba1-31d1-466e-bfd1-812548c73c51`

**Migration Logic:**
- If both category and sub-category exist: `"{category} - {sub_category}"`
- If only category exists: `"{category}"`
- If only sub-category exists: `"{sub_category}"`
- If neither exists: `""` (empty string)

**Standalone Script:**
You can also run the migration directly using the standalone script:

```bash
# Make the script executable (first time only)
chmod +x migrate_categories.py

# Run the migration
./migrate_categories.py --batch-size 50 --max-tickets 100

# Dry run
./migrate_categories.py --dry-run
```

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

## Performance Optimization

### Parallel Processing

For large datasets, you can significantly improve download speed using parallel processing:

```bash
# Enable parallel processing with default settings (5 workers, batch size 10)
unthread-extractor conversations --parallel

# Customize parallel processing
unthread-extractor conversations --parallel --max-workers 10 --batch-size 20
```

**Performance Benefits:**
- **3-5x faster** downloads for large datasets
- **Concurrent API requests** reduce total wait time
- **Batch processing** optimizes memory usage
- **Thread-safe** implementation with separate API clients per thread

**Recommended Settings:**
- **Small datasets (< 1000 conversations)**: Use default settings (`--max-workers 5`)
- **Medium datasets (1000-10000 conversations)**: Use `--max-workers 10`
- **Large datasets (> 10000 conversations)**: Use `--max-workers 15-20`

**Important Notes:**
- Parallel processing uses more memory and CPU
- Be mindful of API rate limits when increasing worker count
- Monitor your API usage to avoid hitting limits
- Start with lower worker counts and increase gradually

### Performance Testing

Run the performance test script to compare sequential vs parallel processing:

```bash
python performance_test.py
```

This will test both methods on a 7-day dataset and show speedup metrics.

## Development
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

