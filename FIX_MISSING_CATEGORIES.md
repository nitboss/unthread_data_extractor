# Fix Missing Categories

This document describes how to use the new functionality to fix missing categories for conversations that resulted in empty migration categories during the original migration process.

## Overview

During the original category migration, approximately 2,269 conversations resulted in empty migration categories (showing as `'None' + 'None' -> ''` in the logs). This happened because:

1. The Unthread database didn't have the latest category/subcategory data
2. The conversations weren't categorized to begin with
3. The data was missing from the source

The `fix-missing-categories` command addresses these cases by:

1. **Extracting conversation IDs** from the migration log that resulted in empty categories
2. **Querying BigQuery** for category information using the provided SQL query
3. **Checking Unthread API** for any category overrides that may have been added after migration
4. **Using AI classification** when no data is found in BigQuery or Unthread API
5. **Updating conversations** with the found or generated category information

## Prerequisites

Before running the fix, ensure you have:

1. **BigQuery credentials**: The script uses `data/bq_connect.json` in the project root for BigQuery authentication
2. **OpenAI API key**: Set the `OPENAI_API_KEY` environment variable (optional, for AI classification)
3. **Unthread API access**: Ensure your API key and base URL are configured
4. **Migration log file**: The `logs/migrate_categories.log` file should be present

## Usage

### Basic Usage

```bash
unthread-extractor fix-missing-categories
```

This will:
- Extract conversation IDs from `logs/migrate_categories.log`
- Query BigQuery in batches of 100 IDs
- Process each conversation to find category data
- Update conversations in Unthread with found data

### Advanced Usage

```bash
unthread-extractor fix-missing-categories \
  --batch-size 50 \
  --log-file logs/migrate_categories.log \
  --log-level INFO
```

### Parameters

- `--batch-size`: Number of conversation IDs to query BigQuery in each batch (default: 100)
- `--log-file`: Path to the migration log file (default: `logs/migrate_categories.log`)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Data Sources Priority

The system tries to find category data in the following order:

1. **BigQuery**: Uses the provided SQL query to find category/subcategory/resolution data
2. **Unthread API**: Checks if any category overrides were added after migration
3. **AI Classification**: Uses the reclassify.md prompt to generate categories from conversation content

## SQL Query

The BigQuery query used is:

```sql
SELECT 
    conversation_id,
    ticket_category,
    ticket_sub_category,
    ticket_resolution
FROM dbt.stg_unthread__conversations uc
    JOIN all_users u ON uc.submitter_email = u.email
WHERE conversation_id IN ('id1', 'id2', ...)
ORDER BY conversation_id DESC
```

## Output

The command provides detailed logging and a summary at the end:

```
==================================================
PROCESSING SUMMARY
==================================================
Total conversations processed: 2269
Found in BigQuery: 1500
Found in Unthread API: 200
Classified with AI: 300
Successfully updated: 2000
Failed to update: 50
No data found: 219
==================================================
```

## Field Mappings

The system updates the following fields in Unthread:

- **Category Field ID**: `1a6900f6-36d2-4380-ad06-790b0b05c4b3`
- **Sub-Category Field ID**: `05492140-551c-49ea-a8a2-4caeec8cda4d`
- **Migration Category Field ID**: `0598cba1-31d1-466e-bfd1-812548c73c51`
- **Resolution Field ID**: `0598cba1-31d1-466e-bfd1-812548c73c52`

## Migration Category Format

The migration category is created by combining category and sub-category:
- If both exist: `"Category - Sub-Category"`
- If only category exists: `"Category"`
- If neither exists: `""` (empty string)

## Error Handling

The system handles various error scenarios:

- **BigQuery connection issues**: Logs error and continues with other data sources
- **Unthread API failures**: Logs error and continues with AI classification
- **AI classification failures**: Logs error and marks conversation as "no data found"
- **Update failures**: Logs error and continues with next conversation

## Testing

You can test the extraction of conversation IDs using the provided script:

```bash
python3 extract_missing_ids.py logs/migrate_categories.log
```

This will show you how many conversation IDs were found and display the first 10 as examples.

## Monitoring

Monitor the process through:

1. **Console output**: Real-time logging of processing status
2. **Log files**: Detailed logs for debugging
3. **Summary report**: Final statistics showing success/failure rates

## Troubleshooting

### Common Issues

1. **BigQuery credentials not found**: Ensure `BIGQUERY_CREDENTIALS_PATH` is set correctly
2. **OpenAI API key missing**: AI classification will be skipped, but other sources will still be used
3. **Unthread API errors**: Check API key and base URL configuration
4. **Log file not found**: Ensure the migration log file exists at the specified path

### Performance Considerations

- **Batch size**: Larger batches reduce BigQuery API calls but may hit SQL IN clause limits
- **Rate limiting**: The system includes delays between API calls to avoid rate limiting
- **Memory usage**: Processing is done in batches to manage memory usage

## Files Modified

The following files were added or modified:

- `src/unthread_extractor/fix_missing_categories.py` - Main implementation
- `src/unthread_extractor/cli.py` - Added CLI command
- `extract_missing_ids.py` - Testing script for conversation ID extraction
- `FIX_MISSING_CATEGORIES.md` - This documentation

## Example Workflow

1. **Prepare environment**:
   ```bash
   export BIGQUERY_CREDENTIALS_PATH="/path/to/service-account.json"
   export OPENAI_API_KEY="your-openai-api-key"
   ```

2. **Test extraction**:
   ```bash
   python3 extract_missing_ids.py logs/migrate_categories.log
   ```

3. **Run the fix**:
   ```bash
   unthread-extractor fix-missing-categories --log-level INFO
   ```

4. **Monitor progress** and review the final summary

5. **Verify results** by checking a few updated conversations in Unthread
