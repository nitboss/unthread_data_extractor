"""
Command Line Interface for Unthread Data Extractor
"""

import argparse
import logging
import sys
from datetime import datetime

from .api import UnthreadAPI
from .config import Config
from .extractor import UnthreadExtractor
from .storage import DuckDBStorage
from .updater import UnthreadUpdater
from .reclassify import process_conversations_batch
from .migrate_categories import CategoryMigrator
from .fix_missing_categories import MissingCategoryFixer

logger = logging.getLogger(__name__)

def setup_logging(level: str = "INFO"):
    """Set up logging configuration
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger.debug(f"Logging configured with level: {level}")

def parse_date(date_str: str) -> str:
    """Parse date string to ISO format
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        ISO formatted date string
        
    Raises:
        ValueError: If date string is in invalid format
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").isoformat()
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD format.")

def main():
    """Main entry point for the CLI"""
    parser = argparse.ArgumentParser(description='Extract data from Unthread API')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Users command
    users_parser = subparsers.add_parser('users', help='Extract users')

    # Conversations command
    conversations_parser = subparsers.add_parser('conversations', help='Extract conversations')
    conversations_parser.add_argument('--conversation-id', help='Extract a specific conversation by ID')
    conversations_parser.add_argument('--start-date', help='Filter conversations modified after this date (YYYY-MM-DD)')
    conversations_parser.add_argument('--end-date', help='Filter conversations modified before this date (YYYY-MM-DD)')
    conversations_parser.add_argument('--parallel', action='store_true', help='Use parallel processing for faster downloads')
    conversations_parser.add_argument('--max-workers', type=int, default=5, help='Maximum number of parallel workers (default: 5)')
    conversations_parser.add_argument('--batch-size', type=int, default=10, help='Number of conversations to process in each batch (default: 10)')

    # Messages command
    messages_parser = subparsers.add_parser('messages', help='Extract messages')
    messages_parser.add_argument('--conversation-id', required=True, help='Extract messages for a specific conversation')

    # Customers command
    customers_parser = subparsers.add_parser('customers', help='Extract customers')

    # All command
    all_parser = subparsers.add_parser('all', help='Extract all data')
    all_parser.add_argument('--parallel', action='store_true', help='Use parallel processing for faster downloads')
    all_parser.add_argument('--max-workers', type=int, default=5, help='Maximum number of parallel workers (default: 5)')
    all_parser.add_argument('--batch-size', type=int, default=10, help='Number of conversations to process in each batch (default: 10)')

    # Update command
    update_parser = subparsers.add_parser('update', help='Update conversations with classifications from database')
    update_parser.add_argument('--batch-size', type=int, default=50, help='Number of conversations to update in each batch (default: 50)')

    # Reclassify command
    reclassify_parser = subparsers.add_parser('reclassify', help='Reclassify conversations using LLM')
    reclassify_parser.add_argument('--batch-size', type=int, default=10, help='Number of conversations to process in each batch (default: 10)')
    reclassify_parser.add_argument('--max-conversations', type=int, default=100, help='Maximum number of conversations to process (default: 100)')

    # Migrate categories command
    migrate_parser = subparsers.add_parser('migrate-categories', help='Migrate ticket categories')
    migrate_parser.add_argument('--batch-size', type=int, default=50, help='Number of tickets to process per batch (default: 50)')
    migrate_parser.add_argument('--max-tickets', type=int, help='Maximum number of tickets to process (default: all)')
    migrate_parser.add_argument('--ticket-ids', nargs='+', help='Specific conversation IDs to migrate (space-separated)')
    migrate_parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without making API calls')

    # Fix missing categories command
    fix_categories_parser = subparsers.add_parser('fix-missing-categories', help='Fix missing categories for conversations that resulted in empty migration categories')
    fix_categories_parser.add_argument('--conversation-id', '-c', help='Test with a specific conversation ID')
    fix_categories_parser.add_argument('--batch-size', type=int, default=100, help='Batch size for BigQuery queries (default: 100)')
    fix_categories_parser.add_argument('--limit', type=int, help='Maximum number of conversations to process (default: all)')
    fix_categories_parser.add_argument('--log-file', default='logs/migrate_categories.log', help='Path to migration log file (default: logs/migrate_categories.log)')

    # Common arguments
    parser.add_argument('--log-level', default='INFO', help='Set the logging level')
    args = parser.parse_args()

    setup_logging(args.log_level)
    logger.debug("Starting Unthread Data Extractor")

    storage = None  # Initialize storage to None
    try:
        config = Config.from_env()
        logger.debug("Configuration loaded from environment")
        
        api = UnthreadAPI(config.api_key, config.base_url)
        storage = DuckDBStorage(config.db_path)
        extractor = UnthreadExtractor(api, storage)
        logger.debug("Extractor initialized")

        if args.command == 'users':
            logger.info("Starting users extraction...")
            extractor.download_users()
            logger.info("Users extraction completed successfully")

        elif args.command == 'customers':
            logger.info("Starting customers extraction...")
            extractor.download_customers()
            logger.info("Customers extraction completed successfully")

        elif args.command == 'conversations':
            logger.info("Starting conversations extraction...")
            modified_after = parse_date(args.start_date) if args.start_date else None
            modified_before = parse_date(args.end_date) if args.end_date else None
            
            if modified_after or modified_before:
                logger.debug(f"Date range: {modified_after} to {modified_before}")
            if args.conversation_id:
                logger.debug(f"Specific conversation ID: {args.conversation_id}")
            
            if args.parallel:
                logger.info(f"Using parallel processing with {args.max_workers} workers and batch size {args.batch_size}")
                extractor.download_conversations_parallel(
                    modified_after=modified_after,
                    modified_before=modified_before,
                    conversation_id=args.conversation_id,
                    max_workers=args.max_workers,
                    batch_size=args.batch_size
                )
            else:
                extractor.download_conversations(
                    modified_after=modified_after,
                    modified_before=modified_before,
                    conversation_id=args.conversation_id
                )
            logger.info("Conversations extraction completed successfully")

        elif args.command == 'messages':
            logger.info("Starting messages extraction...")
            logger.debug(f"Conversation ID: {args.conversation_id}")
            
            extractor.download_messages(args.conversation_id)
            logger.info("Messages extraction completed successfully")

        elif args.command == 'all':
            logger.info("Starting full data extraction...")
            extractor.download_users()
            if args.parallel:
                logger.info(f"Using parallel processing with {args.max_workers} workers and batch size {args.batch_size}")
                extractor.download_conversations_parallel(
                    max_workers=args.max_workers,
                    batch_size=args.batch_size
                )
            else:
                extractor.download_conversations()
            logger.info("Full data extraction completed successfully")

        elif args.command == 'update':
            logger.info("Starting update process...")
            updater = UnthreadUpdater(api, storage, batch_size=args.batch_size)
            try:
                results = updater.update_all_conversations()
                logger.info(f"Update process completed. Results: {results}")
            finally:
                updater.close()
            logger.info("Update process completed successfully")

        elif args.command == 'reclassify':
            logger.info("Starting reclassification process...")
            conversations = storage.get_conversations()
            try:
                process_conversations_batch(conversations, batch_size=args.batch_size, max_conversations=args.max_conversations)
                logger.info("Reclassification process completed successfully")
            except Exception as e:
                logger.error(f"Error during reclassification: {str(e)}", exc_info=True)
                sys.exit(1)

        elif args.command == 'migrate-categories':
            logger.info("Starting category migration process...")
            try:
                migrator = CategoryMigrator(storage, api)
                
                if args.ticket_ids:
                    # Migrate specific tickets
                    logger.info(f"Migrating specific tickets: {args.ticket_ids}")
                    
                    if args.dry_run:
                        logger.info("DRY RUN MODE - No API calls will be made")
                        tickets = migrator.get_tickets_by_ids(args.ticket_ids)
                        logger.info(f"Would process {len(tickets)} specific tickets")
                        for ticket in tickets:
                            migration_category = migrator.create_migration_category(
                                ticket['category'], 
                                ticket['sub_category']
                            )
                            logger.info(f"Ticket {ticket['conversation_id']}: {ticket['category']} + {ticket['sub_category']} -> {migration_category}")
                    else:
                        results = migrator.migrate_specific_tickets(args.ticket_ids)
                        
                        logger.info("Migration Summary:")
                        logger.info(f"  Total processed: {results['total_processed']}")
                        logger.info(f"  Successful: {results['total_successful']}")
                        logger.info(f"  Failed: {results['total_failed']}")
                        
                        if results['all_errors']:
                            logger.warning(f"  Errors: {len(results['all_errors'])}")
                            for error in results['all_errors'][:5]:  # Show first 5 errors
                                logger.warning(f"    {error['conversation_id']}: {error['error']}")
                else:
                    # Migrate all tickets with pagination
                    if args.dry_run:
                        logger.info("DRY RUN MODE - No API calls will be made")
                        # For dry run, just show what would be migrated
                        tickets = migrator.get_tickets_with_pagination(args.batch_size, 0)
                        logger.info(f"Would process {len(tickets)} tickets in first batch")
                        for ticket in tickets[:5]:  # Show first 5 as examples
                            migration_category = migrator.create_migration_category(
                                ticket['category'], 
                                ticket['sub_category']
                            )
                            logger.info(f"Ticket {ticket['conversation_id']}: {ticket['category']} + {ticket['sub_category']} -> {migration_category}")
                    else:
                        # Perform actual migration
                        results = migrator.migrate_all_tickets(
                            batch_size=args.batch_size,
                            max_tickets=args.max_tickets
                        )
                        
                        logger.info("Migration Summary:")
                        logger.info(f"  Total processed: {results['total_processed']}")
                        logger.info(f"  Successful: {results['total_successful']}")
                        logger.info(f"  Failed: {results['total_failed']}")
                        logger.info(f"  Batches processed: {results['batches_processed']}")
                        
                        if results['all_errors']:
                            logger.warning(f"  Errors: {len(results['all_errors'])}")
                            for error in results['all_errors'][:5]:  # Show first 5 errors
                                logger.warning(f"    {error['conversation_id']}: {error['error']}")
                
                logger.info("Category migration process completed successfully")
            except Exception as e:
                logger.error(f"Error during category migration: {str(e)}", exc_info=True)
                sys.exit(1)

        elif args.command == 'fix-missing-categories':
            logger.debug("Starting missing categories fix process...")
            try:
                # Initialize fixer (uses data/bq_connect.json for BigQuery auth)
                fixer = MissingCategoryFixer(storage, api)
                
                if args.conversation_id:
                    # Test with specific conversation ID
                    logger.debug(f"Processing conversation ID: {args.conversation_id}")
                    conversation_ids = [args.conversation_id]
                else:
                    # Extract conversation IDs from log
                    conversation_ids = fixer.extract_conversation_ids_from_log(args.log_file)
                    
                    if not conversation_ids:
                        logger.warning("No conversations with missing categories found")
                        return
                
                # Process conversations
                stats = fixer.process_conversations(conversation_ids, args.batch_size, args.limit)
                
                # Print summary
                logger.info("PROCESSING SUMMARY")
                logger.info("="*50)
                logger.info(f"Total conversations processed: {stats['total_conversations']}")
                logger.info(f"Found in BigQuery: {stats['bigquery_found']}")
                logger.info(f"Found in Unthread API: {stats['unthread_api_found']}")
                logger.info(f"Classified with AI: {stats['ai_classified']}")
                logger.info(f"Successfully updated: {stats['updated_successfully']}")
                logger.info(f"Failed to update: {stats['failed']}")
                logger.info(f"No data found: {stats['no_data_found']}")
                logger.info("="*50)
                
                logger.debug("Missing categories fix process completed successfully")
            except Exception as e:
                logger.error(f"Error during missing categories fix: {str(e)}", exc_info=True)
                sys.exit(1)

        else:
            logger.warning("No command specified")
            parser.print_help()
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        logger.debug("Closing storage connection")
        if storage is not None:
            storage.close()
        logger.info("Extraction process completed")

if __name__ == '__main__':
    main() 