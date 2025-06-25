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
    conversations_parser.add_argument('--start-date', help='Filter conversations created after this date (YYYY-MM-DD)')
    conversations_parser.add_argument('--end-date', help='Filter conversations created before this date (YYYY-MM-DD)')

    # Messages command
    messages_parser = subparsers.add_parser('messages', help='Extract messages')
    messages_parser.add_argument('--conversation-id', required=True, help='Extract messages for a specific conversation')

    # Customers command
    customers_parser = subparsers.add_parser('customers', help='Extract customers')

    # All command
    all_parser = subparsers.add_parser('all', help='Extract all data')

    # Update command
    update_parser = subparsers.add_parser('update', help='Update conversations with classifications from database')
    update_parser.add_argument('--batch-size', type=int, default=50, help='Number of conversations to update in each batch (default: 50)')

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
            created_after = parse_date(args.start_date) if args.start_date else None
            created_before = parse_date(args.end_date) if args.end_date else None
            
            if created_after or created_before:
                logger.debug(f"Date range: {created_after} to {created_before}")
            if args.conversation_id:
                logger.debug(f"Specific conversation ID: {args.conversation_id}")
                
            extractor.download_conversations(
                created_after=created_after,
                created_before=created_before,
                conversation_id=args.conversation_id
            )
            logger.info("Conversations extraction completed successfully")

        elif args.command == 'messages':
            logger.info("Starting messages extraction...")
            start_date = getattr(args, "start_date", None)
            end_date = getattr(args, "end_date", None)
            created_after = parse_date(start_date) if start_date else None
            created_before = parse_date(end_date) if end_date else None

            if created_after or created_before:
                logger.debug(f"Date range: {created_after} to {created_before}")
            logger.debug(f"Conversation ID: {args.conversation_id}")
            
            extractor.download_messages(
                args.conversation_id,
                created_after=created_after,
                created_before=created_before
            )
            logger.info("Messages extraction completed successfully")

        elif args.command == 'all':
            logger.info("Starting full data extraction...")
            extractor.download_users()
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