"""
Migration tool for ticket categories
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from .storage import DuckDBStorage
from .api import UnthreadAPI
from .config import Config

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Field mappings
CATEGORY_FIELD_ID = "1a6900f6-36d2-4380-ad06-790b0b05c4b3"
SUB_CATEGORY_FIELD_ID = "05492140-551c-49ea-a8a2-4caeec8cda4d"
MIGRATION_CATEGORY_FIELD_ID = "0598cba1-31d1-466e-bfd1-812548c73c51"

class CategoryMigrator:
    """Handles migration of ticket categories"""
    
    def __init__(self, storage: DuckDBStorage, api: UnthreadAPI):
        """Initialize the migrator
        
        Args:
            storage: DuckDB storage instance
            api: Unthread API instance
        """
        self.storage = storage
        self.api = api
        logger.info("CategoryMigrator initialized with storage and API instances")
        logger.debug(f"Using field IDs - Category: {CATEGORY_FIELD_ID}, Sub-category: {SUB_CATEGORY_FIELD_ID}, Migration: {MIGRATION_CATEGORY_FIELD_ID}")
    
    def get_tickets_with_pagination(self, page_size: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get tickets from DuckDB with pagination
        
        Args:
            page_size: Number of tickets to fetch per page
            offset: Number of tickets to skip
            
        Returns:
            List of ticket data dictionaries
        """
        logger.debug(f"Fetching tickets with pagination - page_size: {page_size}, offset: {offset}")
        
        query = """
            SELECT 
                c.id as conversation_id,
                c.data as conversation_data
            FROM conversations c
            WHERE c.data IS NOT NULL
            ORDER BY c.id
            LIMIT ? OFFSET ?
        """
        
        try:
            logger.debug(f"Executing query with parameters: page_size={page_size}, offset={offset}")
            results = self.storage.conn.execute(query, [page_size, offset]).fetchall()
            tickets = []
            
            logger.debug(f"Raw query returned {len(results)} rows")
            
            for row in results:
                conversation_id = row[0]
                conversation_data = json.loads(row[1]) if row[1] else {}
                
                # Extract ticket type fields
                ticket_type_fields = conversation_data.get('ticketTypeFields', {})
                
                ticket = {
                    'conversation_id': conversation_id,
                    'conversation_data': conversation_data,
                    'ticket_type_fields': ticket_type_fields,
                    'category': ticket_type_fields.get(CATEGORY_FIELD_ID),
                    'sub_category': ticket_type_fields.get(SUB_CATEGORY_FIELD_ID),
                }
                tickets.append(ticket)
                
                logger.debug(f"Processed ticket {conversation_id}: category='{ticket['category']}', sub_category='{ticket['sub_category']}'")
            
            logger.debug(f"Retrieved {len(tickets)} tickets (offset: {offset}, limit: {page_size})")
            return tickets
            
        except Exception as e:
            logger.error(f"Error retrieving tickets with pagination: {str(e)}")
            raise
    
    def get_tickets_by_ids(self, conversation_ids: List[str]) -> List[Dict[str, Any]]:
        """Get specific tickets by their conversation IDs
        
        Args:
            conversation_ids: List of conversation IDs to fetch
            
        Returns:
            List of ticket data dictionaries
        """
        if not conversation_ids:
            logger.debug("No conversation IDs provided, returning empty list")
            return []
        
        logger.debug(f"Fetching {len(conversation_ids)} specific tickets by IDs")
        logger.debug(f"Conversation IDs: {conversation_ids}")
        
        # Create placeholders for the IN clause
        placeholders = ','.join(['?' for _ in conversation_ids])
        query = f"""
            SELECT 
                c.id as conversation_id,
                c.data as conversation_data
            FROM conversations c
            WHERE c.id IN ({placeholders})
            AND c.data IS NOT NULL
        """
        
        try:
            logger.debug(f"Executing query for specific tickets with {len(conversation_ids)} IDs")
            results = self.storage.conn.execute(query, conversation_ids).fetchall()
            tickets = []
            
            logger.debug(f"Query returned {len(results)} results for {len(conversation_ids)} requested IDs")
            
            for row in results:
                conversation_id = row[0]
                conversation_data = json.loads(row[1]) if row[1] else {}
                
                # Extract ticket type fields
                ticket_type_fields = conversation_data.get('ticketTypeFields', {})
                
                ticket = {
                    'conversation_id': conversation_id,
                    'conversation_data': conversation_data,
                    'ticket_type_fields': ticket_type_fields,
                    'category': ticket_type_fields.get(CATEGORY_FIELD_ID),
                    'sub_category': ticket_type_fields.get(SUB_CATEGORY_FIELD_ID),
                }
                tickets.append(ticket)
                
                logger.debug(f"Processed specific ticket {conversation_id}: category='{ticket['category']}', sub_category='{ticket['sub_category']}'")
            
            logger.debug(f"Retrieved {len(tickets)} tickets for {len(conversation_ids)} requested IDs")
            if len(tickets) != len(conversation_ids):
                missing_ids = set(conversation_ids) - {t['conversation_id'] for t in tickets}
                logger.warning(f"Missing {len(missing_ids)} tickets: {missing_ids}")
            
            return tickets
            
        except Exception as e:
            logger.error(f"Error retrieving tickets by IDs: {str(e)}")
            raise
    
    def create_migration_category(self, category: Optional[str], sub_category: Optional[str]) -> str:
        """Create migration category based on category and sub-category
        
        Args:
            category: The category value
            sub_category: The sub-category value
            
        Returns:
            Migration category string (empty if both fields are blank)
        """
        logger.debug(f"Creating migration category from category='{category}', sub_category='{sub_category}'")
        
        # If both fields are blank, return empty string
        if not category and not sub_category:
            logger.debug("Both category and sub_category are empty, returning empty string")
            return ""
        
        # If both fields have values, combine them
        if category and sub_category:
            migration_category = f"{category} - {sub_category}"
            logger.debug(f"Combined category and sub_category: '{migration_category}'")
            return migration_category
        # If only category has value, return just category
        elif category:
            logger.debug(f"Using category only: '{category}'")
            return category
        # If only sub_category has value, return just sub_category
        else:
            logger.debug(f"Using sub_category only: '{sub_category}'")
            return sub_category
    
    def update_ticket_fields(self, conversation_id: str, migration_category: str, existing_fields: Dict[str, Any]) -> bool:
        """Update ticket fields in Unthread via API
        
        Args:
            conversation_id: The conversation ID to update
            migration_category: The migration category value to set
            existing_fields: Existing ticketTypeFields to preserve
            
        Returns:
            True if successful, False otherwise
        """
        logger.debug(f"Updating ticket fields for conversation {conversation_id}")
        logger.debug(f"Migration category: '{migration_category}'")
        logger.debug(f"Existing fields count: {len(existing_fields)}")
        
        try:
            # Create a copy of existing fields and add/update the migration category
            updated_fields = existing_fields.copy()
            updated_fields[MIGRATION_CATEGORY_FIELD_ID] = migration_category
            
            logger.debug(f"Updated fields count: {len(updated_fields)}")
            
            # Prepare the update data
            update_data = {
                "ticketTypeFields": updated_fields
            }
            
            logger.debug(f"Making API request to update conversation {conversation_id}")
            
            # Make API call to update the conversation
            endpoint = f"/conversations/{conversation_id}"
            response_data, _, _ = self.api.make_api_request(
                endpoint=endpoint,
                method="PATCH",
                data=update_data
            )
            
            logger.debug(f"API response received for conversation {conversation_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating conversation {conversation_id}: {str(e)}")
            return False
    
    def migrate_batch(self, tickets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Migrate a batch of tickets
        
        Args:
            tickets: List of ticket data dictionaries
            
        Returns:
            Dictionary with migration results
        """
        results = {
            'total': len(tickets),
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        for i, ticket in enumerate(tickets, 1):
            try:
                conversation_id = ticket['conversation_id']
                category = ticket['category']
                sub_category = ticket['sub_category']
                
                logger.debug(f"Processing ticket {i}/{len(tickets)}: {conversation_id}")
                
                # Create migration category
                migration_category = self.create_migration_category(category, sub_category)
                
                # Get existing ticket type fields to preserve them
                existing_fields = ticket['ticket_type_fields']
                
                logger.debug(f"Ticket {conversation_id}: category='{category}' + sub_category='{sub_category}' -> migration_category='{migration_category}'")
                
                # Update via API
                success = self.update_ticket_fields(conversation_id, migration_category, existing_fields)
                
                if success:
                    results['successful'] += 1
                    # One INFO line per conversation showing migration details
                    logger.info(f"Migrated {conversation_id}: '{category}' + '{sub_category}' -> '{migration_category}'")
                else:
                    results['failed'] += 1
                    results['errors'].append({
                        'conversation_id': conversation_id,
                        'error': 'API update failed'
                    })
                    logger.warning(f"Failed to migrate ticket {conversation_id}: API update failed")
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append({
                    'conversation_id': ticket.get('conversation_id', 'unknown'),
                    'error': str(e)
                })
                logger.error(f"Error processing ticket {ticket.get('conversation_id', 'unknown')}: {str(e)}")
        
        return results
    
    def migrate_all_tickets(self, batch_size: int = 50, max_tickets: Optional[int] = None) -> Dict[str, Any]:
        """Migrate all tickets with pagination
        
        Args:
            batch_size: Number of tickets to process per batch
            max_tickets: Maximum number of tickets to process (None for all)
            
        Returns:
            Dictionary with overall migration results
        """
        logger.info(f"Starting migration: batch_size={batch_size}, max_tickets={max_tickets}")
        
        overall_results = {
            'total_processed': 0,
            'total_successful': 0,
            'total_failed': 0,
            'batches_processed': 0,
            'all_errors': []
        }
        
        offset = 0
        batch_num = 1
        
        while True:
            logger.debug(f"Fetching batch {batch_num} with offset={offset}, batch_size={batch_size}")
            
            # Get batch of tickets
            tickets = self.get_tickets_with_pagination(batch_size, offset)
            
            if not tickets:
                logger.info("No more tickets to process")
                break
            
            # Check if we've reached the maximum
            if max_tickets and overall_results['total_processed'] >= max_tickets:
                logger.info(f"Reached maximum tickets limit: {max_tickets}")
                break
            
            # Process the batch
            batch_results = self.migrate_batch(tickets)
            
            # Update overall results
            overall_results['total_processed'] += batch_results['total']
            overall_results['total_successful'] += batch_results['successful']
            overall_results['total_failed'] += batch_results['failed']
            overall_results['batches_processed'] += 1
            overall_results['all_errors'].extend(batch_results['errors'])
            
            logger.debug(f"Overall progress: {overall_results['total_processed']} processed, {overall_results['total_successful']} successful, {overall_results['total_failed']} failed")
            
            # Move to next batch
            offset += batch_size
            batch_num += 1
            
            # Check if we've reached the maximum
            if max_tickets and overall_results['total_processed'] >= max_tickets:
                break
        
        logger.info(f"Migration completed: {overall_results['total_successful']} successful, {overall_results['total_failed']} failed")
        return overall_results
    
    def migrate_specific_tickets(self, conversation_ids: List[str]) -> Dict[str, Any]:
        """Migrate specific tickets by their conversation IDs
        
        Args:
            conversation_ids: List of conversation IDs to migrate
            
        Returns:
            Dictionary with migration results
        """
        logger.info(f"Starting migration for {len(conversation_ids)} specific tickets")
        logger.debug(f"Target conversation IDs: {conversation_ids}")
        
        # Get the specific tickets
        tickets = self.get_tickets_by_ids(conversation_ids)
        
        if not tickets:
            logger.warning("No tickets found for the provided IDs")
            return {
                'total_processed': 0,
                'total_successful': 0,
                'total_failed': 0,
                'errors': []
            }
        
        # Process the tickets
        results = self.migrate_batch(tickets)
        
        return {
            'total_processed': results['total'],
            'total_successful': results['successful'],
            'total_failed': results['failed'],
            'all_errors': results['errors']
        }

def main():
    """Main entry point for category migration"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate ticket categories')
    parser.add_argument('--batch-size', type=int, default=50, help='Number of tickets to process per batch (default: 50)')
    parser.add_argument('--max-tickets', type=int, help='Maximum number of tickets to process (default: all)')
    parser.add_argument('--ticket-ids', nargs='+', help='Specific conversation IDs to migrate (space-separated)')
    parser.add_argument('--log-level', default='INFO', help='Set the logging level')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without making API calls')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Category migration tool started")
    logger.debug(f"Command line arguments: {vars(args)}")
    
    try:
        # Initialize components
        logger.debug("Initializing configuration and components")
        config = Config.from_env()
        storage = DuckDBStorage(config.db_path)
        api = UnthreadAPI(config.api_key, config.base_url)
        
        logger.debug(f"Database path: {config.db_path}")
        logger.debug(f"API base URL: {config.base_url}")
        
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
                logger.info(f"  Total processed: {results['total_processed']} Successful: {results['total_successful']} Failed: {results['total_failed']}")
                
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
                logger.info("Starting full migration process")
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
    
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}", exc_info=True)
        return 1
    
    finally:
        if 'storage' in locals():
            logger.debug("Closing database connection")
            storage.close()
    
    logger.info("Category migration tool completed successfully")
    return 0

if __name__ == '__main__':
    exit(main())
