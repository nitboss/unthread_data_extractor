"""
Fix missing categories for conversations that resulted in empty migration categories.
This script addresses cases where the original migration didn't find category/subcategory data.
"""

import os
import re
import json
import logging
import argparse
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from .storage import DuckDBStorage
from .api import UnthreadAPI
from .config import Config
from .reclassify import get_system_prompt
from openai import OpenAI

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Field mappings
CATEGORY_FIELD_ID = "1a6900f6-36d2-4380-ad06-790b0b05c4b3"
SUB_CATEGORY_FIELD_ID = "05492140-551c-49ea-a8a2-4caeec8cda4d"
MIGRATION_CATEGORY_FIELD_ID = "0598cba1-31d1-466e-bfd1-812548c73c51"
RESOLUTION_FIELD_ID = "5ccb3d90-fbaf-4eea-ac88-ef3a82705ab2" 

class MissingCategoryFixer:
    """Handles fixing missing categories for conversations"""
    
    def __init__(self, storage: DuckDBStorage, api: UnthreadAPI):
        """Initialize the fixer
        
        Args:
            storage: DuckDB storage instance
            api: Unthread API instance
        """
        self.storage = storage
        self.api = api
        self.openai_client = None
        
        # Initialize OpenAI client if API key is available
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if openai_api_key:
            self.openai_client = OpenAI(api_key=openai_api_key)
        
        logger.debug("MissingCategoryFixer initialized")
    
    def extract_conversation_ids_from_log(self, log_file_path: str) -> List[str]:
        """Extract conversation IDs from migration log that resulted in empty categories
        
        Args:
            log_file_path: Path to the migration log file
            
        Returns:
            List of conversation IDs that had empty migration categories
        """
        conversation_ids = []
        
        try:
            with open(log_file_path, 'r') as f:
                for line in f:
                    # Look for lines with 'None' + 'None' -> '' pattern
                    if "'None' + 'None' -> ''" in line:
                        # Extract conversation ID using regex
                        match = re.search(r'Migrated ([a-f0-9-]+):', line)
                        if match:
                            conversation_id = match.group(1)
                            conversation_ids.append(conversation_id)
                            logger.debug(f"Found conversation ID: {conversation_id}")
            
            logger.info(f"Extracted {len(conversation_ids)} conversation IDs with missing categories")
            return conversation_ids
            
        except Exception as e:
            logger.error(f"Error extracting conversation IDs from log: {str(e)}")
            raise
    
    def query_bigquery_for_categories(self, conversation_ids: List[str], batch_size: int = 100) -> Dict[str, Dict[str, Any]]:
        """Query BigQuery for category information for given conversation IDs
        
        Args:
            conversation_ids: List of conversation IDs to query
            batch_size: Number of IDs to query in each batch
            
        Returns:
            Dictionary mapping conversation_id to category data
        """
        results = {}
        
        try:
            # Load BigQuery credentials from data/bq_connect.json
            credentials_path = os.getenv("BQ_CREDENTIALS_PATH")
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = bigquery.Client(credentials=credentials, project=credentials.project_id)
            
            # Process in batches to avoid SQL IN clause limits
            for i in range(0, len(conversation_ids), batch_size):
                batch_ids = conversation_ids[i:i + batch_size]
                id_list = "', '".join(batch_ids)
                
                query = f"""
                SELECT 
                    conversation_id,
                    ticket_category,
                    ticket_sub_category,
                    ticket_resolution
                FROM dbt.stg_unthread__conversations uc
                WHERE conversation_id IN ('{id_list}')
                ORDER BY conversation_id DESC
                """
                
                logger.debug(f"Querying BigQuery for batch {i//batch_size + 1} ({len(batch_ids)} IDs)")
                query_job = client.query(query)
                batch_results = query_job.result()
                
                for row in batch_results:
                    conversation_id = row.conversation_id
                    results[conversation_id] = {
                        'ticket_category': row.ticket_category,
                        'ticket_sub_category': row.ticket_sub_category,
                        'ticket_resolution': row.ticket_resolution
                    }
                    logger.debug(f"BigQuery result for {conversation_id}: {results[conversation_id]}")
            
            logger.debug(f"Retrieved category data for {len(results)} conversations from BigQuery")
            return results
            
        except Exception as e:
            logger.error(f"Error querying BigQuery: {str(e)}")
            raise
    
    def get_conversation_from_unthread(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Get conversation data from Unthread API
        
        Args:
            conversation_id: Conversation ID to fetch
            
        Returns:
            Conversation data or None if not found
        """
        try:
            endpoint = f"/conversations/{conversation_id}"
            response_data, _, _ = self.api.make_api_request(endpoint, method="GET")
            
            if response_data:
                logger.debug(f"Retrieved conversation {conversation_id} from Unthread API")
                return response_data
            else:
                logger.warning(f"Conversation {conversation_id} not found in Unthread API")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching conversation {conversation_id} from Unthread API: {str(e)}")
            return None
    
    def get_conversation_content_from_storage(self, conversation_id: str) -> Optional[str]:
        """Get conversation content from local storage for AI classification
        
        Args:
            conversation_id: Conversation ID to fetch
            
        Returns:
            Conversation content as string or None if not found
        """
        try:
            query = """
                SELECT data FROM conversations 
                WHERE id = ?
            """
            result = self.storage.conn.execute(query, [conversation_id]).fetchone()
            
            if result and result[0]:
                conversation_data = json.loads(result[0])
                messages = conversation_data.get('messages', [])
                
                # Extract message content
                content_parts = []
                for message in messages:
                    if message.get('content'):
                        content_parts.append(message['content'])
                
                return "\n<Next_Message>\n".join(content_parts)
            else:
                logger.warning(f"Conversation {conversation_id} not found in local storage")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching conversation {conversation_id} from storage: {str(e)}")
            return None
    
    def classify_conversation_with_ai(self, conversation_content: str) -> Optional[Dict[str, str]]:
        """Use AI to classify conversation content
        
        Args:
            conversation_content: Conversation content to classify
            
        Returns:
            Dictionary with category and sub_category or None if classification failed
        """
        if not self.openai_client:
            logger.warning("OpenAI client not available, skipping AI classification")
            return None
        
        try:
            system_prompt = get_system_prompt("category")
            
            user_prompt = f"Please classify this support case:\n\n<Conversation>{conversation_content}</Conversation>"
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
            )
            
            result_text = response.choices[0].message.content
            
            # Parse JSON response
            try:
                classification = json.loads(result_text)
                logger.debug(f"AI classification result: {classification}")
                return classification
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse AI classification response: {result_text}")
                return None
                
        except Exception as e:
            logger.error(f"Error in AI classification: {str(e)}")
            return None
    
    def create_migration_category(self, category: str, sub_category: str) -> str:
        """Create migration category string from category and sub_category
        
        Args:
            category: Main category
            sub_category: Sub category
            
        Returns:
            Migration category string
        """
        if not category or category == 'None':
            return ''
        
        if not sub_category or sub_category == 'None':
            return category
        
        return f"{category} - {sub_category}"
    
    def update_conversation_in_unthread(self, conversation_id: str, category_data: Dict[str, Any]) -> bool:
        """Update conversation in Unthread with category information
        
        Args:
            conversation_id: Conversation ID to update
            category_data: Dictionary containing category, sub_category, resolution, and migration_category
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            endpoint = f"/conversations/{conversation_id}"
            
            # Prepare update data
            update_data = {}
            
            # Add category fields if present
            if category_data.get('category'):
                update_data[CATEGORY_FIELD_ID] = category_data['category']
            
            if category_data.get('sub_category'):
                update_data[SUB_CATEGORY_FIELD_ID] = category_data['sub_category']
            
            if category_data.get('migration_category'):
                update_data[MIGRATION_CATEGORY_FIELD_ID] = category_data['migration_category']
            
            if category_data.get('resolution'):
                update_data[RESOLUTION_FIELD_ID] = category_data['resolution']
            
            if not update_data:
                logger.warning(f"No valid data to update for conversation {conversation_id}")
                return False
            
            # Make PATCH request
            response_data, _, _ = self.api.make_api_request(
                endpoint, 
                method="PATCH", 
                data=update_data
            )
            
            logger.info(f"Successfully updated conversation {conversation_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating conversation {conversation_id}: {str(e)}")
            return False
    
    def process_conversations(self, conversation_ids: List[str], batch_size: int = 100, limit: Optional[int] = None) -> Dict[str, Any]:
        """Process conversations to fix missing categories
        
        Args:
            conversation_ids: List of conversation IDs to process
            batch_size: Batch size for BigQuery queries
            limit: Maximum number of conversations to process (None for all)
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'total_conversations': len(conversation_ids),
            'bigquery_found': 0,
            'unthread_api_found': 0,
            'ai_classified': 0,
            'updated_successfully': 0,
            'failed': 0,
            'no_data_found': 0
        }
        
        # Apply limit if specified
        if limit and limit > 0:
            conversation_ids = conversation_ids[:limit]
            logger.info(f"Limited to processing {len(conversation_ids)} conversations (limit: {limit})")
        
        logger.info(f"Starting to process {len(conversation_ids)} conversations")
        
        # Step 1: Query BigQuery for category data
        bigquery_data = self.query_bigquery_for_categories(conversation_ids, batch_size)
        stats['bigquery_found'] = len(bigquery_data)
        
        # Step 2: Process each conversation
        for conversation_id in conversation_ids:
            category_data = {}
            data_source = "none"
            
            # Try BigQuery data first
            if conversation_id in bigquery_data:
                bq_data = bigquery_data[conversation_id]
                if bq_data.get('ticket_category') and bq_data['ticket_category'] != 'None':
                    category_data = {
                        'category': bq_data['ticket_category'],
                        'sub_category': bq_data['ticket_sub_category'],
                        'resolution': bq_data['ticket_resolution'],
                        'migration_category': self.create_migration_category(
                            bq_data['ticket_category'], 
                            bq_data['ticket_sub_category']
                        )
                    }
                    data_source = "bigquery"
            
            # Try Unthread API if no BigQuery data
            if not category_data:
                unthread_data = self.get_conversation_from_unthread(conversation_id)
                if unthread_data:
                    # Extract category fields from Unthread response
                    ticket_type_fields = unthread_data.get('ticketTypeFields', {})
                    category = ticket_type_fields.get(CATEGORY_FIELD_ID)
                    sub_category = ticket_type_fields.get(SUB_CATEGORY_FIELD_ID)
                    
                    if category and category != 'None':
                        category_data = {
                            'category': category,
                            'sub_category': sub_category,
                            'resolution': ticket_type_fields.get(RESOLUTION_FIELD_ID),
                            'migration_category': self.create_migration_category(category, sub_category)
                        }
                        data_source = "unthread_api"
                        stats['unthread_api_found'] += 1
            
            # Try AI classification if no data found
            if not category_data:
                conversation_content = self.get_conversation_content_from_storage(conversation_id)
                if conversation_content:
                    ai_classification = self.classify_conversation_with_ai(conversation_content)
                    if ai_classification:
                        category_data = {
                            'category': ai_classification.get('category'),
                            'sub_category': ai_classification.get('sub_category'),
                            'migration_category': self.create_migration_category(
                                ai_classification.get('category'),
                                ai_classification.get('sub_category')
                            )
                        }
                        data_source = "ai"
                        stats['ai_classified'] += 1
            
            # Update conversation if we have data
            if category_data:
                success = self.update_conversation_in_unthread(conversation_id, category_data)
                if success:
                    stats['updated_successfully'] += 1
                    logger.info(f"Processed {conversation_id}: updated using {data_source}")
                else:
                    stats['failed'] += 1
                    logger.error(f"Processed {conversation_id}: failed to update")
            else:
                stats['no_data_found'] += 1
                logger.warning(f"Processed {conversation_id}: no category data found")
        
        logger.info(f"Processing complete. Stats: {stats}")
        return stats

def main():
    """Main function to run the missing category fixer"""
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Fix missing categories for conversations')
    parser.add_argument('--conversation-id', '-c', 
                       help='Test with a specific conversation ID')
    parser.add_argument('--log-file', '-l', 
                       default='logs/migrate_categories.log',
                       help='Path to migration log file (default: logs/migrate_categories.log)')
    parser.add_argument('--limit', '-n', type=int,
                       help='Limit number of conversations to process')
    parser.add_argument('--batch-size', '-b', type=int, default=100,
                       help='Batch size for BigQuery queries (default: 100)')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Load configuration
    config = Config()
    
    # Initialize components
    storage = DuckDBStorage(config.database_path)
    api = UnthreadAPI(config.api_key, config.api_base_url)
    
    # Initialize fixer
    fixer = MissingCategoryFixer(storage, api)
    
    if args.conversation_id:
        # Test with specific conversation ID
        logger.info(f"Testing with conversation ID: {args.conversation_id}")
        conversation_ids = [args.conversation_id]
    else:
        # Extract conversation IDs from log
        conversation_ids = fixer.extract_conversation_ids_from_log(args.log_file)
        
        if not conversation_ids:
            logger.info("No conversations with missing categories found")
            return
    
    # Process conversations
    stats = fixer.process_conversations(
        conversation_ids, 
        batch_size=args.batch_size,
        limit=args.limit
    )
    
    # Log summary
    logger.info("="*50)
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

if __name__ == "__main__":
    main()
