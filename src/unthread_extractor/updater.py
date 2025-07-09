"""
Data updater for Unthread conversations
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from unthread_extractor.api import UnthreadAPI
from unthread_extractor.storage import DuckDBStorage

logger = logging.getLogger(__name__)

class UnthreadUpdater:
    """Data updater for Unthread conversations"""
    
    def __init__(
        self,
        api: Optional[UnthreadAPI] = None,
        storage: Optional[DuckDBStorage] = None,
        db_path: Optional[str] = None,
        batch_size: int = 50
    ):
        """Initialize updater
        
        Args:
            api: Optional UnthreadAPI instance
            storage: Optional DuckDBStorage instance
            db_path: Optional path to database file
            batch_size: Number of conversations to update in each batch
        """
        # Get API key from environment
        api_key = os.environ.get("UNTHREAD_API_KEY")
        if not api_key:
            raise ValueError("UNTHREAD_API_KEY environment variable not set")
        
        # Initialize API client
        self.api = api or UnthreadAPI(api_key, "https://api.unthread.io/api")
        logger.debug("Initialized API client")
        
        # Initialize storage
        if storage:
            self.storage = storage
        elif db_path:
            self.storage = DuckDBStorage(db_path)
        else:
            self.storage = DuckDBStorage("data/unthread_data.duckdb")
        logger.debug("Initialized storage")
        
        # Set batch size
        self.batch_size = batch_size
        logger.debug(f"Batch size set to {batch_size}")
    
    def get_custom_field_id(self, field_name):
        if field_name == "category":
            return "1a6900f6-36d2-4380-ad06-790b0b05c4b3"
        elif field_name == "sub_category":
            return "05492140-551c-49ea-a8a2-4caeec8cda4d"
        elif field_name == "resolution":
            return "5ccb3d90-fbaf-4eea-ac88-ef3a82705ab2"
        elif field_name == "cluster":
            return "59f823e5-921d-4a4d-81bb-052fb2c8593a"
        else:
            return None
        
    def update_conversation(self, conversation_id: str, category: str, sub_category: str, resolution: str, cluster: str) -> bool:
        """Update a single conversation via API
        
        Args:
            conversation_id: The conversation ID to update
            category: The category to set
            resolution: The resolution to set
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare the update payload
            ticketTypeFields = {}
            ticketTypeFields[self.get_custom_field_id("category")] = category
            ticketTypeFields[self.get_custom_field_id("resolution")] = resolution
            ticketTypeFields[self.get_custom_field_id("sub_category")] = sub_category
            ticketTypeFields[self.get_custom_field_id("cluster")] = cluster
                
            update_data = {
                "ticketTypeFields": ticketTypeFields
            }
            
            logger.debug(f"Updating conversation {conversation_id} with category: {category}, resolution: {resolution}, sub_category: {sub_category}, cluster: {cluster}")
            
            # Make the PATCH request
            response_data, _, _ = self.api.make_api_request(
                endpoint=f"/conversations/{conversation_id}",
                method="PATCH",
                data=update_data
            )
            
            logger.debug(f"Successfully updated conversation {conversation_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update conversation {conversation_id}: {str(e)}")
            return False
    
    def update_conversations_batch(self, batch: List[Dict[str, Any]]) -> Dict[str, int]:
        """Update a batch of conversations
        
        Args:
            batch: List of classification dictionaries to update
            
        Returns:
            Dictionary with success and failure counts
        """
        success_count = 0
        failure_count = 0
        
        logger.debug(f"Processing batch of {len(batch)} conversations")
        
        for classification in batch:
            conversation_id = classification['conversation_id']
            category = classification['category']
            resolution = classification['resolution']
            sub_category = classification['sub_category']   
            cluster = classification['cluster']
            
            try:
                success = self.update_conversation(conversation_id, category, sub_category, resolution, cluster)
                
                if success:
                    self.storage.mark_conversation_updated(conversation_id)
                    success_count += 1
                    logger.debug(f"✓ Successfully updated conversation {conversation_id}")
                else:
                    failure_count += 1
                    logger.warning(f"✗ Failed to update conversation {conversation_id}")
                    
            except Exception as e:
                failure_count += 1
                logger.error(f"✗ Exception updating conversation {conversation_id}: {str(e)}")
        
        logger.info(f"Batch completed: {success_count} successful, {failure_count} failed")
        return {
            'success': success_count,
            'failure': failure_count
        }
    
    def update_all_conversations(self) -> Dict[str, int]:
        """Update all conversations that need updating
        
        Returns:
            Dictionary with total success and failure counts
        """
        logger.debug("Starting conversation update process")
        
        # Get all classifications that need updating
        classifications = self.storage.get_classifications_for_update()
        
        if not classifications:
            logger.info("No conversations need updating")
            return {'success': 0, 'failure': 0}
        
        total_success = 0
        total_failure = 0
        batch_count = 0
        
        # Process in batches
        for i in range(0, len(classifications), self.batch_size):
            batch_count += 1
            batch = classifications[i:i + self.batch_size]
            
            logger.info(f"Processing batch {batch_count} ({len(batch)} conversations)")
            
            batch_results = self.update_conversations_batch(batch)
            total_success += batch_results['success']
            total_failure += batch_results['failure']
            
            logger.debug(f"Batch {batch_count} completed. Progress: {total_success + total_failure}/{len(classifications)} conversations processed")
        
        logger.debug(f"Update process completed. Total: {total_success} successful, {total_failure} failed")
        return {
            'success': total_success,
            'failure': total_failure,
            'total_processed': total_success + total_failure
        }
    
    def close(self):
        """Close the storage connection"""
        if hasattr(self, 'storage'):
            self.storage.close()
            logger.debug("Storage connection closed") 