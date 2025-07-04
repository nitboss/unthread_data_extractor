"""
Data extractor for Unthread
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from unthread_extractor.api import UnthreadAPI
from unthread_extractor.storage import DuckDBStorage

logger = logging.getLogger(__name__)

class UnthreadExtractor:
    """Data extractor for Unthread"""
    
    def __init__(
        self,
        api: Optional[UnthreadAPI] = None,
        storage: Optional[DuckDBStorage] = None,
        db_path: Optional[str] = None
    ):
        """Initialize extractor
        
        Args:
            api: Optional UnthreadAPI instance
            storage: Optional DuckDBStorage instance
            db_path: Optional path to database file
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
    
    def download_users(self) -> List[Dict[str, Any]]:
        """Download users from the API
        
        Returns:
            List of user data dictionaries
        """
        logger.debug("[Extract] Downloading users...")
        all_users = []
        cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            logger.debug(f"Downloading users page {page_count}")
            
            data = {
                "limit": 200
            }
            
            try:
                users, cursor, has_next = self.api.make_api_request(
                    endpoint="/users/list",
                    method="POST",
                    data=data,
                    cursor=cursor
                )
                
                if not users:
                    logger.debug("No more users to download")
                    break
                
                all_users.extend(users)
                self.storage.store_users(users)
                logger.debug(f"Downloaded and stored {len(users)} users")
                
                if not cursor or not has_next:
                    logger.debug("Reached end of user pagination")
                    break
                    
            except Exception as e:
                logger.error(f"Error downloading users page {page_count}: {str(e)}")
                raise
        
        logger.info(f"Successfully downloaded {len(all_users)} users")
        return all_users

    def download_customers(self) -> List[Dict[str, Any]]:
        """Download customers from the API
        
        Returns:
            List of customer data dictionaries
        """
        logger.debug("[Extract] Downloading customers...")
        all_customers = []
        cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            logger.debug(f"Downloading customers page {page_count}")
            
            data = {
                "limit": 200
            }
            
            try:
                customers, cursor, has_next = self.api.make_api_request(
                    endpoint="/customers/list",
                    method="POST",
                    data=data,
                    cursor=cursor
                )
                
                if not customers:
                    logger.debug("No more customers to download")
                    break
                
                all_customers.extend(customers)
                self.storage.store_customers(customers)
                logger.debug(f"Downloaded and stored {len(customers)} customers")
                
                if not cursor or not has_next:
                    logger.debug("Reached end of customer pagination")
                    break
                    
            except Exception as e:
                logger.error(f"Error downloading users page {page_count}: {str(e)}")
                raise
        
        logger.info(f"Successfully downloaded {len(all_customers)} customers")
        return all_customers
    
    def download_conversations(
        self,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        conversation_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Download conversations from the API
        
        Args:
            created_after: Filter conversations created after this date
            created_before: Filter conversations created before this date
            conversation_id: Download a specific conversation
            
        Returns:
            List of conversation data dictionaries
        """
        logger.debug("[Extract] Downloading conversations...")
        all_convs = []
        cursor = None
        page_count = 0
        date_str_after = created_after.split('T')[0] if created_after else None
        date_str_before = created_before.split('T')[0] if created_before else None
        
        while True:
            page_count += 1
            logger.debug(f"Downloading conversations page {page_count}")
            
            data = {
                "order": ["createdAt", "id"],
                "descending": True
            }
            
            if conversation_id:
                data["where"] = [{"field": "id", "operator": "==", "value": conversation_id}]
                logger.debug(f"Filtering for conversation ID: {conversation_id}")
            elif date_str_after or date_str_before:
                data["where"] = []
                if date_str_after:
                    data["where"].append({"field": "createdAt", "operator": ">=", "value": date_str_after})
                if date_str_before:
                    data["where"].append({"field": "createdAt", "operator": "<=", "value": date_str_before})
                logger.debug(f"Filtering conversations between {date_str_after} and {date_str_before}")
            
            try:
                conversations, cursor, has_next = self.api.make_api_request(
                    endpoint="/conversations/list",
                    method="POST",
                    data=data,
                    cursor=cursor
                )
                
                if not conversations:
                    logger.debug("No more conversations to download")
                    break
                
                for conversation in conversations:
                    try:
                        conversation = self.download_conversation(conversation["id"])
                        self.download_messages(conversation["id"])
                        all_convs.append(conversation)
                    except Exception as e:
                        logger.error(f"Error processing conversation {conversation['id']}: {str(e)}")
                        continue
                
                if not cursor or not has_next:
                    logger.debug("Reached end of conversation pagination")
                    break
                    
            except Exception as e:
                logger.error(f"Error downloading conversations page {page_count}: {str(e)}")
                raise
        
        logger.info(f"Successfully downloaded {len(all_convs)} conversations")
        return all_convs
    
    def download_conversation(self, conversation_id: str) -> Dict[str, Any]:
        """Download a single conversation
        
        Args:
            conversation_id: ID of the conversation to download
            
        Returns:
            Conversation data dictionary
        """
        logger.debug(f"[Extract] Downloading conversation {conversation_id}...")
        try:
            conversation, _, _ = self.api.make_api_request(
                endpoint=f"/conversations/{conversation_id}",
                method="GET"
            )
            if not conversation:
                raise ValueError(f"No data returned for conversation {conversation_id}")
            self.storage.store_conversations([conversation])
            logger.debug(f"Successfully downloaded and stored conversation {conversation_id}")
            return conversation
        except Exception as e:
            logger.error(f"Error downloading conversation {conversation_id}: {str(e)}")
            raise
    
    def download_messages(self, conversation_id: str) -> List[Dict[str, Any]]:
        """Download messages for a conversation
        
        Args:
            conversation_id: ID of the conversation to download messages for
            
        Returns:
            List of message data dictionaries
        """
        logger.debug(f"[Extract] Downloading messages for conversation {conversation_id}...")
        all_messages = []
        cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            logger.debug(f"Downloading messages page {page_count} for conversation {conversation_id}")
            
            try:
                messages, cursor, has_next = self.api.make_api_request(
                    endpoint=f"/conversations/{conversation_id}/messages/list",
                    method="POST",
                    data={},
                    cursor=cursor
                )
                
                if not messages:
                    logger.debug(f"No more messages to download for conversation {conversation_id}")
                    break
                
                all_messages.extend(messages)
                self.storage.store_messages(messages)
                logger.debug(f"Downloaded and stored {len(messages)} messages")
                                    
            except Exception as e:
                logger.error(f"Error downloading messages page {page_count} for conversation {conversation_id}: {str(e)}")
                raise
        
        logger.info(f"Successfully downloaded {len(all_messages)} messages for conversation {conversation_id}")
        return all_messages 