"""
Storage module for Unthread data
"""

import os
import json
import logging
from typing import List, Dict, Any
import duckdb

logger = logging.getLogger(__name__)

class DuckDBStorage:
    """DuckDB storage implementation"""
    
    def __init__(self, db_path: str):
        """Initialize storage
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        if db_path != ":memory:":
            self._ensure_data_dir()
        self.conn = duckdb.connect(db_path)
        self._create_tables()
        logger.info(f"Initialized DuckDB storage at {db_path}")
    
    def _ensure_data_dir(self):
        """Ensure data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        logger.debug(f"Ensured data directory exists at {os.path.dirname(self.db_path)}")
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        logger.debug("Creating database tables if they don't exist")
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR PRIMARY KEY,
                data JSON
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id VARCHAR PRIMARY KEY,
                data JSON
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id VARCHAR PRIMARY KEY,
                data JSON
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id VARCHAR PRIMARY KEY,
                data JSON,
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS conversation_classifications (
                conversation_id VARCHAR PRIMARY KEY,
                category VARCHAR,
                sub_category VARCHAR,
                reasoning TEXT,
                resolution VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS clio_clusters (
                example_id VARCHAR PRIMARY KEY,
                unthread_id VARCHAR,
                summary VARCHAR,
                cluster_name VARCHAR,
                category VARCHAR
            )
        """)
        
        # Ensure updated_time column exists (for existing databases)
        self._ensure_updated_time_column()
        
        logger.debug("Database tables created successfully")
    
    def _ensure_updated_time_column(self):
        """Ensure updated_time column exists in conversation_classifications table"""
        try:
            # Check if updated_time column exists
            columns = self.conn.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'conversation_classifications' 
                AND column_name = 'updated_time'
            """).fetchall()
            
            if not columns:
                logger.info("Adding updated_time column to conversation_classifications table")
                self.conn.execute("""
                    ALTER TABLE conversation_classifications 
                    ADD COLUMN updated_time TIMESTAMP
                """)
                logger.info("Successfully added updated_time column")
            else:
                logger.debug("updated_time column already exists")
                
        except Exception as e:
            logger.warning(f"Could not check/add updated_time column: {str(e)}")
    
    def store_users(self, users: List[Dict[str, Any]]):
        """Store users in the database
        
        Args:
            users: List of user data dictionaries
        """
        logger.info(f"Storing {len(users)} users in database")
        for user in users:
            self.conn.execute(
                "INSERT OR REPLACE INTO users (id, data) VALUES (?, ?)",
                [user["id"], json.dumps(user)]
            )
        logger.debug(f"Successfully stored {len(users)} users")
    
    def store_customers(self, customers: List[Dict[str, Any]]):
        """Store customers in the database
        
        Args:
            customers: List of customer data dictionaries
        """
        logger.info(f"Storing {len(customers)} customers in database")
        for customer in customers:
            self.conn.execute(
                "INSERT OR REPLACE INTO customers (id, data) VALUES (?, ?)",
                [customer["id"], json.dumps(customer)]
            )
        logger.debug(f"Successfully stored {len(customers)} customers")    

    def store_conversations(self, conversations: List[Dict[str, Any]]):
        """Store conversations in the database
        
        Args:
            conversations: List of conversation data dictionaries
        """
        logger.info(f"Storing {len(conversations)} conversations in database")
        for conversation in conversations:
            self.conn.execute(
                "INSERT OR REPLACE INTO conversations (id, data) VALUES (?, ?)",
                [conversation["id"], json.dumps(conversation)]
            )
        logger.debug(f"Successfully stored {len(conversations)} conversations")
    
    def store_messages(self, messages: List[Dict[str, Any]]):
        """Store messages in the database
        
        Args:
            messages: List of message data dictionaries
        """
        logger.info(f"Storing {len(messages)} messages in database")
        for message in messages:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO messages (id, data)
                VALUES (?, ?)
                """,
                [
                    message['id'],
                    json.dumps(message)
                ]
            )
        logger.debug(f"Successfully stored {len(messages)} messages")
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn'):
            logger.debug("Closing database connection")
            self.conn.close()
            logger.debug("Database connection closed")

    def save_classifications(self, conversations: List[Dict[str, Any]], results: List[Dict[str, Any]]):
        """
        Save classification results to the database using UPSERT, updating only non-null values.
        """
        classification_data = []
        for conversation, result in zip(conversations, results):
            if isinstance(result, dict) and 'error' not in result:
                classification_data.append((
                    conversation['conversation_id'],
                    result.get('category'),
                    result.get('sub_category'),
                    result.get('reasoning'),
                    result.get('resolution')
                ))
            else:
                classification_data.append((
                    conversation['conversation_id'],
                    'Error',
                    'Error',
                    str(result) if result else 'Unknown error',
                    'Error'
                ))
        for row in classification_data:
            self.conn.execute("""
                INSERT INTO conversation_classifications 
                    (conversation_id, category, sub_category, reasoning, resolution)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    category = CASE WHEN excluded.category IS NOT NULL THEN excluded.category ELSE conversation_classifications.category END,
                    sub_category = CASE WHEN excluded.sub_category IS NOT NULL THEN excluded.sub_category ELSE conversation_classifications.sub_category END,
                    reasoning = CASE WHEN excluded.reasoning IS NOT NULL THEN excluded.reasoning ELSE conversation_classifications.reasoning END,
                    resolution = CASE WHEN excluded.resolution IS NOT NULL THEN excluded.resolution ELSE conversation_classifications.resolution END
            """, row)
        logger.info(f"Saved {len(classification_data)} classifications to database (UPSERT)")

    def get_conversations(self) -> List[Dict[str, Any]]:
        """Get conversations from the database."""
        with open('data/extract_for_summary.sql', 'r') as f:
            query = f.read()
        results = self.conn.execute(query).fetchall()
        # Convert to list of dictionaries
        conversations = []
        for row in results:
            conv = {
                'conversation_id': row[0],
                'ticket_type': row[13],
                'message_content': row[20]
            }
            conversations.append(conv)
        return conversations 

    def get_classifications_for_update(self) -> List[Dict[str, Any]]:
        """Get classifications from database that need to be updated
        
        Returns:
            List of classification data dictionaries
        """
        query = """
            SELECT 
                cc.conversation_id,
                cc.category,
                cc.sub_category,
                cc.resolution,
                cl.cluster_name as cluster,
                cc.created_at
            FROM conversation_classifications cc
                LEFT JOIN clio_clusters cl ON cc.conversation_id = cl.unthread_id
            WHERE 1 = 1
                AND cc.category IS NOT NULL 
                AND cc.resolution IS NOT NULL
                AND (cc.updated_time IS NULL)
            ORDER BY created_at DESC
            LIMIT 100
        """
        try:
            results = self.conn.execute(query).fetchall()
            classifications = []
            for row in results:
                classification = {
                    'conversation_id': row[0],
                    'category': row[1],
                    'sub_category': row[2],
                    'resolution': row[3],
                    'cluster': row[4],
                    'created_at': row[5]
                }
                classifications.append(classification)
            return classifications
        except Exception as e:
            logger.error(f"Error fetching classifications: {str(e)}")
            raise

    def mark_conversation_updated(self, conversation_id: str):
        """Mark a conversation as successfully updated in the database
        
        Args:
            conversation_id: The conversation ID to mark as updated
        """
        try:
            update_query = """
                UPDATE conversation_classifications 
                SET updated_time = CURRENT_TIMESTAMP 
                WHERE conversation_id = ?
            """
            self.conn.execute(update_query, [conversation_id])
        except Exception as e:
            logger.error(f"Error marking conversation {conversation_id} as updated: {str(e)}") 