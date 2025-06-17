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
        logger.debug("Database tables created successfully")
    
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
            logger.info("Database connection closed") 