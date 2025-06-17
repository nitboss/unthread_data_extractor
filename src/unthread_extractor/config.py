"""
Configuration settings for the Unthread Data Extractor
"""

import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Config:
    """Configuration settings for the Unthread Data Extractor"""
    api_key: str
    base_url: str = "https://api.unthread.io/api"
    db_path: str = "data/unthread_data.duckdb"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> 'Config':
        """Create a Config instance from environment variables"""
        api_key = os.getenv('UNTHREAD_API_KEY')
        if not api_key:
            raise ValueError("UNTHREAD_API_KEY environment variable not set")
        
        return cls(
            api_key=api_key,
            base_url=os.getenv('UNTHREAD_API_URL', cls.base_url),
            db_path=os.getenv('UNTHREAD_DB_PATH', cls.db_path),
            log_level=os.getenv('UNTHREAD_LOG_LEVEL', cls.log_level)
        ) 