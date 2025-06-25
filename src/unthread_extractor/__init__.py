"""
Unthread Data Extractor - A tool to extract data from Unthread API and store it in DuckDB
"""

__version__ = "0.1.0"

from .api import UnthreadAPI
from .storage import DuckDBStorage
from .extractor import UnthreadExtractor
from .config import Config
from .updater import UnthreadUpdater

__all__ = ['UnthreadAPI', 'DuckDBStorage', 'UnthreadExtractor', 'Config', 'UnthreadUpdater'] 