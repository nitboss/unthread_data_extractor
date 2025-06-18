import pytest
import os
from unittest.mock import MagicMock

@pytest.fixture(autouse=True)
def mock_logging():
    """Mock logging to prevent actual log output during tests"""
    with pytest.MonkeyPatch.context() as m:
        m.setattr("logging.getLogger", MagicMock())
        yield

@pytest.fixture(autouse=True)
def clean_test_env():
    """Clean up test environment before and after tests"""
    # Clean up any test files that might have been left behind
    test_files = [
        "test.db",
        "test_data/test.db"
    ]
    
    for file_path in test_files:
        if os.path.exists(file_path):
            os.remove(file_path)
    
    if os.path.exists("test_data"):
        os.rmdir("test_data")
    
    yield
    
    # Clean up after tests
    for file_path in test_files:
        if os.path.exists(file_path):
            os.remove(file_path)
    
    if os.path.exists("test_data"):
        os.rmdir("test_data") 