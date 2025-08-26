import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from src.unthread_extractor.cli import parse_date, setup_logging, main

def test_parse_date_valid():
    """Test parsing valid date string"""
    date_str = "2024-03-20"
    expected = datetime(2024, 3, 20).isoformat()
    assert parse_date(date_str) == expected

def test_parse_date_invalid():
    """Test parsing invalid date string"""
    with pytest.raises(ValueError):
        parse_date("invalid-date")

@pytest.fixture
def mock_config():
    """Mock configuration fixture"""
    with patch('src.unthread_extractor.cli.Config') as mock:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "test.db"
        mock.from_env.return_value = config
        yield config

@pytest.fixture
def mock_extractor():
    """Mock extractor fixture"""
    with patch('src.unthread_extractor.cli.UnthreadExtractor') as mock:
        extractor = MagicMock()
        mock.return_value = extractor
        yield extractor

def test_cli_users_command(mock_extractor):
    """Test users command"""
    with patch('src.unthread_extractor.cli.Config.from_env') as mock_from_env:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "data/test.db"
        mock_from_env.return_value = config
        with patch('sys.argv', ['script', 'users']):
            main()
            mock_extractor.download_users.assert_called_once()

def test_cli_customers_command(mock_extractor):
    """Test customers command"""
    with patch('src.unthread_extractor.cli.Config.from_env') as mock_from_env:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "data/test.db"
        mock_from_env.return_value = config
        with patch('sys.argv', ['script', 'customers']):
            main()
            mock_extractor.download_customers.assert_called_once()

def test_cli_conversations_command(mock_extractor):
    """Test conversations command with date filters"""
    with patch('src.unthread_extractor.cli.Config.from_env') as mock_from_env:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "data/test.db"
        mock_from_env.return_value = config
        with patch('sys.argv', [
            'script', 'conversations',
            '--start-date', '2024-03-01',
            '--end-date', '2024-03-20'
        ]):
            main()
            mock_extractor.download_conversations.assert_called_once_with(
                modified_after=datetime(2024, 3, 1).isoformat(),
                modified_before=datetime(2024, 3, 20).isoformat(),
                conversation_id=None
            )

def test_cli_messages_command(mock_extractor):
    """Test messages command"""
    with patch('src.unthread_extractor.cli.Config.from_env') as mock_from_env:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "data/test.db"
        mock_from_env.return_value = config
        with patch('sys.argv', [
            'script', 'messages',
            '--conversation-id', 'test-conv-id'
        ]):
            main()
            mock_extractor.download_messages.assert_called_once_with('test-conv-id')

def test_cli_all_command(mock_extractor):
    """Test all command"""
    with patch('src.unthread_extractor.cli.Config.from_env') as mock_from_env:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "data/test.db"
        mock_from_env.return_value = config
        with patch('sys.argv', ['script', 'all']):
            main()
            mock_extractor.download_users.assert_called_once()
            mock_extractor.download_conversations.assert_called_once()

def test_cli_no_command(mock_extractor):
    """Test behavior when no command is provided"""
    with patch('src.unthread_extractor.cli.Config.from_env') as mock_from_env:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "data/test.db"
        mock_from_env.return_value = config
        with patch('sys.argv', ['script']), \
             patch('sys.exit') as mock_exit:
            main()
            mock_exit.assert_called_once_with(1)

def test_cli_error_handling(mock_extractor):
    """Test error handling in CLI"""
    with patch('src.unthread_extractor.cli.Config.from_env') as mock_from_env:
        config = MagicMock()
        config.api_key = "test-api-key"
        config.base_url = "https://api.test.com"
        config.db_path = "data/test.db"
        mock_from_env.return_value = config
        mock_extractor.download_users.side_effect = Exception("Test error")
        with patch('sys.argv', ['script', 'users']), \
             patch('sys.exit') as mock_exit:
            main()
            mock_exit.assert_called_once_with(1) 