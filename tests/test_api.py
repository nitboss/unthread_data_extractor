import pytest
from unittest.mock import patch, MagicMock
import requests
from src.unthread_extractor.api import UnthreadAPI

@pytest.fixture
def api_client():
    """Fixture for API client"""
    return UnthreadAPI(api_key="test-key", base_url="https://api.test.com")

@pytest.fixture
def mock_response():
    """Fixture for mock response"""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "data": [
            {"id": "1", "name": "Test Item 1"},
            {"id": "2", "name": "Test Item 2"}
        ],
        "cursors": {
            "next": "next-cursor",
            "hasNext": True
        }
    }
    return response

def test_api_initialization(api_client):
    """Test API client initialization"""
    assert api_client.api_key == "test-key"
    assert api_client.base_url == "https://api.test.com"
    assert api_client.headers["X-Api-Key"] == "test-key"
    assert api_client.headers["Content-Type"] == "application/json"

@patch('requests.get')
def test_make_api_request_get(mock_get, api_client, mock_response):
    """Test GET request"""
    mock_get.return_value = mock_response
    
    items, next_cursor, has_next = api_client.make_api_request(
        endpoint="/test",
        method="GET"
    )
    
    # Verify request
    mock_get.assert_called_once_with(
        url="https://api.test.com/test",
        headers=api_client.headers,
        params=None
    )
    
    # Verify response handling
    assert len(items) == 2
    assert items[0]["id"] == "1"
    assert items[1]["id"] == "2"
    assert next_cursor == "next-cursor"
    assert has_next is True

@patch('requests.post')
def test_make_api_request_post(mock_post, api_client, mock_response):
    """Test POST request"""
    mock_post.return_value = mock_response
    
    items, next_cursor, has_next = api_client.make_api_request(
        endpoint="/test",
        method="POST",
        data={"test": "data"}
    )
    
    # Verify request
    mock_post.assert_called_once_with(
        url="https://api.test.com/test",
        headers=api_client.headers,
        json={"test": "data"},
        params=None
    )
    
    # Verify response handling
    assert len(items) == 2
    assert next_cursor == "next-cursor"
    assert has_next is True

@patch('requests.patch')
def test_make_api_request_patch(mock_patch, api_client, mock_response):
    """Test PATCH request"""
    mock_patch.return_value = mock_response
    
    items, next_cursor, has_next = api_client.make_api_request(
        endpoint="/test",
        method="PATCH",
        data={"test": "data"}
    )
    
    # Verify request
    mock_patch.assert_called_once_with(
        url="https://api.test.com/test",
        headers=api_client.headers,
        json={"test": "data"},
        params=None
    )
    
    # Verify response handling
    assert len(items) == 2
    assert next_cursor is None
    assert has_next is False

def test_make_api_request_invalid_method(api_client):
    """Test request with invalid HTTP method"""
    with pytest.raises(ValueError):
        api_client.make_api_request(
            endpoint="/test",
            method="INVALID"
        )

@patch('requests.get')
def test_make_api_request_retry(mock_get, api_client):
    """Test request retry behavior"""
    # First two attempts fail, third succeeds
    mock_get.side_effect = [
        requests.exceptions.RequestException("First failure"),
        requests.exceptions.RequestException("Second failure"),
        MagicMock(
            status_code=200,
            json=lambda: {
                "data": [{"id": "1"}],
                "cursors": {"hasNext": False}
            }
        )
    ]
    
    items, next_cursor, has_next = api_client.make_api_request(
        endpoint="/test",
        method="GET",
        max_retries=3
    )
    
    assert mock_get.call_count == 3
    assert len(items) == 1
    assert next_cursor is None
    assert has_next is False

@patch('requests.get')
def test_make_api_request_max_retries_exceeded(mock_get, api_client):
    """Test behavior when max retries are exceeded"""
    mock_get.side_effect = requests.exceptions.RequestException("API Error")
    
    with pytest.raises(Exception) as exc_info:
        api_client.make_api_request(
            endpoint="/test",
            method="GET",
            max_retries=3
        )
    
    assert "API request failed after 3 attempts" in str(exc_info.value)
    assert mock_get.call_count == 3

@patch('requests.get')
def test_make_api_request_with_cursor(mock_get, api_client, mock_response):
    """Test request with cursor-based pagination"""
    mock_get.return_value = mock_response
    
    items, next_cursor, has_next = api_client.make_api_request(
        endpoint="/test",
        method="GET",
        data={"cursor": "test-cursor"}
    )
    
    # Verify request
    mock_get.assert_called_once_with(
        url="https://api.test.com/test",
        headers=api_client.headers,
        params=None
    )
    
    # Verify response handling
    assert len(items) == 2
    assert next_cursor == "next-cursor"
    assert has_next is True 