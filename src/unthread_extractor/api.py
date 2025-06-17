"""
API client for Unthread
"""

import requests
import logging
from typing import Optional, Dict, Any, Tuple, List

logger = logging.getLogger(__name__)

class UnthreadAPI:
    """API client for Unthread"""
    
    def __init__(self, api_key: str, base_url: str):
        """Initialize API client"""
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "X-Api-Key": f"{api_key}",
            "Content-Type": "application/json"
        }
        logger.debug(f"Initialized UnthreadAPI with base_url: {base_url}")
    
    def make_api_request(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        cursor: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
        """Make an API request with cursor-based pagination support
        
        Args:
            endpoint: API endpoint to call
            method: HTTP method (GET, POST, etc.)
            data: Request body data
            params: URL query parameters
            max_retries: Maximum number of retry attempts
            cursor: Pagination cursor
            
        Returns:
            Tuple containing:
            - List of data items
            - Next cursor (if any)
            - Whether there are more items to fetch
        """
        url = f"{self.base_url}{endpoint}"
        logger.debug(f"Making {method} request to {url}")
        if data:
            logger.debug(f"Request data: {data}")
        
        # Add cursor to request data if provided
        if cursor and data is not None:
            data["cursor"] = cursor
            logger.debug(f"Using cursor: {cursor}")
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Request attempt {attempt + 1}/{max_retries}")
                if method == "GET":
                    response = requests.get(
                        url=url,
                        headers=self.headers,
                        params=params
                    )
                elif method == "PATCH":
                    response = requests.patch(
                        url=url,
                        headers=self.headers,
                        json=data,
                        params=params
                    )
                elif method == "POST":
                    response = requests.post(
                        url=url,
                        headers=self.headers,
                        json=data,
                        params=params
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Log response details for debugging
                logger.debug(f"Response status code: {response.status_code}")
                logger.debug(f"Response headers: {dict(response.headers)}")
                                
                response.raise_for_status()

                if method == "GET" or method == "PATCH":
                    return response.json(), None, False

                try:
                    response_data = response.json()
                    logger.debug(f"Response data: {response_data}")
                except ValueError as e:
                    logger.error(f"Failed to parse JSON response: {response.text}")
                    raise Exception(f"Invalid JSON response: {str(e)}")
                
                # Extract data and pagination info
                items = response_data.get("data", [])
                response_metadata = response_data.get('cursors', {})
                next_cursor = response_metadata.get('next')
                has_next = response_metadata.get('hasNext', False)
                
                logger.debug(f"Received {len(items)} items, has_next: {has_next}")
                return items, next_cursor, has_next
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    error_msg = f"API request failed after {max_retries} attempts: {str(e)}"
                    if hasattr(e.response, 'text'):
                        error_msg += f"\nResponse: {e.response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                logger.warning(f"API request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                continue 