"""
Collibra API client module

This module provides a client for interacting with the Collibra API.
"""

import os
import time
import logging
import requests
from dotenv import load_dotenv

from collibra_exporter.utils.auth import get_auth_header
from collibra_exporter.utils.common import PerformanceLogger

# Configure logger
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get base URL from environment
base_url = os.getenv('COLLIBRA_INSTANCE_URL')

class CollibraClient:
    """
    Client for interacting with the Collibra API.
    
    This class provides methods for making requests to the Collibra API
    with automatic token refresh and error handling.
    """
    
    def __init__(self):
        """Initialize the Collibra API client."""
        self.session = requests.Session()
        self.graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        
    def make_request(self, url, method='post', **kwargs):
        """
        Make a request to the Collibra API with automatic token refresh.
        
        Args:
            url (str): The URL to make the request to
            method (str): The HTTP method to use (default: 'post')
            **kwargs: Additional arguments to pass to the request
            
        Returns:
            requests.Response: The response from the API
            
        Raises:
            requests.RequestException: If the request fails
        """
        try:
            # Always get fresh headers before making a request
            headers = get_auth_header()
            if 'headers' in kwargs:
                kwargs['headers'].update(headers)
            else:
                kwargs['headers'] = headers

            response = getattr(self.session, method)(url=url, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            logger.error(f"Request failed: {str(error)}")
            raise
            
    def fetch_data(self, asset_type_id, paginate, limit, nested_offset=0, nested_limit=50):
        """
        Fetch asset data from Collibra using GraphQL.
        
        Args:
            asset_type_id (str): The asset type ID to query
            paginate (str): Pagination token (asset ID) or "null" for first page
            limit (int): Maximum number of assets to return
            nested_offset (int): Offset for nested fields
            nested_limit (int): Limit for nested fields
            
        Returns:
            dict: The response data or None if the request fails
        """
        from collibra_exporter.api.graphql import get_query
        
        try:
            query = get_query(asset_type_id, f'"{paginate}"' if paginate else 'null', nested_offset, nested_limit)
            variables = {'limit': limit}
            logger.debug(f"Sending GraphQL request for asset_type_id: {asset_type_id}, paginate: {paginate}, nested_offset: {nested_offset}")

            with PerformanceLogger("graphql_request"):
                response = self.make_request(
                    url=self.graphql_url,
                    json={
                        'query': query,
                        'variables': variables
                    }
                )
            
            data = response.json()
            
            if 'errors' in data:
                logger.error(f"GraphQL errors received: {data['errors']}")
                return None
                
            return data
        except Exception as error:
            logger.error(f'Error fetching data: {error}')
            return None
            
    def fetch_nested_data(self, asset_type_id, asset_id, field_name, nested_offset=0, nested_limit=20000):
        """
        Fetch nested data for a specific field of an asset.
        
        Args:
            asset_type_id (str): The asset type ID
            asset_id (str): The asset ID
            field_name (str): The name of the nested field to fetch
            nested_offset (int): Offset for pagination
            nested_limit (int): Limit for number of nested items per request
            
        Returns:
            list: The nested data items or None if the request fails
        """
        from collibra_exporter.api.graphql import get_nested_query
        
        try:
            query = get_nested_query(asset_type_id, asset_id, field_name, nested_offset, nested_limit)
            
            with PerformanceLogger(f"nested_graphql_request_{field_name}"):
                response = self.make_request(
                    url=self.graphql_url,
                    json={'query': query}
                )
            
            data = response.json()
            
            if 'errors' in data:
                logger.error(f"GraphQL errors in nested query: {data['errors']}")
                return None
                
            if not data['data']['assets']:
                logger.error(f"No asset found in nested query response")
                return None
                
            return data['data']['assets'][0][field_name]
        except Exception as e:
            logger.exception(f"Failed to fetch nested data for {field_name}: {str(e)}")
            return None
            
    def fetch_nested_data_with_pagination(self, asset_type_id, asset_id, field_name, batch_size=20000):
        """
        Fetch all nested data for a field using pagination.
        
        Args:
            asset_type_id (str): ID of the asset type
            asset_id (str): ID of the specific asset
            field_name (str): Name of the nested field to fetch
            batch_size (int): Number of items to fetch per request
        
        Returns:
            list: All nested items for the field or None if the request fails
        """
        all_items = []
        offset = 0
        batch_number = 1

        while True:
            logger.info(f"Fetching batch {batch_number} for {field_name} (offset: {offset})")
            
            current_items = self.fetch_nested_data(
                asset_type_id, 
                asset_id, 
                field_name, 
                offset, 
                batch_size
            )
            
            if not current_items:
                break
                
            current_batch_size = len(current_items)
            
            all_items.extend(current_items)
            logger.info(f"Retrieved {current_batch_size} items in batch {batch_number}")
            
            # If we got fewer items than the batch size, we've reached the end
            if current_batch_size < batch_size:
                break
                
            offset += batch_size
            batch_number += 1

        logger.info(f"Completed fetching {field_name}. Total items: {len(all_items)}")
        return all_items

# Create a singleton instance
client = CollibraClient()
