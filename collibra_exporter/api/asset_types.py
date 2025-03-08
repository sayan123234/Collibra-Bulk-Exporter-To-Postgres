"""
Asset type module for Collibra API

This module provides functions to interact with Collibra asset types.
"""

import os
import logging
import requests
from functools import lru_cache
from dotenv import load_dotenv

from collibra_exporter.utils.auth import get_auth_header

# Configure logger
logger = logging.getLogger(__name__)

# Create a session for reuse
session = requests.Session()

@lru_cache(maxsize=1)
def get_available_asset_types():
    """
    Get all available asset types from Collibra.
    
    Returns:
        dict: Dictionary containing asset types with their IDs and names
        
    Raises:
        requests.RequestException: If the API request fails
    """
    base_url = os.getenv('COLLIBRA_INSTANCE_URL')
    url = f"https://{base_url}/rest/2.0/assetTypes"

    try:
        session.headers.update(get_auth_header())
        response = session.get(url)
        response.raise_for_status()
        original_results = response.json()["results"]
        modified_results = [{"id": asset["id"], "name": asset["name"]} for asset in original_results]
        
        logger.info(f"Successfully retrieved {len(modified_results)} asset types")
        return {"results": modified_results}
    except requests.RequestException as e:
        logger.error(f"Failed to retrieve asset types: {e}")
        raise

def get_asset_type_name(asset_type_id):
    """
    Get the name of an asset type by its ID.
    
    Args:
        asset_type_id (str): The ID of the asset type
        
    Returns:
        str: The name of the asset type or None if not found
        
    Raises:
        requests.RequestException: If the API request fails
    """
    base_url = os.getenv('COLLIBRA_INSTANCE_URL')
    url = f"https://{base_url}/rest/2.0/assetTypes/{asset_type_id}"

    try:
        session.headers.update(get_auth_header())
        response = session.get(url)
        response.raise_for_status()
        json_response = response.json()
        return json_response["name"]
    except requests.RequestException as e:
        logger.error(f"Asset type not found in Collibra: {e}")
        raise
