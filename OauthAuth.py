import requests
import os
import logging
import time
from dotenv import load_dotenv
from functools import lru_cache

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

session = requests.Session()

class OAuthTokenManager:
    def __init__(self):
        self._token = None
        self._expiration_time = 0
        # Add buffer time (30 seconds) to refresh before actual expiration
        self._refresh_buffer = 30

    def get_valid_token(self):
        """Get a valid OAuth token, refreshing if necessary."""
        current_time = time.time()
        
        # Check if token is expired or will expire soon
        if not self._token or current_time >= (self._expiration_time - self._refresh_buffer):
            self._fetch_new_token()
            
        return self._token

    def _fetch_new_token(self):
        """Fetch a new OAuth token from the server."""
        client_id = os.getenv('CLIENT_ID')
        client_secret = os.getenv('CLIENT_SECRET')
        base_url = os.getenv('COLLIBRA_INSTANCE_URL')
        
        url = f"https://{base_url}/rest/oauth/v2/token"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        payload = f'client_id={client_id}&grant_type=client_credentials&client_secret={client_secret}'
        
        try:
            response = session.post(url=url, data=payload, headers=headers)
            response.raise_for_status()
            token_data = response.json()
            
            self._token = token_data["access_token"]
            # Set expiration time based on server response
            self._expiration_time = time.time() + token_data["expires_in"]
            
            logging.info("Successfully obtained new OAuth token")
            return self._token
            
        except requests.RequestException as e:
            logging.error(f"Error obtaining OAuth token: {e}")
            raise

# Create a singleton instance
token_manager = OAuthTokenManager()

def get_oauth_token():
    """Get a valid OAuth token."""
    return token_manager.get_valid_token()

def get_auth_header():
    """Get the authorization header with a valid token."""
    return {'Authorization': f'Bearer {get_oauth_token()}'}