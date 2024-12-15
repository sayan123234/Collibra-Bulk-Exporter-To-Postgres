#Optional script to get all asset types in your collibra Instance
import os
import logging
import requests
from functools import lru_cache
from dotenv import load_dotenv
from OauthAuth import oauth_bearer_token

load_dotenv()

logger = logging.getLogger(__name__)

session = requests.Session()

@lru_cache(maxsize=1)
def get_available_asset_type():
    base_url = os.getenv('COLLIBRA_INSTANCE_URL')
    url = f"https://{base_url}/rest/2.0/assetTypes"

    try:
        token = oauth_bearer_token()
        if not token:
            logger.error("Failed to obtain OAuth token")
            return None

        session.headers.update({'Authorization': f'Bearer {token}'})
        response = session.get(url)
        response.raise_for_status()
        original_results = response.json()["results"]
        modified_results = [{"id": asset["id"], "name": asset["name"]} for asset in original_results]
        
        logger.info(f"Successfully retrieved {len(modified_results)} asset types")
        return {"results": modified_results}
    except requests.RequestException as e:
        logger.error(f"Failed to retrieve asset types: {e}")
        return None