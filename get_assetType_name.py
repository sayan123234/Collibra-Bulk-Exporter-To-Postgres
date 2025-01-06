import os
import logging
import requests
from dotenv import load_dotenv
from OauthAuth import get_auth_header

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

session = requests.Session()
#session.verify = os.getenv('SSL_CERT')

def get_asset_type_name(asset_type_id):
    base_url = os.getenv('COLLIBRA_INSTANCE_URL')
    url = f"https://{base_url}/rest/2.0/assetTypes/{asset_type_id}"

    try:
        session.headers.update(get_auth_header())
        response = session.get(url)
        response.raise_for_status()
        json_response = response.json()
        return json_response["name"]
    except requests.RequestException as e:
        logging.error(f"Asset type not found in Collibra: {e}")
        return None