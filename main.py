import os
import json
import time
import logging
import sys
import codecs
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, DateTime, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError

from graphql_queries import QUERY_TYPES
from get_assetType_name import get_asset_type_name
from OauthAuth import oauth_bearer_token

def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    os.makedirs('logs', exist_ok=True)
    
    # Ensure UTF-8 encoding for all log files
    file_handlers = [
        logging.FileHandler(f'logs/app_{time.strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.FileHandler('logs/latest.log', encoding='utf-8', mode='w')
    ]
    
    # Use a custom StreamHandler with UTF-8 encoding
    stream_handler = logging.StreamHandler(codecs.getwriter('utf-8')(sys.stdout.buffer, 'replace'))
    
    handlers = file_handlers + [stream_handler]
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )
    
    # Add debug log with UTF-8 encoding
    debug_handler = logging.FileHandler('logs/debug.log', encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(logging.Formatter(log_format, date_format))
    logging.getLogger().addHandler(debug_handler)

setup_logging()
load_dotenv()

if sys.platform == 'win32':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')
    os.system('chcp 65001')

base_url = os.getenv('COLLIBRA_INSTANCE_URL')
database_url = os.getenv('DATABASE_URL')

engine = create_engine(database_url)
SessionLocal = sessionmaker(bind=engine)

with open('Collibra_Asset_Type_Id_Manager.json', 'r', encoding='utf-8') as file:
    ASSET_TYPE_IDS = json.load(file)['ids']

session = requests.Session()
session.headers.update({'Authorization': f'Bearer {oauth_bearer_token()}'})

QUERY_LIMITS = {
    'main': 10000,
    'string': 490,
    'multi': 490,
    'numeric': 490,
    'boolean': 490,
    'outgoing': 490,
    'incoming': 490,
    'responsibilities': 490
}

class PerformanceLogger:
    def __init__(self, operation_name):
        self.operation_name = operation_name
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        logging.debug(f"Starting {self.operation_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        if exc_type:
            logging.error(f"{self.operation_name} failed after {duration:.2f} seconds")
        else:
            logging.debug(f"{self.operation_name} completed in {duration:.2f} seconds")

def fetch_data_for_query_type(asset_type_id, query_type, paginate):
    try:
        query_func = QUERY_TYPES[query_type]
        limit = QUERY_LIMITS[query_type]
        
        paginate_value = f'"{paginate}"' if paginate else 'null'
        query = query_func(asset_type_id, paginate_value, limit)
        
        variables = {'limit': limit}
        
        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        response = session.post(
            url=graphql_url,
            json={'query': query, 'variables': variables}
        )
        response.raise_for_status()
        
        data = response.json()
        if 'errors' in data:
            logging.error(f"GraphQL errors for {query_type}: {data['errors']}")
            return None
            
        return data
    except Exception as error:
        logging.exception(f"Failed fetching {query_type} data")
        return None

def fetch_all_data_for_query_type(asset_type_id, query_type):
    all_results = []
    paginate = None
    batch_count = 0

    while True:
        batch_count += 1
        response = fetch_data_for_query_type(asset_type_id, query_type, paginate)
        
        if not response or 'data' not in response or 'assets' not in response['data']:
            break

        assets = response['data']['assets']
        if not assets:
            break

        paginate = assets[-1]['id']
        all_results.extend(assets)
        logging.info(f"Batch {batch_count}: Fetched {len(assets)} assets for {query_type}")

    return all_results

def merge_asset_data(main_data, attribute_data):
    merged = {asset['id']: asset for asset in main_data}
    
    for attr_result in attribute_data:
        asset_id = attr_result['id']
        if asset_id in merged:
            attr_copy = attr_result.copy()
            attr_copy.pop('id', None)
            merged[asset_id].update(attr_copy)
    
    return list(merged.values())

def process_data(asset_type_id):
    main_assets = fetch_all_data_for_query_type(asset_type_id, 'main')
    if not main_assets:
        logging.error("Failed to fetch main asset data")
        return []

    additional_query_types = ['string', 'multi', 'numeric', 'boolean', 
                            'outgoing', 'incoming', 'responsibilities']
    
    merged_assets = main_assets
    for query_type in additional_query_types:
        query_results = fetch_all_data_for_query_type(asset_type_id, query_type)
        if query_results:
            merged_assets = merge_asset_data(merged_assets, query_results)

    return merged_assets

def sanitize_identifier(name):
    if name is None:
        return 'unnamed'
    
    sanitized = name.lower()
    sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in sanitized)
    
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    
    while '__' in sanitized:
        sanitized = sanitized.replace('__', '_')
    
    sanitized = sanitized[:63].rstrip('_')
    
    return sanitized

def is_empty(value):
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, tuple)) and not value:
        return True
    return False

def safe_convert_to_str(value):
    if value is None:
        return None
        
    try:
        if isinstance(value, (list, tuple)):
            # Handle lists with proper encoding
            return ', '.join(
                str(v).encode('utf-8', errors='replace').decode('utf-8')
                for v in value 
                if v is not None
            )
        # Handle single values with proper encoding
        return str(value).encode('utf-8', errors='replace').decode('utf-8')
    except Exception as e:
        logging.error(f"Error converting value to string: {e}")
        return None

def create_table_if_not_exists(db_session, table_name, columns):
    try:
        columns_def = ["uuid VARCHAR PRIMARY KEY"]
        
        for col_name, _ in columns.items():
            if col_name != 'UUID of Asset':
                safe_col_name = sanitize_identifier(col_name)
                columns_def.append(f"{safe_col_name} TEXT NULL")

        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_def)}
            )
        """)
        
        db_session.execute(create_table_sql)
        db_session.commit()
        
        inspector = inspect(engine)
        actual_columns = [col['name'] for col in inspector.get_columns(table_name)]
        logging.info(f"Table columns: {actual_columns}")
        
    except Exception as e:
        db_session.rollback()
        logging.error(f"Error creating table {table_name}: {e}")
        raise

def save_to_postgres(asset_type_name, data):
    if not data:
        logging.warning(f"No data to save for {asset_type_name}")
        return

    with PerformanceLogger(f"save_to_postgres_{asset_type_name}"):
        # Ensure asset_type_name is properly encoded
        safe_asset_type_name = sanitize_identifier(
            asset_type_name.encode('utf-8', errors='replace').decode('utf-8')
            if asset_type_name else 'unknown_asset_type'
        )
        table_name = f"collibra_{safe_asset_type_name}"
        
        db_session = SessionLocal()
        
        try:
            base_columns = set()
            for row in data:
                base_columns.update(row.keys())
            
            columns_dict = {col: 'TEXT' for col in base_columns}
            create_table_if_not_exists(db_session, table_name, columns_dict)
            
            sanitized_columns = {
                key: 'uuid' if key == 'UUID of Asset' else sanitize_identifier(key)
                for key in base_columns
            }

            columns_list = list(sanitized_columns.values())
            placeholders = ', '.join([f':{col}' for col in columns_list])
            update_columns = ', '.join([f'{col} = EXCLUDED.{col}' for col in columns_list if col != 'uuid'])
            
            upsert_stmt = text(f"""
                INSERT INTO {table_name} ({', '.join(columns_list)})
                VALUES ({placeholders})
                ON CONFLICT (uuid) DO UPDATE SET
                    {update_columns}
            """)
            
            prepared_data = []
            for row in data:
                prepared_row = {}
                for original_key, column_name in sanitized_columns.items():
                    value = row.get(original_key)
                    prepared_row[column_name] = safe_convert_to_str(value)
                prepared_data.append(prepared_row)
            
            if prepared_data:
                db_session.execute(upsert_stmt, prepared_data)
                db_session.commit()
                logging.info(f"Saved {len(prepared_data)} records to {table_name}")
        
        except Exception as e:
            db_session.rollback()
            logging.error(f"Error saving data for {asset_type_name}: {e}")
            raise
        
        finally:
            db_session.close()

def flatten_json(asset, asset_type_name):
    """
    Flatten the JSON with proper text normalization
    """
    try:
        flattened = {
            "UUID of Asset": asset.get('id'),
            f"{asset_type_name} Full Name": normalize_text(asset.get('fullName')),
            f"{asset_type_name} Name": normalize_text(asset.get('displayName')),
            "Asset Type": asset.get('type', {}).get('name'),
            "Status": asset.get('status', {}).get('name'),
            f"Domain of {asset_type_name}": asset.get('domain', {}).get('name'),
            f"Community of {asset_type_name}": asset.get('domain', {}).get('parent', {}).get('name'),
            f"{asset_type_name} modified on": asset.get('modifiedOn'),
            f"{asset_type_name} last modified By": asset.get('modifiedBy', {}).get('fullName'),
            f"{asset_type_name} created on": asset.get('createdOn'),
            f"{asset_type_name} created By": asset.get('createdBy', {}).get('fullName'),
        }

        responsibilities = asset.get('responsibilities', [])
        if responsibilities:
            flattened.update({
                f"User Role Against {asset_type_name}": ', '.join(filter(None, (r.get('role', {}).get('name') for r in responsibilities))),
                f"User Name Against {asset_type_name}": ', '.join(filter(None, (r.get('user', {}).get('fullName') for r in responsibilities))),
                f"User Email Against {asset_type_name}": ', '.join(filter(None, (r.get('user', {}).get('email') for r in responsibilities)))
            })

        string_attrs = defaultdict(list)
        for attr_type in ['multiValueAttributes', 'stringAttributes', 'numericAttributes', 'dateAttributes', 'booleanAttributes']:
            for attr in asset.get(attr_type, []):
                attr_name = attr.get('type', {}).get('name')
                if not attr_name:
                    continue

                if attr_type == 'multiValueAttributes':
                    values = [v.strip() for v in attr.get('stringValues', []) if v and v.strip()]
                    flattened[attr_name] = ', '.join(values) if values else None
                elif attr_type == 'stringAttributes':
                    value = attr.get('stringValue', '').strip()
                    if value:
                        string_attrs[attr_name].append(value)
                else:
                    value_key = f"{attr_type[:-10]}Value"
                    flattened[attr_name] = attr.get(value_key)

        for attr_name, values in string_attrs.items():
            unique_values = list(dict.fromkeys(values))
            flattened[attr_name] = ', '.join(unique_values) if unique_values else None

        relation_types = defaultdict(list)
        relation_ids = defaultdict(list)
        
        for relation_direction in ['outgoingRelations', 'incomingRelations']:
            for relation in asset.get(relation_direction, []):
                target_or_source = 'target' if relation_direction == 'outgoingRelations' else 'source'
                role_or_corole = 'role' if relation_direction == 'outgoingRelations' else 'corole'
                
                related_obj = relation.get(target_or_source, {})
                role_type = relation.get('type', {}).get(role_or_corole, '')
                
                if relation_direction == 'outgoingRelations':
                    rel_type = f"{asset_type_name} {role_type} {related_obj.get('type', {}).get('name')}"
                else:
                    rel_type = f"{related_obj.get('type', {}).get('name')} {role_type} {asset_type_name}"
                
                display_name = related_obj.get('displayName', '').strip()
                asset_id = related_obj.get('id')
                
                if display_name:
                    relation_types[rel_type].append(display_name)
                    if asset_id:
                        relation_ids[f"{rel_type} Asset IDs"].append(asset_id)

        for rel_type, values in relation_types.items():
            flattened[rel_type] = ', '.join(values) if values else None
            id_key = f"{rel_type} Asset IDs"
            if id_key in relation_ids:
                flattened[id_key] = ', '.join(relation_ids[id_key]) if relation_ids[id_key] else None

        # Apply text normalization to all string values
        return {k: normalize_text(v) if isinstance(v, str) else v 
                for k, v in flattened.items() 
                if not is_empty(v)}
                
    except Exception as e:
        logging.error(f"Error flattening JSON: {e}")
        return {"UUID of Asset": asset.get('id')}

def process_asset_type(asset_type_id):
    """Process a single asset type with the new multi-query approach."""
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logging.info(f"Processing asset type: {asset_type_name}")

    all_assets = process_data(asset_type_id)

    if all_assets:
        flattened_assets = [flatten_json(asset, asset_type_name) for asset in all_assets]
        save_to_postgres(asset_type_name, flattened_assets)

        elapsed_time = time.time() - start_time
        logging.info(f"Completed processing {asset_type_name} in {elapsed_time:.2f} seconds")
        return elapsed_time
    else:
        logging.error(f"No data found for {asset_type_name}")
        return 0

def main():
    """Main execution function with parallel processing."""
    try:
        total_start_time = time.time()
        success_count = 0
        error_count = 0
        
        logging.info(f"Starting processing of {len(ASSET_TYPE_IDS)} asset types")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_asset = {
                executor.submit(process_asset_type, asset_type_id): asset_type_id 
                for asset_type_id in ASSET_TYPE_IDS
            }
            
            for future in as_completed(future_to_asset):
                asset_type_id = future_to_asset[future]
                try:
                    elapsed_time = future.result()
                    if elapsed_time > 0:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    logging.error(f"Error processing asset type {asset_type_id}: {str(e)}")
        
        total_time = time.time() - total_start_time
        logging.info(f"""
Export Summary:
--------------
Total asset types: {len(ASSET_TYPE_IDS)}
Successful: {success_count}
Failed: {error_count}
Total time: {total_time:.2f} seconds
        """)
        
    except Exception as e:
        logging.critical(f"Critical error in main execution: {str(e)}")
        raise

# Add new function to handle text normalization
def normalize_text(text):
    """Normalize text to handle special characters"""
    if text is None:
        return None
    try:
        # Handle various types of quotes and special characters
        import unicodedata
        # Normalize Unicode characters
        normalized = unicodedata.normalize('NFKD', str(text))
        # Remove non-ASCII characters but keep basic punctuation
        ascii_text = normalized.encode('ascii', errors='replace').decode('ascii')
        return ascii_text
    except Exception as e:
        logging.warning(f"Error normalizing text: {e}")
        return str(text).encode('ascii', errors='replace').decode('ascii')

if __name__ == "__main__":
    main()
