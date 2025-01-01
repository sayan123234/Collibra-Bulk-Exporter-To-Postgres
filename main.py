import os
import json
import time
import logging
import sys
import codecs
import locale
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, DateTime, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError

from graphql_query import get_query
from get_assetType_name import get_asset_type_name
from OauthAuth import oauth_bearer_token
from get_asset_type import get_available_asset_type

# Set console encoding to UTF-8
if sys.platform == 'win32':
    # For Windows, force UTF-8 encoding
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')
    # Set console code page to UTF-8
    os.system('chcp 65001')

# Setup logging with encoding specification
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('app.log', encoding='utf-8')
    ]
)

# Load environment variables
load_dotenv()

# Load configuration from environment
base_url = os.getenv('COLLIBRA_INSTANCE_URL')
database_url = os.getenv('DATABASE_URL')

# Create SQLAlchemy engine and session
engine = create_engine(database_url)
SessionLocal = sessionmaker(bind=engine)
metadata = MetaData()

# Load asset type IDs
with open('Collibra_Asset_Type_Id_Manager.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# For logging:
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('app.log', encoding='utf-8', errors='replace')
    ]
)

ASSET_TYPE_IDS = data['ids']

session = requests.Session()
session.headers.update({'Authorization': f'Bearer {oauth_bearer_token()}'})

def is_empty(value):
    """
    Check if a value is considered empty
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ''
    if isinstance(value, list):
        return len(value) == 0
    return False

def safe_convert_to_str(value):
    if value is None:
        return None
    try:
        if isinstance(value, (list, tuple)):
            return ', '.join(str(v).encode('utf-8', 'replace').decode('utf-8') for v in value if v is not None)
        return str(value).encode('utf-8', 'replace').decode('utf-8')
    except Exception as e:
        logging.error(f"Error converting value {value} to string: {e}")
        return None

def create_table_dynamically(asset_type_name, data):
    """
    Dynamically create a table for a specific asset type if it doesn't exist
    """
    # Same implementation as before, with added logging and error handling
    from sqlalchemy import text, Column, String, DateTime
    from sqlalchemy.exc import SQLAlchemyError

    # Sanitize table name to ensure it's database-friendly
    table_name = f"collibra_{asset_type_name.lower().replace(' ', '_')}"
    table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)

    # Prepare columns dictionary
    columns = {
        'uuid': Column('uuid', String, primary_key=True),
        'last_modified_on': Column('last_modified_on', DateTime, nullable=False)
    }

    # Create dynamic columns from the first data row
    if data:
        first_row = data[0]
        for key, value in first_row.items():
            if key not in ['UUID of Asset', 'Asset last Modified On']:
                # Sanitize column names
                column_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in key.lower().replace(' ', '_'))
                columns[column_name] = Column(
                    column_name, 
                    String, 
                    nullable=True
                )

    # Use a transaction to ensure atomic operations
    with engine.begin() as connection:
        try:
            # Check if table exists using raw SQL
            exists_query = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)")
            table_exists = connection.execute(exists_query, {"table_name": table_name}).scalar()

            if not table_exists:
                # Create table using SQLAlchemy's Table and metadata
                from sqlalchemy import Table, MetaData
                meta = MetaData()
                new_table = Table(
                    table_name, 
                    meta,
                    Column('uuid', String, primary_key=True),
                    Column('last_modified_on', DateTime, nullable=False),
                    extend_existing=True
                )

                # Add dynamic columns
                for col_name, col_def in columns.items():
                    if col_name not in ['uuid', 'last_modified_on']:
                        new_table.append_column(col_def)

                # Create the table
                meta.create_all(engine)
                logging.info(f"Created table: {table_name}")
            
            # If table exists, ensure all columns are present
            # Get existing columns
            column_query = text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name
            """)
            existing_columns_result = connection.execute(column_query, {"table_name": table_name})
            existing_columns = [row[0] for row in existing_columns_result]

            # Add missing columns
            for col_name, col_def in columns.items():
                if col_name not in existing_columns and col_name not in ['uuid', 'last_modified_on']:
                    try:
                        # Use ALTER TABLE to add columns safely
                        add_column_sql = text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} VARCHAR NULL")
                        connection.execute(add_column_sql)
                        logging.info(f"Added column {col_name} to table {table_name}")
                    except Exception as e:
                        logging.error(f"Error adding column {col_name}: {e}")

        except SQLAlchemyError as e:
            logging.error(f"Error creating/checking table {table_name}: {e}")
            raise

    return table_name

def create_table_if_not_exists(db_session, table_name, columns):
    try:
        columns_def = []
        columns_def.append("uuid VARCHAR PRIMARY KEY")
        
        timestamp_columns = [
            "modified_on",
            "created_on",
            f"{table_name}_modified_on",
            f"{table_name}_created_on"
        ]
        
        text_columns = [
            f"{table_name}_modified_by",
            f"{table_name}_created_by",
            "last_modified_by"
        ]
        
        for col in timestamp_columns:
            columns_def.append(f"{col} TIMESTAMP NULL")
            
        for col in text_columns:
            columns_def.append(f"{col} TEXT NULL")
        
        for col_name, _ in columns.items():
            safe_col_name = ''.join(c if c.isalnum() or c == '_' else '_' 
                                  for c in col_name.lower().replace(' ', '_')).encode('utf-8', 'replace').decode('utf-8')
            
            if safe_col_name in timestamp_columns or safe_col_name in text_columns:
                continue
                
            columns_def.append(f"{safe_col_name} TEXT NULL")

        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_def)}
            )
        """)
        
        db_session.execute(create_table_sql)
        db_session.commit()
        
    except Exception as e:
        db_session.rollback()
        logging.error(f"Error creating table {table_name}: {e}")
        raise

def save_to_postgres(asset_type_name, data):
    if not data:
        return

    table_name = ''.join(c if c.isalnum() or c == '_' else '_' 
                        for c in f"collibra_{asset_type_name.lower().replace(' ', '_')}").encode('utf-8', 'replace').decode('utf-8')
    
    db_session = SessionLocal()
    
    try:
        base_columns = set(data[0].keys()) - {'UUID of Asset'}
        columns_dict = {col: 'TEXT' for col in base_columns}
        
        create_table_if_not_exists(db_session, table_name, columns_dict)
        
        sanitized_columns = {
            key: ''.join(c if c.isalnum() or c == '_' else '_' 
                        for c in key.lower().replace(' ', '_')).encode('utf-8', 'replace').decode('utf-8')
            for key in base_columns
        }

        columns_list = ['uuid'] + list(sanitized_columns.values())
        placeholders = ', '.join([f':{col}' for col in columns_list])
        update_columns = ', '.join([f'{col} = EXCLUDED.{col}' for col in columns_list[1:]])
        
        upsert_stmt = text(f"""
            INSERT INTO {table_name} ({', '.join(columns_list)})
            VALUES ({placeholders})
            ON CONFLICT (uuid) DO UPDATE SET
                {update_columns}
        """)
        
        prepared_data = []
        for row in data:
            if not row.get('UUID of Asset'):
                continue
                
            prepared_row = {'uuid': safe_convert_to_str(row['UUID of Asset'])}
            
            for original_key in base_columns:
                column_name = sanitized_columns[original_key]
                value = row.get(original_key)
                
                if any(time_word in column_name for time_word in ['modified_on', 'created_on']):
                    prepared_row[column_name] = value
                else:
                    prepared_row[column_name] = safe_convert_to_str(value)
            
            prepared_data.append(prepared_row)
        
        if prepared_data:
            db_session.execute(upsert_stmt, prepared_data)
            db_session.commit()
    
    except Exception as e:
        db_session.rollback()
        logging.error(f"Error saving data for {asset_type_name}: {e}")
        raise
    
    finally:
        db_session.close()
        
def fetch_data(asset_type_id, paginate, limit):
    try:
        query = get_query(asset_type_id, f'"{paginate}"' if paginate else 'null')
        variables = {'limit': limit}
        logging.info(f"Sending request with variables: {variables} and paginate: {paginate}")

        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        response = session.post(
            url=graphql_url,
            json={
                'query': query,
                'variables': variables
            }
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as error:
        logging.error(f'Error fetching data: {error}')
        return None

def process_data(asset_type_id, limit=94):
    all_assets = []
    paginate = None

    while True:
        object_response = fetch_data(asset_type_id, paginate, limit)

        if object_response and 'data' in object_response and 'assets' in object_response['data']:
            assets = object_response['data']['assets']

            if not assets:
                logging.info("No more assets to fetch.")
                break

            paginate = assets[-1]['id']

            logging.info(f"Fetched {len(assets)} assets")
            all_assets.extend(assets)
        else:
            logging.warning('No assets found or there was an error fetching data.')
            break

    if not all_assets:
        logging.warning("No data was fetched.")

    logging.info(f"Total assets fetched: {len(all_assets)}")
    return all_assets

def flatten_json(asset, asset_type_name):
    """
    Flatten the JSON for database storage with enhanced null handling
    """
    flattened = {
        "UUID of Asset": asset.get('id'),
        f"Asset last Modified On": asset.get('modifiedOn'),
        f"{asset_type_name} Full Name": asset.get('fullName'),
        f"{asset_type_name} Name": asset.get('displayName'),
        "Asset Type": asset.get('type', {}).get('name'),
        "Status": asset.get('status', {}).get('name'),
        f"Domain of {asset_type_name}": asset.get('domain', {}).get('name'),
        f"Community of {asset_type_name}": asset.get('domain', {}).get('parent', {}).get('name'),
        f"{asset_type_name} modified on": asset.get('modifiedOn'),
        f"{asset_type_name} last modified By": asset.get('modifiedBy', {}).get('fullName'),
        f"{asset_type_name} created on": asset.get('createdOn'),
        f"{asset_type_name} created By": asset.get('createdBy', {}).get('fullName'),
    }

    # Process responsibilities
    responsibilities = asset.get('responsibilities', [])
    if responsibilities:
        user_roles = [r.get('role', {}).get('name') for r in responsibilities if r.get('role')]
        user_names = [r.get('user', {}).get('fullName') for r in responsibilities if r.get('user')]
        user_emails = [r.get('user', {}).get('email') for r in responsibilities if r.get('user')]
        
        flattened[f"User Role Against {asset_type_name}"] = ', '.join(filter(None, user_roles)) or None
        flattened[f"User Name Against {asset_type_name}"] = ', '.join(filter(None, user_names)) or None
        flattened[f"User Email Against {asset_type_name}"] = ', '.join(filter(None, user_emails)) or None

    # Process attributes section
    for attr_type in ['multiValueAttributes', 'stringAttributes', 'numericAttributes', 'dateAttributes', 'booleanAttributes']:
        # Create a temporary dictionary to collect string attributes
        string_attrs = defaultdict(list)
        
        for attr in asset.get(attr_type, []):
            attr_name = attr.get('type', {}).get('name')
            if not attr_name:
                continue

            if attr_type == 'multiValueAttributes':
                # Get string values and filter out any empty ones
                values = [v.strip() for v in attr.get('stringValues', []) if v and v.strip()]
                flattened[attr_name] = ', '.join(values) if values else None
            elif attr_type == 'stringAttributes':
                # Collect string attributes
                value = attr.get('stringValue', '').strip()
                if value:
                    string_attrs[attr_name].append(value)
            else:
                value_key = f"{attr_type[:-10]}Value"
                value = attr.get(value_key)
                flattened[attr_name] = str(value) if value is not None else None
        
        # Process collected string attributes
        for attr_name, values in string_attrs.items():
            # Remove duplicates while preserving order
            unique_values = list(dict.fromkeys(values))
            flattened[attr_name] = ', '.join(unique_values) if len(unique_values) > 0 else None

    # Process relations with separate name and ID tracking
    relation_types = defaultdict(list)
    relation_ids = defaultdict(list)
    
    for relation_direction in ['outgoingRelations', 'incomingRelations']:
        for relation in asset.get(relation_direction, []):
            role_or_corole = 'role' if relation_direction == 'outgoingRelations' else 'corole'
            role_type = relation.get('type', {}).get(role_or_corole, '')
            target_or_source = 'target' if relation_direction == 'outgoingRelations' else 'source'
            
            if relation_direction == 'outgoingRelations':
                rel_type = f"{asset_type_name} {role_type} {relation.get(target_or_source, {}).get('type', {}).get('name')}"
            else:
                rel_type = f"{relation.get(target_or_source, {}).get('type', {}).get('name')} {role_type} {asset_type_name}"
            
            target_source_obj = relation.get(target_or_source, {})
            display_name = target_source_obj.get('displayName', '').strip()
            asset_id = target_source_obj.get('id')
            
            if display_name:
                relation_types[rel_type].append(display_name)
                if asset_id:
                    relation_ids[f"{rel_type} Asset IDs"].append(asset_id)

    # Add relations and their IDs to flattened data
    for rel_type, values in relation_types.items():
        flattened[rel_type] = ', '.join(values) if values else None
        id_key = f"{rel_type} Asset IDs"
        if id_key in relation_ids:
            flattened[id_key] = ', '.join(relation_ids[id_key]) if relation_ids[id_key] else None

    # Final pass to remove any remaining None or empty string values
    for key, value in list(flattened.items()):
        if is_empty(value):
            flattened[key] = None

    return flattened

def is_empty(value):
    """Helper function to check if a value should be considered empty"""
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False

def process_asset_type(asset_type_id):
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logging.info(f"Processing asset type: {asset_type_name}")

    all_assets = process_data(asset_type_id)

    if all_assets:
        flattened_assets = [flatten_json(asset, asset_type_name) for asset in all_assets]
        save_to_postgres(asset_type_name, flattened_assets)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logging.info(f"Time taken to process {asset_type_name}: {elapsed_time:.2f} seconds")
        return elapsed_time
    else:
        logging.critical(f"No data to save")
        return 0

def main():
    total_start_time = time.time()
    
    total_elapsed_time = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_asset = {executor.submit(process_asset_type, asset_type_id): asset_type_id for asset_type_id in ASSET_TYPE_IDS}
        for future in as_completed(future_to_asset):
            elapsed_time = future.result()
            if elapsed_time:
                total_elapsed_time += elapsed_time
    
    total_end_time = time.time()
    total_program_time = total_end_time - total_start_time
    
    logging.info(f"\nTotal time taken to process all asset types: {total_elapsed_time:.2f} seconds")
    logging.info(f"Total program execution time: {total_program_time:.2f} seconds")

if __name__ == "__main__":
    main()