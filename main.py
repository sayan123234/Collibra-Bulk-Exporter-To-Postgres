# Standard library imports
import os
import json
import time
import logging
import sys
import codecs
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# Third-party imports
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, DateTime, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError

# Local imports
from graphql_query import get_query, get_nested_query
from get_assetType_name import get_asset_type_name
from OauthAuth import get_auth_header  # Changed from oauth_bearer_token to get_auth_header
from get_asset_type import get_available_asset_type

# Configure logging with both file and console handlers
def setup_logging():
    """Configure logging with both file and console handlers with proper formatting"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'logs/app_{time.strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
            logging.FileHandler('logs/latest.log', encoding='utf-8', mode='w')
        ]
    )
    
    # Add debug file handler
    debug_handler = logging.FileHandler('logs/debug.log', encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(logging.Formatter(log_format, date_format))
    logging.getLogger().addHandler(debug_handler)

# Initialize logging
setup_logging()

# Set console encoding to UTF-8 for Windows
if sys.platform == 'win32':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')
    os.system('chcp 65001')
    logging.debug("Windows console encoding set to UTF-8")

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

ASSET_TYPE_IDS = data['ids']

# Create session with request handling
session = requests.Session()

def make_request(url, method='post', **kwargs):
    """Make a request with automatic token refresh handling."""
    try:
        # Always get fresh headers before making a request
        headers = get_auth_header()
        if 'headers' in kwargs:
            kwargs['headers'].update(headers)
        else:
            kwargs['headers'] = headers

        response = getattr(session, method)(url=url, **kwargs)
        response.raise_for_status()
        return response
    except requests.RequestException as error:
        logging.error(f"Request failed: {str(error)}")
        raise

class PerformanceLogger:
    """Context manager for logging execution time of code blocks"""
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
    """Handle special characters and encodings"""
    if value is None:
        return None
        
    try:
        if isinstance(value, (list, tuple)):
            return ', '.join(str(v).encode('ascii', 'ignore').decode('ascii') for v in value if v is not None)
        return str(value).encode('ascii', 'ignore').decode('ascii')
    except Exception as e:
        logging.error(f"Error converting value to string: {e}")
        return None

def sanitize_identifier(name):
    """Sanitize table/column names more reliably"""
    if name is None:
        return 'unnamed'
    
    # Replace common problematic characters
    sanitized = name.lower()
    sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in sanitized)
    
    # Ensure it starts with a letter or underscore
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    
    # Remove consecutive underscores and trim length
    while '__' in sanitized:
        sanitized = sanitized.replace('__', '_')
    
    # Trim to PostgreSQL's maximum identifier length (63 characters)
    sanitized = sanitized[:63]
    
    # Remove trailing underscores
    sanitized = sanitized.rstrip('_')
    
    return sanitized

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
            if key != 'UUID of Asset':  # Remove check for 'Asset last Modified On'
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
        # Log the columns that will be created
        logging.info(f"Creating table {table_name} with columns: {list(columns.keys())}")
        
        columns_def = []
        columns_def.append("uuid VARCHAR PRIMARY KEY")
        
        for col_name, _ in columns.items():
            if col_name != 'UUID of Asset':
                safe_col_name = sanitize_identifier(col_name)
                logging.debug(f"Creating column: {safe_col_name} (original: {col_name})")
                columns_def.append(f"{safe_col_name} TEXT NULL")

        create_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_def)}
            )
        """)
        
        # Execute table creation
        db_session.execute(create_table_sql)
        db_session.commit()
        
        # Verify created columns
        inspector = inspect(engine)
        actual_columns = [col['name'] for col in inspector.get_columns(table_name)]
        logging.info(f"Actual columns in table: {actual_columns}")
        
    except Exception as e:
        db_session.rollback()
        logging.error(f"Error creating table {table_name}: {e}")
        raise
def get_current_schema(engine):
    """Get the current schema from the engine"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT current_schema()"))
            current_schema = result.scalar()
            logging.info(f"Current database schema: {current_schema}")
            return current_schema
    except Exception as e:
        logging.error(f"Error getting current schema: {e}")
        return 'public'

def has_dependent_views(engine, table_name):
    """
    Check if a table has any dependent views using information_schema
    """
    schema = get_current_schema(engine)
    try:
        with engine.connect() as connection:
            # First check if table exists
            table_exists_query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables 
                WHERE table_schema = :schema 
                AND table_name = :table_name
            );
            """
            
            table_exists = connection.execute(
                text(table_exists_query), 
                {'schema': schema, 'table_name': table_name}
            ).scalar()
            
            if not table_exists:
                logging.info(f"Table {schema}.{table_name} does not exist yet")
                return False
            
            # Check for dependent views using view_table_usage
            check_query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.view_table_usage v
                WHERE v.table_schema = :schema
                AND v.table_name = :table_name
            );
            """
            
            result = connection.execute(text(check_query), {
                'schema': schema,
                'table_name': table_name
            })
            
            has_views = result.scalar()
            return has_views
            
    except Exception as e:
        logging.error(f"Error checking dependent views for table {table_name}: {e}")
        return False

def get_dependent_views(engine, table_name):
    """
    Get views that depend on a specific table using information_schema
    """
    schema = get_current_schema(engine)
    logging.info(f"Finding views dependent on {schema}.{table_name}")
    
    try:
        with engine.connect() as connection:
            view_query = """
            WITH RECURSIVE view_deps AS (
                -- First level views directly dependent on our table
                SELECT DISTINCT 
                    vtu.view_name as viewname,
                    pg_get_viewdef(vtu.view_name::regclass) as definition,
                    0 as level,
                    ARRAY[vtu.view_name] as path
                FROM information_schema.view_table_usage vtu
                WHERE vtu.table_schema = :schema
                AND vtu.table_name = :table_name
                
                UNION ALL
                
                -- Recursively get views dependent on other views
                SELECT DISTINCT 
                    vtu.view_name,
                    pg_get_viewdef(vtu.view_name::regclass),
                    vd.level + 1,
                    vd.path || vtu.view_name
                FROM information_schema.view_table_usage vtu
                JOIN view_deps vd ON vtu.table_name = vd.viewname
                WHERE vtu.table_schema = :schema
                AND NOT vtu.view_name = ANY(vd.path)  -- Prevent cycles
            )
            SELECT viewname, definition, level
            FROM view_deps
            ORDER BY level DESC;
            """
            
            result = connection.execute(text(view_query), {
                'schema': schema,
                'table_name': table_name
            })
            
            views = {row.viewname: {
                'definition': row.definition,
                'level': row.level
            } for row in result}
            
            logging.info(f"Found {len(views)} dependent views for table {table_name}")
            if views:
                for viewname, view_info in views.items():
                    logging.debug(f"Dependent view: {viewname} at level {view_info['level']}")
            
            return views
            
    except Exception as e:
        logging.error(f"Error getting dependent views for table {table_name}: {e}")
        raise

def restore_views(engine, views):
    """
    Restore views in correct dependency order
    """
    schema = get_current_schema(engine)
    try:
        with engine.connect() as connection:
            for viewname, view_info in sorted(views.items(), key=lambda x: x[1]['level']):
                try:
                    create_view_sql = f"CREATE OR REPLACE VIEW {schema}.{viewname} AS {view_info['definition']}"
                    connection.execute(text(create_view_sql))
                    connection.commit()
                    logging.info(f"Restored dependent view: {schema}.{viewname}")
                except Exception as e:
                    logging.error(f"Error restoring view {schema}.{viewname}: {e}")
                    logging.error(f"View definition: {create_view_sql}")
                    raise
    except Exception as e:
        logging.error(f"Error in restore_views: {e}")
        raise

def save_to_postgres(asset_type_name, data):
    """
    Save flattened asset data to PostgreSQL database, preserving views only for tables that have them
    """
    if not data:
        logging.warning(f"No data to save for {asset_type_name}")
        return

    with PerformanceLogger(f"save_to_postgres_{asset_type_name}"):
        safe_asset_type_name = sanitize_identifier(asset_type_name or 'unknown_asset_type')
        table_name = f"collibra_{safe_asset_type_name}"
        
        db_session = SessionLocal()
        dependent_views = None
        
        try:
            # First check if this table has any dependent views
            if has_dependent_views(engine, table_name):
                logging.info(f"Table {table_name} has dependent views, saving them...")
                dependent_views = get_dependent_views(engine, table_name)
                if dependent_views:
                    logging.info(f"Found {len(dependent_views)} dependent views to preserve")
            else:
                logging.info(f"No dependent views found for table {table_name}")
            
            # Get columns from the flattened data
            base_columns = set()
            for row in data:
                base_columns.update(row.keys())
            
            logging.info(f"Total unique columns found: {len(base_columns)}")
            columns_dict = {col: 'TEXT' for col in base_columns}
            
            # Drop the table (CASCADE only if we have dependent views)
            drop_stmt = text(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            db_session.execute(drop_stmt)
            db_session.commit()
            logging.info(f"Dropped table: {table_name}")
            
            # Create fresh table with all columns
            create_table_if_not_exists(db_session, table_name, columns_dict)
            
            # Prepare the data with consistent UUID handling
            sanitized_columns = {}
            for key in base_columns:
                safe_name = sanitize_identifier(key)
                if key == 'UUID of Asset':
                    safe_name = 'uuid'
                sanitized_columns[key] = safe_name

            # Prepare the insert statement
            columns_list = list(sanitized_columns.values())
            placeholders = ', '.join([f':{col}' for col in columns_list])
            
            insert_stmt = text(f"""
                INSERT INTO {table_name} ({', '.join(columns_list)})
                VALUES ({placeholders})
            """)
            
            # Prepare the data
            prepared_data = []
            for row in data:
                if not row.get('UUID of Asset'):
                    continue
                    
                prepared_row = {}
                for original_key in base_columns:
                    if original_key not in sanitized_columns:
                        logging.warning(f"Missing column mapping for: {original_key}")
                        continue
                        
                    column_name = sanitized_columns[original_key]
                    value = row.get(original_key)
                    prepared_row[column_name] = safe_convert_to_str(value)
                
                prepared_data.append(prepared_row)
            
            if prepared_data:
                db_session.execute(insert_stmt, prepared_data)
                db_session.commit()
                logging.info(f"Successfully saved {len(prepared_data)} records to {table_name}")
            
            # After data insertion, restore views only if we saved any
            if dependent_views:
                logging.info("Restoring dependent views...")
                try:
                    restore_views(engine, dependent_views)
                    logging.info("Dependent views restored successfully")
                except Exception as e:
                    logging.error(f"Failed to restore dependent views: {e}")
                    raise
            
        except Exception as e:
            db_session.rollback()
            logging.error(f"Error saving data for {asset_type_name}: {e}")
            raise
        
        finally:
            db_session.close()

def fetch_data(asset_type_id, paginate, limit, nested_offset=0, nested_limit=50):
    """Fetch data with improved nested field handling"""
    try:
        query = get_query(asset_type_id, f'"{paginate}"' if paginate else 'null', nested_offset, nested_limit)
        variables = {'limit': limit}
        logging.debug(f"Sending GraphQL request for asset_type_id: {asset_type_id}, paginate: {paginate}, nested_offset: {nested_offset}")

        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        start_time = time.time()
        
        response = make_request(
            url=graphql_url,
            json={
                'query': query,
                'variables': variables
            }
        )
        
        response_time = time.time() - start_time
        logging.debug(f"GraphQL request completed in {response_time:.2f} seconds")

        data = response.json()
        
        if 'errors' in data:
            logging.error(f"GraphQL errors received: {data['errors']}")
            return None
            
        return data
    except Exception as error:
        logging.error(f'Error fetching data: {error}')
        return None
    
def fetch_nested_data_with_pagination(asset_type_id, asset_id, field_name, make_request, logger, batch_size=20000):
    """
    Fetch all nested data for a field using pagination.
    
    Args:
        asset_type_id: ID of the asset type
        asset_id: ID of the specific asset
        field_name: Name of the nested field to fetch
        make_request: Function to make GraphQL requests
        logger: Logger instance
        batch_size: Number of items to fetch per request
    
    Returns:
        List of all nested items for the field
    """
    all_items = []
    offset = 0
    batch_number = 1

    while True:
        logger.info(f"Fetching batch {batch_number} for {field_name} (offset: {offset})")
        
        query = get_nested_query(asset_type_id, asset_id, field_name, offset, batch_size)
        
        try:
            response = make_request(
                url=f"https://{base_url}/graphql/knowledgeGraph/v1",
                json={'query': query}
            )
            
            data = response.json()
            
            if 'errors' in data:
                logger.error(f"GraphQL errors in nested query: {data['errors']}")
                break
                
            if not data['data']['assets']:
                logger.error(f"No asset found in nested query response")
                break
                
            current_items = data['data']['assets'][0][field_name]
            current_batch_size = len(current_items)
            
            all_items.extend(current_items)
            logger.info(f"Retrieved {current_batch_size} items in batch {batch_number}")
            
            # If we got fewer items than the batch size, we've reached the end
            if current_batch_size < batch_size:
                break
                
            offset += batch_size
            batch_number += 1
            
        except Exception as e:
            logger.exception(f"Failed to fetch batch {batch_number} for {field_name}: {str(e)}")
            break

    logger.info(f"Completed fetching {field_name}. Total items: {len(all_items)}")
    return all_items

def fetch_nested_data(asset_type_id, asset_id, field_name, nested_limit=20000):
    """
    Updated fetch_nested_data function that uses pagination when needed.
    This function should replace the existing fetch_nested_data in main.py
    """
    try:
        # First attempt with maximum limit to see if pagination is needed
        query = get_nested_query(asset_type_id, asset_id, field_name, 0, nested_limit)
        
        graphql_url = f"https://{base_url}/graphql/knowledgeGraph/v1"
        start_time = time.time()
        
        response = make_request(
            url=graphql_url,
            json={'query': query}
        )
        
        response_time = time.time() - start_time
        logging.debug(f"Nested GraphQL request completed in {response_time:.2f} seconds")

        data = response.json()
        if 'errors' in data:
            logging.error(f"GraphQL errors in nested query: {data['errors']}")
            return None
            
        if not data['data']['assets']:
            logging.error(f"No asset found in nested query response")
            return None
            
        initial_results = data['data']['assets'][0][field_name]
        
        # If we hit the limit, use pagination to fetch all results
        if len(initial_results) == nested_limit:
            logging.info(f"Hit nested limit of {nested_limit} for {field_name}, switching to pagination")
            return fetch_nested_data_with_pagination(
                asset_type_id,
                asset_id,
                field_name,
                make_request,
                logging
            )
            
        return initial_results
    except Exception as e:
        logging.exception(f"Failed to fetch nested data for {field_name}: {str(e)}")
        return None    

def process_data(asset_type_id, limit=94, nested_limit=50):
    """Process asset data with improved nested field handling"""
    asset_type_name = get_asset_type_name(asset_type_id)
    logging.info("="*60)
    logging.info(f"Starting data processing for asset type: {asset_type_name} (ID: {asset_type_id})")
    logging.info(f"Configuration - Batch Size: {limit}, Nested Limit: {nested_limit}")
    logging.info("="*60)
    
    all_assets = []
    paginate = None
    batch_count = 0
    start_time = time.time()

    while True:
        batch_count += 1
        batch_start_time = time.time()
        logging.info(f"\n[Batch {batch_count}] Starting new batch for {asset_type_name}")
        logging.debug(f"[Batch {batch_count}] Pagination token: {paginate}")
        
        # Get initial batch of assets
        object_response = fetch_data(asset_type_id, paginate, limit, 0, nested_limit)
        if not object_response or 'data' not in object_response or 'assets' not in object_response['data']:
            logging.error(f"[Batch {batch_count}] Failed to fetch initial data")
            break

        current_assets = object_response['data']['assets']
        if not current_assets:
            logging.info(f"[Batch {batch_count}] No more assets to fetch")
            break

        logging.info(f"[Batch {batch_count}] Processing {len(current_assets)} assets")

        # Process each asset
        processed_assets = []
        for asset_idx, asset in enumerate(current_assets, 1):
            asset_id = asset['id']
            asset_name = asset.get('displayName', 'Unknown Name')
            logging.info(f"\n[Batch {batch_count}][Asset {asset_idx}/{len(current_assets)}] Processing: {asset_name}")
            
            # Initialize complete asset with base data
            complete_asset = asset.copy()
            
            # Define nested fields to process
            nested_fields = [
                'stringAttributes', 
                'multiValueAttributes', 
                'numericAttributes', 
                'dateAttributes', 
                'booleanAttributes', 
                'outgoingRelations', 
                'incomingRelations', 
                'responsibilities'
            ]

            # Process each nested field using new fetch_nested_data function
            for field in nested_fields:
                if field not in asset:
                    continue
                    
                initial_data = asset[field]
                
                # If we hit the initial limit, fetch all data using new pagination function
                if len(initial_data) == nested_limit:
                    logging.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                              f"Fetching complete data with pagination...")
                              
                    complete_data = fetch_nested_data(
                        asset_type_id,
                        asset_id,
                        field
                    )
                    
                    if complete_data:
                        complete_asset[field] = complete_data
                        logging.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                  f"Retrieved {len(complete_data)} total items")
                    else:
                        logging.warning(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                     f"Failed to fetch complete data, using initial data")
                        complete_asset[field] = initial_data
                else:
                    complete_asset[field] = initial_data

            processed_assets.append(complete_asset)
            logging.info(f"[Batch {batch_count}][Asset {asset_idx}] Completed processing")

        all_assets.extend(processed_assets)
        
        if len(current_assets) < limit:
            logging.info(f"[Batch {batch_count}] Retrieved fewer assets than limit, ending pagination")
            break
            
        paginate = current_assets[-1]['id']
        batch_time = time.time() - batch_start_time
        logging.info(f"\n[Batch {batch_count}] Completed batch in {batch_time:.2f}s")
        logging.info(f"Total assets processed so far: {len(all_assets)}")

    total_time = time.time() - start_time
    logging.info("\n" + "="*60)
    logging.info(f"[DONE] Completed processing {asset_type_name}")
    logging.info(f"Total assets processed: {len(all_assets)}")
    logging.info(f"Total batches processed: {batch_count}")
    logging.info(f"Total time taken: {total_time:.2f} seconds")
    logging.info("="*60)
    
    return all_assets

def flatten_json(asset, asset_type_name):
    """
    Flatten the JSON for database storage with enhanced null handling
    """
    flattened = {
        "UUID of Asset": asset.get('id'),
        f"{asset_type_name} Full Name": asset.get('fullName'),
        f"{asset_type_name} Name": asset.get('displayName'),
        "Asset Type": asset.get('type', {}).get('name'),
        "Status": asset.get('status', {}).get('name'),
        f"Domain of {asset_type_name}": asset.get('domain', {}).get('name'),
        f"Community of {asset_type_name}": asset.get('domain', {}).get('parent', {}).get('name'),
        f"{asset_type_name} modified on": asset.get('modifiedOn'),  # This is the only modified_on we keep
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
                rel_type = f"{relation.get(target_or_source, {}).get('type', {}).get('name')} {role_type} {asset_type_name}"
            else:
                rel_type = f"{asset_type_name} {role_type} {relation.get(target_or_source, {}).get('type', {}).get('name')}"
                
            
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
    """
    Main execution function with improved error handling and logging
    """
    with PerformanceLogger("main_execution"):
        try:
            total_start_time = time.time()
            success_count = 0
            error_count = 0
            
            logging.info("Starting Collibra Bulk Export process")
            logging.info(f"Processing {len(ASSET_TYPE_IDS)} asset types")
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_asset = {
                    executor.submit(process_asset_type, asset_type_id): asset_type_id 
                    for asset_type_id in ASSET_TYPE_IDS
                }
                
                for future in as_completed(future_to_asset):
                    asset_type_id = future_to_asset[future]
                    try:
                        elapsed_time = future.result()
                        if elapsed_time:
                            success_count += 1
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

if __name__ == "__main__":
    main()
