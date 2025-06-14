"""
PostgreSQL database module

This module provides functions for interacting with PostgreSQL database.
"""

import os
import logging
from sqlalchemy import create_engine, Column, String, DateTime, MetaData, Table, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

from collibra_exporter.utils.common import PerformanceLogger, sanitize_identifier, safe_convert_to_str

# Configure logger
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get database URL from environment
database_url = os.getenv('DATABASE_URL')

# Create SQLAlchemy engine and session
engine = create_engine(database_url)
SessionLocal = sessionmaker(bind=engine)
metadata = MetaData()

def get_current_schema():
    """
    Get the current schema from the database connection.
    
    Returns:
        str: The current schema name or 'public' if not found
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT current_schema()"))
            current_schema = result.scalar()
            logger.info(f"Current database schema: {current_schema}")
            return current_schema
    except Exception as e:
        logger.error(f"Error getting current schema: {e}")
        return 'public'

def has_dependent_views(table_name):
    """
    Check if a table has any dependent views.
    
    Args:
        table_name (str): The name of the table to check
        
    Returns:
        bool: True if the table has dependent views, False otherwise
    """
    schema = get_current_schema()
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
                logger.info(f"Table {schema}.{table_name} does not exist yet")
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
        logger.error(f"Error checking dependent views for table {table_name}: {e}")
        return False

def get_dependent_views(table_name):
    """
    Get views that depend on a specific table.
    
    Args:
        table_name (str): The name of the table
        
    Returns:
        dict: Dictionary of dependent views with their definitions and levels
    """
    schema = get_current_schema()
    logger.info(f"Finding views dependent on {schema}.{table_name}")
    
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
            
            logger.info(f"Found {len(views)} dependent views for table {table_name}")
            if views:
                for viewname, view_info in views.items():
                    logger.debug(f"Dependent view: {viewname} at level {view_info['level']}")
            
            return views
            
    except Exception as e:
        logger.error(f"Error getting dependent views for table {table_name}: {e}")
        raise

def restore_views(views):
    """
    Restore views in correct dependency order.
    
    Args:
        views (dict): Dictionary of views to restore
    """
    schema = get_current_schema()
    try:
        with engine.connect() as connection:
            for viewname, view_info in sorted(views.items(), key=lambda x: x[1]['level']):
                try:
                    create_view_sql = f"CREATE OR REPLACE VIEW {schema}.{viewname} AS {view_info['definition']}"
                    connection.execute(text(create_view_sql))
                    connection.commit()
                    logger.info(f"Restored dependent view: {schema}.{viewname}")
                except Exception as e:
                    logger.error(f"Error restoring view {schema}.{viewname}: {e}")
                    logger.error(f"View definition: {create_view_sql}")
                    raise
    except Exception as e:
        logger.error(f"Error in restore_views: {e}")
        raise

def create_table_if_not_exists(table_name, columns):
    """
    Create a table if it doesn't exist.
    
    Args:
        table_name (str): The name of the table to create
        columns (dict): Dictionary of column names and types
    """
    db_session = SessionLocal()
    try:
        # Log the columns that will be created
        logger.info(f"Creating table {table_name} with columns: {list(columns.keys())}")
        
        columns_def = []
        columns_def.append("asset_id VARCHAR PRIMARY KEY")
        
        for col_name, _ in columns.items():
            if col_name != 'UUID of Asset':
                safe_col_name = sanitize_identifier(col_name)
                logger.debug(f"Creating column: {safe_col_name} (original: {col_name})")
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
        logger.info(f"Actual columns in table: {actual_columns}")
        
    except Exception as e:
        db_session.rollback()
        logger.error(f"Error creating table {table_name}: {e}")
        raise
    finally:
        db_session.close()

def save_to_postgres(asset_type_name, data):
    """
    Save flattened asset data to PostgreSQL database.
    
    Args:
        asset_type_name (str): The name of the asset type
        data (list): List of flattened asset data dictionaries
    """
    if not data:
        logger.warning(f"No data to save for {asset_type_name}")
        return

    with PerformanceLogger(f"save_to_postgres_{asset_type_name}"):
        safe_asset_type_name = sanitize_identifier(asset_type_name or 'unknown_asset_type')
        table_name = f"collibra_{safe_asset_type_name}"
        
        db_session = SessionLocal()
        dependent_views = None
        
        try:
            # First check if this table has any dependent views
            if has_dependent_views(table_name):
                logger.info(f"Table {table_name} has dependent views, saving them...")
                dependent_views = get_dependent_views(table_name)
                if dependent_views:
                    logger.info(f"Found {len(dependent_views)} dependent views to preserve")
            else:
                logger.info(f"No dependent views found for table {table_name}")
            
            # Get columns from the flattened data
            base_columns = set()
            for row in data:
                base_columns.update(row.keys())
            
            logger.info(f"Total unique columns found: {len(base_columns)}")
            columns_dict = {col: 'TEXT' for col in base_columns}
            
            # Drop the table (CASCADE only if we have dependent views)
            drop_stmt = text(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            db_session.execute(drop_stmt)
            db_session.commit()
            logger.info(f"Dropped table: {table_name}")
            
            # Create fresh table with all columns
            create_table_if_not_exists(table_name, columns_dict)
            
            # Prepare the data with consistent UUID handling
            sanitized_columns = {}
            for key in base_columns:
                safe_name = sanitize_identifier(key)
                if key == 'UUID of Asset':
                    safe_name = 'asset_id'
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
                        logger.warning(f"Missing column mapping for: {original_key}")
                        continue
                        
                    column_name = sanitized_columns[original_key]
                    value = row.get(original_key)
                    prepared_row[column_name] = safe_convert_to_str(value)
                
                prepared_data.append(prepared_row)
            
            if prepared_data:
                db_session.execute(insert_stmt, prepared_data)
                db_session.commit()
                logger.info(f"Successfully saved {len(prepared_data)} records to {table_name}")
            
            # After data insertion, restore views only if we saved any
            if dependent_views:
                logger.info("Restoring dependent views...")
                try:
                    restore_views(dependent_views)
                    logger.info("Dependent views restored successfully")
                except Exception as e:
                    logger.error(f"Failed to restore dependent views: {e}")
                    raise
            
        except Exception as e:
            db_session.rollback()
            logger.error(f"Error saving data for {asset_type_name}: {e}")
            raise
        
        finally:
            db_session.close()
