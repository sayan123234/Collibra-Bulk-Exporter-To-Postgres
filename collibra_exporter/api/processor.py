"""
Asset data processor module

This module provides functions for processing and transforming Collibra asset data.
"""

import logging
from collections import defaultdict

from collibra_exporter.utils.common import is_empty, PerformanceLogger
from collibra_exporter.api.client import client
from collibra_exporter.api.asset_types import get_asset_type_name

# Configure logger
logger = logging.getLogger(__name__)

def process_data(asset_type_id, limit=94, nested_limit=50):
    """
    Process asset data with improved nested field handling.
    
    Args:
        asset_type_id (str): The asset type ID to process
        limit (int): Maximum number of assets to return per batch
        nested_limit (int): Limit for nested fields
        
    Returns:
        list: List of processed assets
    """
    asset_type_name = get_asset_type_name(asset_type_id)
    logger.info("="*60)
    logger.info(f"Starting data processing for asset type: {asset_type_name} (ID: {asset_type_id})")
    logger.info(f"Configuration - Batch Size: {limit}, Nested Limit: {nested_limit}")
    logger.info("="*60)
    
    all_assets = []
    paginate = None
    batch_count = 0

    with PerformanceLogger(f"process_data_{asset_type_name}"):
        while True:
            batch_count += 1
            with PerformanceLogger(f"batch_{batch_count}"):
                logger.info(f"\n[Batch {batch_count}] Starting new batch for {asset_type_name}")
                logger.debug(f"[Batch {batch_count}] Pagination token: {paginate}")
                
                # Get initial batch of assets
                object_response = client.fetch_data(asset_type_id, paginate, limit, 0, nested_limit)
                if not object_response or 'data' not in object_response or 'assets' not in object_response['data']:
                    logger.error(f"[Batch {batch_count}] Failed to fetch initial data")
                    break

                current_assets = object_response['data']['assets']
                if not current_assets:
                    logger.info(f"[Batch {batch_count}] No more assets to fetch")
                    break

                logger.info(f"[Batch {batch_count}] Processing {len(current_assets)} assets")

                # Process each asset
                processed_assets = []
                for asset_idx, asset in enumerate(current_assets, 1):
                    asset_id = asset['id']
                    asset_name = asset.get('displayName', 'Unknown Name')
                    logger.info(f"\n[Batch {batch_count}][Asset {asset_idx}/{len(current_assets)}] Processing: {asset_name}")
                    
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

                    # Process each nested field
                    for field in nested_fields:
                        if field not in asset:
                            continue
                            
                        initial_data = asset[field]
                        
                        # If we hit the initial limit, fetch all data using pagination
                        if len(initial_data) == nested_limit:
                            logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                      f"Fetching complete data with pagination...")
                                      
                            complete_data = client.fetch_nested_data_with_pagination(
                                asset_type_id,
                                asset_id,
                                field
                            )
                            
                            if complete_data:
                                complete_asset[field] = complete_data
                                logger.info(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                          f"Retrieved {len(complete_data)} total items")
                            else:
                                logger.warning(f"[Batch {batch_count}][Asset {asset_idx}][{field}] "
                                             f"Failed to fetch complete data, using initial data")
                                complete_asset[field] = initial_data
                        else:
                            complete_asset[field] = initial_data

                    processed_assets.append(complete_asset)
                    logger.info(f"[Batch {batch_count}][Asset {asset_idx}] Completed processing")

                all_assets.extend(processed_assets)
                
                if len(current_assets) < limit:
                    logger.info(f"[Batch {batch_count}] Retrieved fewer assets than limit, ending pagination")
                    break
                    
                paginate = current_assets[-1]['id']
                logger.info(f"\n[Batch {batch_count}] Completed batch")
                logger.info(f"Total assets processed so far: {len(all_assets)}")

    logger.info("\n" + "="*60)
    logger.info(f"[DONE] Completed processing {asset_type_name}")
    logger.info(f"Total assets processed: {len(all_assets)}")
    logger.info(f"Total batches processed: {batch_count}")
    logger.info("="*60)
    
    return all_assets

def flatten_json(asset, asset_type_name):
    """
    Flatten the JSON for database storage with enhanced null handling.
    
    Args:
        asset (dict): The asset data to flatten
        asset_type_name (str): The name of the asset type
        
    Returns:
        dict: Flattened asset data
    """
    flattened = {
        # "UUID of Asset": asset.get('id'),
        f"UUID of Asset": asset.get('fullName'),
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
                rel_type = f"OGR {asset_type_name} {role_type} {relation.get(target_or_source, {}).get('type', {}).get('name')}"
            else:
                rel_type = f"ICR {asset_type_name} {role_type} {relation.get(target_or_source, {}).get('type', {}).get('name')}"
                
            
            target_source_obj = relation.get(target_or_source, {})
            display_name = target_source_obj.get('displayName', '').strip()
            asset_id = target_source_obj.get('fullName')
            
            if display_name:
                relation_types[rel_type].append(display_name)
                if asset_id:
                    relation_ids[f"{rel_type}_id"].append(asset_id)

    # Add relations and their IDs to flattened data
    for rel_type, values in relation_types.items():
        flattened[rel_type] = ', '.join(values) if values else None
        id_key = f"{rel_type}_id"
        if id_key in relation_ids:
            flattened[id_key] = ', '.join(relation_ids[id_key]) if relation_ids[id_key] else None

    # Final pass to remove any remaining None or empty string values
    for key, value in list(flattened.items()):
        if is_empty(value):
            flattened[key] = None

    return flattened
