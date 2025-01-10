# Function to get the main asset query with basic nested_limit
def get_query(asset_type_id, paginate, nested_offset=0, nested_limit=50):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }} id:{{gt:{paginate}}} }}
            limit: $limit
        ) {{
            id
            fullName
            displayName
            modifiedOn
            modifiedBy{{ 
                fullName
            }}
            createdOn
            createdBy{{
                fullName
            }}
            status{{
                name
            }}
            type {{
                name
            }}
            domain {{
                name
                parent {{
                    name
                }}
            }}
            stringAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValue
            }}
            multiValueAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValues
            }}
            numericAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                numericValue
            }}
            dateAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                dateValue
            }}
            booleanAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                booleanValue
            }}
            outgoingRelations (offset: {nested_offset}, limit: {nested_limit}) {{
                target {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
                type {{
                    role
                }}
            }}
            incomingRelations (offset: {nested_offset}, limit: {nested_limit}) {{
                source {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
                type {{
                    corole
                }}
            }}
            responsibilities (offset: {nested_offset}, limit: {nested_limit}) {{
                role {{
                    name
                }}
                user {{
                    fullName
                    email
                }}
            }}
        }}
    }}
    """

def get_nested_query(asset_type_id, asset_id, field_name, nested_offset=0, nested_limit=20000):
    """
    Generate a query for fetching a specific nested field with pagination support.
    
    Args:
        asset_type_id: ID of the asset type
        asset_id: ID of the specific asset
        field_name: Name of the nested field to fetch
        nested_offset: Offset for pagination
        nested_limit: Limit for number of nested items per request
    """
    # Base query structure with limit=1 to ensure we only get one asset
    base_query = f"""
    query Assets {{
        assets(
            where: {{ 
                type: {{ id: {{ eq: "{asset_type_id}" }} }}
                id: {{ eq: "{asset_id}" }}
            }}
            limit: 1
        ) {{
            id
    """

    # Field-specific query parts with pagination parameters
    field_queries = {
        'stringAttributes': f"""
            stringAttributes(offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValue
            }}
        """,
        'multiValueAttributes': f"""
            multiValueAttributes(offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValues
            }}
        """,
        'numericAttributes': f"""
            numericAttributes(offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                numericValue
            }}
        """,
        'dateAttributes': f"""
            dateAttributes(offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                dateValue
            }}
        """,
        'booleanAttributes': f"""
            booleanAttributes(offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                booleanValue
            }}
        """,
        'outgoingRelations': f"""
            outgoingRelations(offset: {nested_offset}, limit: {nested_limit}) {{
                target {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
                type {{
                    role
                }}
            }}
        """,
        'incomingRelations': f"""
            incomingRelations(offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    corole
                }}
                source {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
            }}
        """,
        'responsibilities': f"""
            responsibilities(offset: {nested_offset}, limit: {nested_limit}) {{
                role {{
                    name
                }}
                user {{
                    fullName
                    email
                }}
            }}
        """
    }

    if field_name not in field_queries:
        raise ValueError(f"Unsupported field name: {field_name}")

    # Construct the complete query
    complete_query = base_query + field_queries[field_name] + "}}"

    return complete_query