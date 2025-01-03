def get_main_assets_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            fullName
            displayName
            modifiedOn
            modifiedBy {{ 
                fullName
            }}
            createdOn
            createdBy {{
                fullName
            }}
            status {{
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
        }}
    }}
    """

def get_string_attributes_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            stringAttributes(limit: $limit) {{
                type {{
                    name
                }}
                stringValue
            }}
        }}
    }}
    """

def get_multi_value_attributes_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            multiValueAttributes(limit: $limit) {{
                type {{
                    name
                }}
                stringValues
            }}
        }}
    }}
    """

def get_numeric_attributes_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            numericAttributes(limit: $limit) {{
                type {{
                    name
                }}
                numericValue
            }}
        }}
    }}
    """

def get_boolean_attributes_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            booleanAttributes(limit: $limit) {{
                type {{
                    name
                }}
                booleanValue
            }}
        }}
    }}
    """

def get_outgoing_relations_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            outgoingRelations(limit: $limit) {{
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
        }}
    }}
    """

def get_incoming_relations_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            incomingRelations(limit: $limit) {{
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
        }}
    }}
    """

def get_responsibilities_query(asset_type_id, paginate, limit):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }}, id: {{gt: {paginate}}} }}
            limit: $limit
        ) {{
            id
            responsibilities(limit: $limit) {{
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

QUERY_TYPES = {
    'main': get_main_assets_query,
    'string': get_string_attributes_query,
    'multi': get_multi_value_attributes_query,
    'numeric': get_numeric_attributes_query,
    'boolean': get_boolean_attributes_query,
    'outgoing': get_outgoing_relations_query,
    'incoming': get_incoming_relations_query,
    'responsibilities': get_responsibilities_query
}