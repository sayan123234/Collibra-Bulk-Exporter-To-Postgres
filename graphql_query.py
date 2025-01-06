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