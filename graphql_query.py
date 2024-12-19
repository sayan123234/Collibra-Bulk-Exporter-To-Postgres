def get_query(asset_type_id, paginate):
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
            stringAttributes (limit: 50) {{
                type {{
                    name
                }}
                stringValue
            }}
            multiValueAttributes (limit: 50) {{
                type {{
                    name
                }}
                stringValues
            }}
            numericAttributes (limit: 50) {{
                type {{
                    name
                }}
                numericValue
            }}
            dateAttributes (limit: 50) {{
                type {{
                    name
                }}
                dateValue
            }}
            booleanAttributes (limit: 50) {{
                type {{
                    name
                }}
                booleanValue
            }}
            outgoingRelations (limit: 50) {{
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
            incomingRelations (limit: 50) {{
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
            responsibilities (limit: 50) {{
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
