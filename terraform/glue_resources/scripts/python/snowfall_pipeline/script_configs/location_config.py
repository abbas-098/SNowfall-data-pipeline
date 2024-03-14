columns = {
    "locations":{
        "transform_json":[
            "u_franchisee_regional_manager",
            "u_regional_manager",
            "u_fanchisee",
            "u_franchisees_chief_ops_manager",
            "u_franchisees_ops_manager",
            "u_director",
            "u_chief_ops_officer",
            "u_franchisees_consultant",
            "u_franchisees_director",
            "u_ops_manager"
        ],
        "process_timestamp": [
            "sys_updated_on",
            "sys_created_on"
        ],
        "split_restaurant_name_and_id":[
            "full_name"
        ],
        "partition_columns":[
            "sys_created_on_dt"
        ],
        "primary_key":[
            "full_name"
        ],
        "joining_key":[
            "full_name"
        ]
    }
}

dataquality_rules = {
    "locations": """Rules = [
        ColumnCount = 76,
        RowCount > 0,
        IsComplete "sys_created_on"
    ]"""
    }


appflow_names = {
    "locations":"UK-SNowFall-ServiceNow-Location"
    }