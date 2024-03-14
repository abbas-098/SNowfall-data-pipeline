columns = {
    "service-offering":{
        "transform_json":[
            "parent",
            "owned_by",
            "managed_by",
            "sys_domain",
            "duplicate_of"
        ],
        "process_timestamp": [
            "sys_updated_on",
            "sys_created_on"
        ],
        "partition_columns":[
            "sys_created_on_dt"
        ],
        "primary_key":[
            "number"
        ]
    }
}

 

dataquality_rules = {
    "service-offering": """Rules = [
        ColumnCount = 120,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]"""
    }


appflow_names = {
    "service-offering": "UK-SNowFall-ServiceNow-ServiceOffering"
    }