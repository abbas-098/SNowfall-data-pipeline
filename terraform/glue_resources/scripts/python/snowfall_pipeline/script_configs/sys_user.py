columns = {
    "sys-users":{
        "transform_json":[
            "building",
            "sys_domain",
            "cost_center",
            "company",
            "department",
            "location"
        ],
        "process_timestamp": [
            "last_login_time",
            "sys_updated_on",
            "sys_created_on"
        ],
        "partition_columns":[
            "sys_created_on_dt"
        ],
        "primary_key":[
            "sys_id"
        ],
        "joining_key":["location_display_value"]
    }
}

 

dataquality_rules = {
    "sys-users": """Rules = [
        ColumnCount = 61,
        RowCount > 0,
        IsComplete "sys_id",
        IsComplete "sys_created_on"
    ]"""
    }


appflow_names = {
    "sys-users": "UK-SNowFall-ServiceNow-SysUser" 
    }