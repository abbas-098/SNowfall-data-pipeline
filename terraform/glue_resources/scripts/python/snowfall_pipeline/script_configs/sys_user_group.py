columns = {
    "sys-users-group":{
        "transform_json":[
            "manager"
        ],
        "process_timestamp": [
            "sys_updated_on",
            "sys_created_on"
        ],
        "partition_columns":[
            "sys_created_on_dt"
        ],
        "primary_key":[
            "sys_id"
        ]
    }
}

 

dataquality_rules = {

    "sys-users-group": """Rules = [
        ColumnCount = 19,
        RowCount > 0,
        IsComplete "sys_id",
        IsComplete "sys_created_on"
    ]"""
    }


appflow_names = {
    "sys-users-group": "UK-SNowFall-ServiceNow-SysUserGroup",
    }