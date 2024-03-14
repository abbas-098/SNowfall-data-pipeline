columns = {
    "problem":{
        "transform_json":[
            "opened_by",
            "sys_domain",
            "u_franchisee",
            "business_service",
            "assignment_group",
            "service_offering",
            "u_related_kb",
            "assigned_to",
            "u_franchisees_consultant",
            "location"
        ],
        "process_timestamp": [
            "sys_updated_on",
            "sys_created_on",
            "opened_at"
        ],
        "redact_pii_columns": [
            "work_notes",
            "description",
            "comments",
            "comments_and_work_notes",
            "u_string_1"
        ],
        "partition_columns":[
            "sys_created_on_dt"
        ],
        "primary_key":[
            "number"
        ],
        "unique_ticket_extract": ["Yes"],
        "joining_key":["location_display_value"]
    }
}

 

dataquality_rules = {
    "problem": """Rules = [
        ColumnCount = 91,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]"""    
    }


appflow_names = {
    "problem": "UK-SNowFall-ServiceNow-ProblemRecord",
    }