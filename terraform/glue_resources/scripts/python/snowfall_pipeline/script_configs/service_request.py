columns = {
    "service_requests":{
        "transform_json":[
            "requested_for",
            "opened_by",
            "sys_domain",
            "order_guide",
            "request",
            "assignment_group",
            "closed_by",
            "assigned_to",
            "cat_item",
            "business_service"
        ],
        "process_timestamp": [
            "sys_updated_on",
            "sys_created_on",
            "closed_at",
            "opened_at",
            "approval_set",
            "due_date"
        ],
        "redact_pii_columns": [
            "work_notes",
            "comments",
            "comments_and_work_notes"
        ],
        "partition_columns":[
            "sys_created_on_dt"
        ],
        "primary_key":[
            "number"
        ],
        "unique_ticket_extract":[
            "number",
            "sys_updated_on"
        ]
    }
}

 

dataquality_rules = {
    "service_requests": """Rules = [
        ColumnCount = 88,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]"""
    }


appflow_names = {
    "service_requests": "UK-SNowFall-ServiceNow-ServiceRequest"
    }