columns = {
    "change_requests": {
        "transform_json": [
            "assignment_group",
            "business_service",
            "cab_delegate",
            "closed_by",
            "cmdb_ci",
            "opened_by",
            "requested_by",
            "service_offering",
            "std_change_producer_version",
            "sys_domain",
            "u_change_implementer",
            "assigned_to"

        ],
        "add_seconds": [
            "business_duration",
            "cab_date_time",
            "calendar_duration",
            "time_worked",
        ],
        "explode_columns":[
            "u_site"
        ],
        "partition_columns":[
            "sys_created_on_dt"
        ],
        "process_timestamp": [
            "sys_created_on",
            "sys_updated_on",
            "approval_set",
            "end_date",
            "work_start",
            "start_date",
            "closed_at",
            "opened_at",
            "work_end",
            "cab_date_time",
            "conflict_last_run"
        ],
        "joining_key":[
            "u_site"
        ],
        "primary_key":[
            "number"
        ],
        "composite_key":[
            "u_site"
        ],
        "redact_pii_columns":[
            "comments_and_work_notes",
            "description",
            "comments"
        ],
        "unique_ticket_extract": ["Yes"]
    }
}

 

dataquality_rules = {
    "change_requests": """Rules = [
        ColumnCount = 129,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]"""

    }


appflow_names = {
    "change_requests": "UK-SNowFall-ServiceNow-ChangeRequest"
    }