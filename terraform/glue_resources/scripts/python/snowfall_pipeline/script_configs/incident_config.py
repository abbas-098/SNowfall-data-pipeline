columns = {
    "incidents": {
        "transform_json": [
            "u_franchisee_regional_manager",
            "u_franchisees_consultant",
            "u_franchisee_director",
            "cmdb_ci",
            "u_vendor",
            "u_connect_chat",
            "service_offering",
            "closed_by",
            "parent_incident",
            "reopened_by",
            "assigned_to",
            "u_reopen_detail",
            "u_chased_out_detail",
            "u_franchisee_ops_manager",
            "resolved_by",
            "opened_by",
            "sys_domain",
            "u_franchisee",
            "u_franchisees_chief_ops_manager",
            "u_strike_details",
            "business_service",
            "caller_id",
            "u_related_kb_article",
            "assignment_group",
            "u_last_assignment_group",
            "problem_id",
            "company",
            "u_chased_in_detail",
            "location",
            "rfc"
        ],
        "add_seconds": [
            "business_duration",
            "u_time_store_may_close",
            "u_time_dt_may_close",
            "u_time_store_ok2operate",
            "u_time_dt_down",
            "u_time_dt_ok2operate",
            "calendar_duration",
            "u_time_store_down",
        ],
        "process_timestamp": [
            "sys_updated_on",
            "u_vista_dispatched_eta",
            "u_vista_offsite",
            "u_vista_onsite",
            "u_call_back_time",
            "u_last_assignment_time",
            "u_techsee_agent_session_start_time",
            "u_l1_5_assignment_time",
            "u_customer_escalation_time",
            "sys_created_on",
            "closed_at",
            "opened_at",
            "work_end",
            "reopened_time",
            "resolved_at",
            "u_reopen_date_time",
        ],
        "redact_pii_columns": [
            "work_notes",
            "description",
            "close_notes",
            "comments",
            "u_chased_in_detail_display_value",
            "comments_and_work_notes",
        ],
        "split_restaurant_name_and_id":[
            "location_display_value"
        ],
        "partition_columns":[
            "sys_updated_on_dt",
            "sys_created_on_dt"
        ],
        "unique_ticket_extract":[
            "number",
            "sys_updated_on"
        ],
        "primary_key":[
            "number"
        ],
        "joining_key":[
            "location_display_value"
        ],
        "composite_key":["state"]
    }
}

 

dataquality_rules = {
    "incidents": """Rules = [
        ColumnCount = 176,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]"""
    }


appflow_names = {
    "incident_intraday": "UK-SNowFall-ServiceNow-Incident-Intraday",
    "incident_daily": "UK-SNowFall-ServiceNow-Incident-Daily"
    }
