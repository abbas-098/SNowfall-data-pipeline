dq_rules = {

    "location": """Rules = [
        ColumnCount <= 80,
        RowCount > 0,
        IsComplete "sys_created_on"
        ]""",

    "location_hierarchy":"""Rules = [
        ColumnCount <= 80,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "STORE_NAME"
        ]""",

    "incidents":"""Rules = [
        ColumnCount <= 181,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
        ]""",

    "amazon_connect": """Rules = [
        ColumnCount <= 90,
        RowCount > 0,
        IsComplete "queue",
        IsComplete "file_upload_date"
    ]""" ,

    "adj_trading_hours": """Rules = [
        ColumnCount <= 12,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "CHANNEL"
    ]""",

    "change_request": """Rules = [
        ColumnCount <= 133,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""",

    "problem_record": """Rules = [
        ColumnCount <= 95,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""" ,

    "service_offering": """Rules = [
        ColumnCount <= 125,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""",

    "service_request": """Rules = [
        ColumnCount <= 95,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""",

    "sys_user_group": """Rules = [
        ColumnCount <= 20,
        RowCount > 0,
        IsComplete "sys_id",
        IsComplete "sys_created_on"
    ]""",

    "sys_user": """Rules = [
        ColumnCount <= 65,
        RowCount > 0,
        IsComplete "sys_id",
        IsComplete "sys_created_on"
    ]""",
    
    "trading_hours": """Rules = [
        ColumnCount <= 10,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "CHANNEL"
    ]"""
}