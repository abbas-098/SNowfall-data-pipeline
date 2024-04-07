dq_rules = {

    "location": """Rules = [
        ColumnCount = 76,
        RowCount > 0,
        IsComplete "sys_created_on"
        ]""",

    "location_hierarchy":"""Rules = [
        ColumnCount = 76,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "STORE_NAME"
        ]""",

    "incidents":"""Rules = [
        ColumnCount = 176,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
        ]""",

    "amazon_connect": """Rules = [
        ColumnCount = 86,
        RowCount > 0,
        IsComplete "queue",
        IsComplete "file_upload_date"
    ]""" ,

    "adj_trading_hours": """Rules = [
        ColumnCount = 9,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "CHANNEL"
    ]""",

    "change_request": """Rules = [
        ColumnCount = 131,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""",

    "problem_record": """Rules = [
        ColumnCount = 92,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""" ,

    "service_offering": """Rules = [
        ColumnCount = 120,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""",

    "service_request": """Rules = [
        ColumnCount = 88,
        RowCount > 0,
        IsComplete "number",
        IsComplete "sys_created_on"
    ]""",

    "sys_user_group": """Rules = [
        ColumnCount = 19,
        RowCount > 0,
        IsComplete "sys_id",
        IsComplete "sys_created_on"
    ]""",

    "sys_user": """Rules = [
        ColumnCount = 62,
        RowCount > 0,
        IsComplete "sys_id",
        IsComplete "sys_created_on"
    ]""",
    
    "trading_hours": """Rules = [
        ColumnCount = 9,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "CHANNEL"
    ]"""
}