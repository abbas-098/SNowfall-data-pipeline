dq_rules = {

    "location": """Rules = [
        ColumnCount = 75,
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
    ]""" 


}