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
        ]"""


}