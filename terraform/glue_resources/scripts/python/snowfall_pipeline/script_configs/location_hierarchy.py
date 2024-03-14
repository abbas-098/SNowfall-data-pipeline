columns = {
    "location_hierarchy":{
        "primary_key":[
            "STORE_NUMBER"
        ],
        "redact_pii_columns":[
            "OO_EMAIL",
            "FS_EMAIL"
        ],
        "composite_key":["STORE_NAME"]
    }
}

 

dataquality_rules = {
    "location_hierarchy": """Rules = [
        ColumnCount = 76,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "STORE_NAME"
    ]"""
}

