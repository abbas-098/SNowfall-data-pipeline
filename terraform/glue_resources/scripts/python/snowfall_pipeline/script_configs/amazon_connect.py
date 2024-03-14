columns = {
    "amazon_connect":{
        "primary_key":[
            "queue"
        ],
        "composite_key":[
            "file_upload_date"
        ]

    }
}

dataquality_rules = {
    "amazon_connect": """Rules = [
        ColumnCount = 86,
        RowCount > 0,
        IsComplete "queue",
        IsComplete "file_upload_date"
    ]"""
    }
