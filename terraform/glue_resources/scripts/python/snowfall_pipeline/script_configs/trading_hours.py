columns = {
    "trading_hours":{
        "primary_key":[
            "STORE_NUMBER"
        ],
        "composite_key":[
            "CHANNEL"
        ]
    }
}

 

dataquality_rules = {
    "trading_hours": """Rules = [
        ColumnCount = 9,
        RowCount > 0,
        IsComplete "STORE_NUMBER",
        IsComplete "CHANNEL"
    ]"""
    }

