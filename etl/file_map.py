import pandas as pd

def map_transaction_columns(trans_df):
    trans_df.columns = [
        'Mode',
        'BusinessDate',
        'DateTime',
        'CardID',
        'Card_Type_ID',
        'VehicleID',
        'ParentRoute',
        'RouteID',
        'StopID'
    ]

    return trans_df
