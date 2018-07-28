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

def map_master_columns(master_df, file_type):
    if file_type == "calendar":
        master_df.columns = [
            'YYYYMMDD',
            'YYYY-MM-DD',
            'YYYY',
            'Financial_Year',
            'Month_num_2y',
            'Month',
            'YYYYMM',
            'YYYYQQ',
            'Financial_Quarter',
            'unknown',
            'unknown_2',
            'Day_Type',
            'Day_Type_2',
            'Day_Type_Hol',
            'Day_of_Week_Num',
            'Day_of_Week',
            'YYYY2M',
            'unknown_3',
            'MM',
            'HHMM',
            'Week_Ending',
            'Quarter'
        ]
        master_df['YYYYQQ'] = master_df['YYYYQQ'].map(str)

    if file_type == "stop_locations":
        master_df.columns = [
            'StopID',
            'street_name',
            'street_corner',
            'location_type',
            'suburb',
            'postcode',
            'city',
            'region',
            'region_type',
            'lat',
            'lng'
        ]
    if file_type == "card_types":
        master_df.columns = [
            'Card_Type_ID',
            'Card_Description',
            'Card_Payment_Type',
            'Card_Type',
            'Card_Type_Detail',
            'Card_Type_Description'
        ]

    return master_df
