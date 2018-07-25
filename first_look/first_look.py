# just in case we need to import data

# import gzip
# import shutil
# with gzip.open('QID3530478_20180713_14000_0.txt.gz', 'rb') as f_in:
#     with open('QID3530478_20180713_14000_0.txt', 'wb') as f_out:
#         shutil.copyfileobj(f_in, f_out)

import pandas as pd
import numpy as np

trans_df = pd.read_csv('QID3530478_20180713_14000_0.txt', sep='|')

trans_df.columns = [
    'Mode',
    'BusinessDate',
    'DateTime',
    'Card_Type_ID',
    'CardType',
    'VehicleID',
    'ParentRoute',
    'RouteID',
    'StopID'
]

len(trans_df)

stop_locations = pd.read_csv('stop_locations.txt', sep='|', header=None)

stop_locations.columns = [
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

# date transformations
calendar = pd.read_csv('calendar.txt', sep='|', header=None)

calendar.columns = [
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

# card types
card_types = pd.read_csv('card_types.txt', sep='|', header=None)

card_types.columns = [
    'Card_Type_ID',
    'Card_Description',
    'Card_Payment_Type',
    'Card_Type',
    'Card_Type_Detail',
    'Card_Type_Description'
]

card_types

trans_df['Mode'].unique()



trans_df


trans_df.groupby(
    'VehicleID',
    as_index=False
)['CardID'].\
count().\
sort_values(
    'CardID',
    ascending=False
)

# take a look at a specific vehicleID
trans_df[
    trans_df['VehicleID'] == 3074
].groupby(
    'StopID',
    as_index = False
)['CardID'].\
count()


stop_locations[
    stop_locations['StopID'] == 24310
]

df[
    df['VehicleID'] == 0
].sort_values(
    'DateTime'
)



df['VehicleID'].nunique()

df


card_types

calendar

stop_locations

stop_locations.columns = [
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

df

stop_locations['city'].unique()
