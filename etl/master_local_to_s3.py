import pandas as pd
from datetime import datetime
import os
import gzip
import shutil
from tqdm import tqdm # want to see progress
import re
import boto3
from file_map import map_transaction_columns, map_master_columns
from io import BytesIO
import json

# init
session = boto3.Session(profile_name='admin')
s3 = session.client('s3',region_name='ap-southeast-2')

# relevant for my file directory.
#base_dir = 'F:\MelbDatathon2018\Samp_1\ScanOffTransaction'

# base_dirs = [
#     'F:\MelbDatathon2018\Samp_0\ScanOnTransaction',
#     'F:\MelbDatathon2018\Samp_0\ScanOnTransaction',
# ]
#QID3530815_20180713_20515_0.txt
#QID3530815_20180713_20515_0.txt
#=======
base_dir = '..\..\Data\Datathon\MelbDatathon2018'

#>>>>>>> 7b8b7a625de2c7cd5fb56b491679c633c77c29ff:etl/master_local_to_s3.py
# Functions

def Write_To_S3(s3, data, filename):
    """
        Helper function that writes data to S3
    """

    bytes_buff = BytesIO(data)
    s3.upload_fileobj(bytes_buff
                    , 'stevan-melbourne-datathon'
                    , filename)


def Extract_File(file_directory):
    """ This function takes a file directory from the datathon
        transactions and unzips it in its local directory
    """

    file = re.findall(
        pattern="(.*)\.txt",
        string=file_directory
    )[0]

    return file

def Transform_Data(file, file_type):
    """ After Extract_File has been run, This function loads the data
        in memory, runs some transformations and writes it into a
        parquet file
    """

    # read file - and transform data.
    df = pd.read_csv(file_directory, sep="|", header=None)
    df = map_master_columns(df, file_type)

    df.iloc[:,4:22]

    # send to parquet
    parquet_file = re.findall(
        pattern="(.*)\.txt",
        string=file
    )[0] + ".parquet"

    df.to_parquet(parquet_file)

    return parquet_file

def Load_File(s3, parquet_file):
    """ After Transform_Data has been run, this function takes the parquet
        file and loads it into S3, ready for our Athena Table.
    """
    # Load Data.
    with open(parquet_file,'rb') as f:
        data = f.read()

    # preparing the filename for S3 / Athena, doing some chained regex.

    s3_file = re.findall(
        r"(?<=MelbDatathon2018\\).*",
        parquet_file
    )[0].replace("\\","/")

    Write_To_S3(s3, data, s3_file)


def Clean_Files(file, parquet_file):
    """
        Just cleans up the files in the ETL procedure
    """
    # clean-up
    #os.remove(file)
    os.remove(parquet_file)


def Log_ETL(file_directory, message):
    with open("transaction_etl_log.json", "a") as log_file:
        log_file.write(
            json.dumps(
                {
                    "file_directory" : file_directory,
                    "message" : message
                }
            )
        )

s3


def File_ETL_Main(s3, file_directory, file_type):
    """
        Main ETL Code broken down into components
    """
    try:
        # ETL
        #file = Extract_File(file_directory)
        parquet_file = Transform_Data(file_directory, file_type)
        Load_File(s3, parquet_file)
        Clean_Files(file_directory, parquet_file)
        # Log Success
        Log_ETL(file_directory,'success')
    except Exception as e:
        Log_ETL(file_directory,str(e))



if __name__ == "__main__":
    file_directory = '..\..\Data\Datathon\MelbDatathon2018\calendar.txt'
    file_type = "calendar"
    File_ETL_Main(s3, base_dir, file_type)

                # file_directory
