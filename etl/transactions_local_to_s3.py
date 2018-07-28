import pandas as pd
from datetime import datetime
import os
import gzip
import shutil
from tqdm import tqdm # want to see progress
import re
import boto3
from file_map import map_transaction_columns
from io import BytesIO
import json

# init
session = boto3.Session(profile_name='admin')
s3 = session.client('s3',region_name='ap-southeast-2')

# relevant for my file directory.
# base_dir = '..\..\Data\Datathon\MelbDatathon2018\Samp_1\ScanOnTransaction'

# relevant for my file directory.
#base_dir = 'F:\MelbDatathon2018\Samp_1\ScanOffTransaction'

base_dirs = [
    'F:\MelbDatathon2018\Samp_0\ScanOffTransaction',
    'F:\MelbDatathon2018\Samp_1\ScanOffTransaction',
]

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
        transactions and unzips it in its local directory. Returns key
        parameters used throughout the procedure.
    """

    with gzip.open(file_directory, 'rb') as f_in:
        file = re.findall(
            pattern="(.*)\.gz",
            string=file_directory
        )[0]
        with open(file, 'wb') as f_out:
            #extract file to disk.
            shutil.copyfileobj(f_in, f_out)

    scan_type = re.findall(
        pattern="Scan(On|Off)Transaction",
        string=file
    )[0]

    sample = re.findall(
        pattern="Samp_(\d)",
        string=file
    )[0]

    return file, scan_type, sample



def Transform_Data(file, scan_type):
    """ After Extract_File has been run, This function loads the data
        in memory, runs some transformations and writes it into a
        parquet file
    """

    # read file - and transform data.
    df = pd.read_csv(file, sep="|", header=None)
    df = map_transaction_columns(df)
    df['Scan_Type'] = scan_type

    # send to parquet
    parquet_file = re.findall(
        pattern="(.*)\.txt",
        string=file
    )[0] + ".parquet"

    df.to_parquet(parquet_file)

    return parquet_file

def Load_File(s3, parquet_file, year, week, sample):
    """ After Transform_Data has been run, this function takes the parquet
        file and loads it into S3, ready for our Athena Table.
    """
    # Load Data.
    with open(parquet_file,'rb') as f:
        data = f.read()

    # preparing the filename for S3 / Athena, doing some chained regex.
    s3_file = re.findall(
        r"Week\d*\\(.*)",
        parquet_file
    )[0].replace(".parquet","_%s.parquet" % str(sample))

    s3_file = "Transactions/Year=%i/Week=%i/%s" % (
        int(year),
        int(week.replace("Week","")),
        s3_file
    )

    Write_To_S3(s3, data, s3_file)


def Clean_Files(file, parquet_file):
    """
        Just cleans up the files in the ETL procedure
    """
    # clean-up
    os.remove(file)
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


def File_ETL_Main(s3, file_directory, year, week):
    """
        Main ETL Code broken down into components
    """
    try:
        # ETL
        file_directory
        file, scan_type, sample = Extract_File(file_directory)
        parquet_file = Transform_Data(file, scan_type)
        Load_File(s3, parquet_file, year, week, sample)
        Clean_Files(file, parquet_file)
        # Log Success
        Log_ETL(file_directory,'success')
    except Exception as e:
        Log_ETL(file_directory,str(e))


def Load_All_Transactions_To_S3(s3, base_dir):
    """
        Script to loop through all files in the directories
        and perform ETL task for each.
    """
    years = os.listdir(base_dir)
    for year in years:
        # DEBUG
        # year = years[-1]

        weeks = os.listdir(base_dir+r'\%s' % year)
        for week in tqdm(weeks):
            # DEBUG
            # week = weeks[-2]

            files = os.listdir(
                        base_dir+r'\%s\%s' % (
                            year,
                            week
                        )
            )

            for filename in files:
                    # DEBUG
                    # filename = files[0]
                    file_directory = base_dir+r'\%s\%s\%s' % (
                        year,
                        week,
                        filename
                    )

                    File_ETL_Main(s3, file_directory, year, week)


if __name__ == "__main__":
    for base_dir in base_dirs:
        Load_All_Transactions_To_S3(s3, base_dir)

                # file_directory
