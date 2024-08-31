import pandas as pd
import os

start_date = pd.to_datetime("2018-01-01")
end_date = pd.to_datetime("2020-10-23")

dates = pd.date_range(start=start_date, end=end_date)

for date in dates:
    df = pd.DataFrame(data)  # Assuming 'data' is the relevant data for each date
    df.to_parquet(f"/path/to/your/folder/TSK_{date.strftime('%Y%m%d')}.parquet")
import shutil

for date in dates:
    year_month = date.strftime('%Y%m')
    os.makedirs(f"/path/to/DataLake/{year_month}", exist_ok=True)
    shutil.move(f"/path/to/your/folder/TSK_{date.strftime('%Y%m%d')}.parquet", f"/path/to/DataLake/{year_month}/")

def read_parquets_in_date_range(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date)
    parquet_files = []
    
    for date in date_range:
        year_month = date.strftime('%Y%m')
        parquet_files.append(f"/mnt/DataLake/{year_month}/TSK_{date.strftime('%Y%m%d')}.parquet")
    
    df = spark.read.parquet(*parquet_files)
    return df
