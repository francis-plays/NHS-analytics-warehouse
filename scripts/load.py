import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from io import StringIO

from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_BUCKET_NAME,
    AWS_REGION,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_WAREHOUSE
)

def connect_to_s3():
    """Step 3: Connect to S3"""
    print("Connecting to S3...")
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    print("Connected to S3.")
    return s3

def get_csv_files(s3):
    """Step 4: List all CSV files in raw/ folder"""
    print("Listing CSV files in raw/ folder...")
    response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix="raw/")
    
    csv_files = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            csv_files.append(key)
    
    print(f"Found {len(csv_files)} CSV files:")
    for f in csv_files:
        print(f"  - {f}")
    
    return csv_files

def download_and_clean_csv(s3, file_key):
    """Steps 5-6: Download CSV, select columns, clean data"""
    print(f"\nProcessing {file_key}...")
    
    # Download CSV from S3
    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=file_key)
    df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
    
    print(f"  Loaded {len(df)} rows")
    
    # Select only the columns we need and rename them
    df_clean = df[[
        "Period",
        "Org Code",
        "Parent Org",
        "Org name",
        "A&E attendances Type 1",
        "Attendances over 4hrs Type 1"
    ]].copy()
    

    # Rename columns to match Snowflake table (UPPERCASE to match Snowflake)
    df_clean.columns = [
        "PERIOD",
        "ORG_CODE",
        "PARENT_ORG",
        "ORG_NAME",
        "TYPE1_ATTENDANCES",
        "TYPE1_OVER_4HRS"
    ]
    
    # Remove TOTAL row (only check ORG_CODE)
    df_clean = df_clean[df_clean["ORG_CODE"] != "TOTAL"]

    # Also remove any rows where PERIOD contains the word TOTAL
    df_clean = df_clean[~df_clean["PERIOD"].str.contains("TOTAL", case=False, na=False)]
    
    # Fill null values with 0
    
    df_clean["TYPE1_ATTENDANCES"] = df_clean["TYPE1_ATTENDANCES"].fillna(0).astype(int)
    df_clean["TYPE1_OVER_4HRS"] = df_clean["TYPE1_OVER_4HRS"].fillna(0).astype(int)


    print(f"  Cleaned to {len(df_clean)} rows (removed TOTAL row)")
    
    return df_clean

def combine_dataframes(dataframes):
    """Step 7: Combine all monthly DataFrames into one"""
    print("\nCombining all DataFrames...")
    combined = pd.concat(dataframes, ignore_index=True)
    print(f"Combined DataFrame has {len(combined)} total rows")
    return combined

def connect_to_snowflake():
    """Step 8: Connect to Snowflake"""
    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE
    )
    print("Connected to Snowflake.")
    return conn

def load_to_snowflake(conn, df):
    """Step 9: Load DataFrame to Snowflake"""
    print("\nLoading data to Snowflake...")
    
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="AE_ATTENDANCE_RAW",
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    
    if success:
        print(f"Successfully loaded {nrows} rows to Snowflake!")
    else:
        print("Load failed.")
    
    return success

def run():
    """Main execution flow"""
    # Step 3: Connect to S3
    s3 = connect_to_s3()
    
    # Step 4: Get list of CSV files
    csv_files = get_csv_files(s3)
    
    # Steps 5-6: Download and clean each CSV
    cleaned_dataframes = []
    for file_key in csv_files:
        df_clean = download_and_clean_csv(s3, file_key)
        cleaned_dataframes.append(df_clean)
    
    # Step 7: Combine all DataFrames
    combined_df = combine_dataframes(cleaned_dataframes)
    
    # Step 8: Connect to Snowflake
    conn = connect_to_snowflake()
    
    # Step 9: Load to Snowflake
    load_to_snowflake(conn, combined_df)
    
    # Step 10: Close connection
    conn.close()
    print("\nConnection closed. Load complete!")

if __name__ == "__main__":
    run()