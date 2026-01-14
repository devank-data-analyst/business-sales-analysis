import logging
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime


# ----- Logging Setup -----
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
    )

# ----- AWS S3 Setup -----
s3 = boto3.client('s3')
BUCKET_NAME = 'devank-data-projects'
FILE_NAME = 'Sample - Superstore.csv'

# ----- Data Pipeline Functions -----
def load_data_from_s3(bucket, key):
    try:
        logging.info(f"Loading the data from S3: {bucket}/{key}")
        response = s3.get_object(Bucket = bucket, Key = key)
        data = response['Body'].read().decode('latin1')
        df = pd.read_csv(StringIO(data))
        logging.info("Data loaded successfully from S3")
        return df
    except Exception as e:
        logging.error(f"Error loading the data from S3: {e}")
        raise


def transform_data(df):
    try:
        logging.info("Starting data transformation")
        processed_df = df.groupby('Category').agg({
            'Sales': 'sum', 'Profit': 'sum', 'Discount': 'mean'
        }).reset_index()
        logging.info("Data transformation completed")
        return processed_df
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise


def save_data_to_s3(df, bucket):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        key = f"processed/processed_superstore_{timestamp}.csv"

        logging.info(f"Uploading processed data to S3 as {key}")

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket, Key=key, Body = csv_buffer.getvalue()
        )
        logging.info("Upload successful")
        return key
    
    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")
        raise

# ----- Main Execution -----
if __name__ == "__main__":
    try:
        df =  load_data_from_s3(BUCKET_NAME, FILE_NAME)
        processed_df = transform_data(df)
        key = save_data_to_s3(processed_df, BUCKET_NAME)
        logging.info(f"Pipeline executed successfully. File saved as {key}")
    except Exception as e:
        logging.critical(f"Pipeline Failed: {e}")
        print(f"Pipeline Failed! as {key}")