import pymongo
import pandas as pd
import psycopg2
import boto3
import io
from config import *


class DataExtractor:
    def __init__(self):
        self.mongo_client = None
        self.mongo_collection = None

    def extract_last_updated_date(self, job_name):
        logger.info(f"Extracting last_updated_at for job: {job_name}")
        try:
            conn = psycopg2.connect(**REDSHIFT_PARAMS)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT last_updated_at 
                FROM  interns.etl_job_metadata
                WHERE job_name = %s
            """,
                (job_name,),
            )
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            logger.info(
                f"Last updated date for {job_name}: {result[0] if result else None}"
            )
            return result[0] if result else None
        except Exception as e:
            logger.error(f"[{job_name}] Error extracting last_updated_at: {str(e)}")
            raise

    def extract_mongo_data(self, last_updated_date):
        logger.info("Starting MongoDB data extraction")
        try:
            # Establish MongoDB connection
            self.mongo_client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
            mongo_db = self.mongo_client[MONGO_DATABASE]
            self.mongo_collection = mongo_db[MONGO_COLLECTION]

            # Query for records updated since last ETL run
            query = {"updatedAt": {"$gte": last_updated_date}}
            cursor = list(self.mongo_collection.find(query))

            df = pd.DataFrame(cursor)

            logger.info(f"Extracted {len(cursor)} records from MongoDB")
            return cursor, df

        except Exception as query_error:
            error_message = (
                f"{MSG_TEXT}: deliveryAttempts, mongo Query Error: {str(query_error)}"
            )
            logger.error(error_message)
            raise

    def from_mongo_to_s3(self, cursor):
        logger.info("Uploading DataFrame to S3")
        try:
            df = pd.DataFrame(cursor)

            # Convert DataFrame to CSV in memory buffer
            csv_buffer = icsv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            # Initialize S3 client and upload
            s3 = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )
            bucket_name = S3_BUCKET_NAME
            s3.upload_fileobj(csv_buffer, bucket_name, "data/delivery_attempts.csv")
            logger.info("DataFrame uploaded to S3 successfully")

        except Exception as e:
            error_message = f"{MSG_TEXT}: Error uploading data to S3: {str(e)}"
            print(error_message)
            raise

    def extract_s3_data(self):
        logger.info("Downloading data from S3")
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )

            # Download CSV from S3 into memory buffer
            csv_buffer = io.BytesIO()
            s3.download_fileobj(
                S3_BUCKET_NAME, "data/delivery_attempts.csv", csv_buffer
            )

            # Reset buffer position and read as DataFrame
            csv_buffer.seek(0)
            df = pd.read_csv(csv_buffer)

            logger.info(f"Successfully downloaded {len(df)} records from S3")
            return df

        except Exception as e:
            logger.error(f"{MSG_TEXT}: Error extracting data from S3: {str(e)}")
            raise

    def run_extraction(self):
        logger.info("Starting extraction phase")
        try:
            # Get the last updated date for the ETL job
            last_updated_date = self.extract_last_updated_date(ETL_JOB_NAME)

            # Extract new/updated records from MongoDB
            cursor, mongo_df = self.extract_mongo_data(last_updated_date)

            # Upload to S3 as intermediate storage
            self.from_mongo_to_s3(cursor)
            s3_df = self.extract_s3_data()

            logger.info("Extraction phase completed successfully")
            return cursor, s3_df, last_updated_date

        except Exception as e:
            error_message = f"{MSG_TEXT}: Extraction phase failed: {str(e)}"
            logger.error(error_message)
            raise


if __name__ == "__main__":
    extractor = DataExtractor()
    cursor,