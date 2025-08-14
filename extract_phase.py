import psycopg2
import pymongo
import boto3
import io
import pandas as pd

class DataExtractor:
    def __init__(
        self,
        redshift_params,
        mongo_connection_string,
        mongo_database,
        mongo_collection,
        s3_bucket_name,
        aws_access_key_id,
        aws_secret_access_key,
        etl_job_name,
        logger,
        msg_text
    ):
        # Set up all the connections and config needed for extraction
        self.redshift_params = redshift_params
        self.mongo_connection_string = mongo_connection_string
        self.mongo_database = mongo_database
        self.mongo_collection = mongo_collection
        self.s3_bucket_name = s3_bucket_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.etl_job_name = etl_job_name
        self.logger = logger
        self.msg_text = msg_text

    def extract_last_updated_date(self):
        # Get the last time we updated this job from Redshift metadata
        self.logger.info(f"Extracting last_updated_at for job: {self.etl_job_name}")
        try:
            with psycopg2.connect(**self.redshift_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT last_updated_at 
                        FROM interns.etl_job_metadata
                        WHERE job_name = %s
                        """,
                        (self.etl_job_name,),
                    )
                    result = cursor.fetchone()
                    extracted_date = result[0] if result else None
                    self.logger.info(f"Last updated date: {extracted_date}")
                    return extracted_date
        except Exception as e:
            self.logger.error(f"Error extracting last_updated_at: {e}")
            raise

    def extract_mongo_data(self, last_updated_date):
        # Pull new or updated records from MongoDB since the last update
        self.logger.info("Starting MongoDB data extraction")
        try:
            client = pymongo.MongoClient(self.mongo_connection_string)
            db = client[self.mongo_database]
            collection = db[self.mongo_collection]
            query = {"updatedAt": {"$gte": last_updated_date}} if last_updated_date else {}
            cursor = list(collection.find(query))
            self.logger.info(f"Extracted {len(cursor)} records from MongoDB")
            return cursor
        except Exception as e:
            error_message = f"{self.msg_text}: MongoDB query error: {e}"
            self.logger.error(error_message)
            raise

    def upload_to_s3(self, data):
        # Save the extracted data as a CSV and upload to S3 for downstream processing
        self.logger.info("Uploading DataFrame to S3")
        try:
            df = pd.DataFrame(data)
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
            s3.upload_fileobj(csv_buffer, self.s3_bucket_name, "data/delivery_attempts.csv")
            self.logger.info("Upload to S3 successful")
        except Exception as e:
            error_message = f"{self.msg_text}: Error uploading to S3: {e}"
            self.logger.error(error_message)
            raise

    def run_extraction(self):
        # Main entry point for the extraction phase
        try:
            extracted_date = self.extract_last_updated_date()
            mongo_data = self.extract_mongo_data(extracted_date)
            self.upload_to_s3(mongo_data)
            self.logger.info("Extraction phase completed successfully")
        except Exception as e:
            self.logger.error(f"{self.msg_text}: Extraction failed: {e}")
            raise