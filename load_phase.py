import psycopg2
import boto3
from datetime import timedelta, date

class DataLoader:
    def __init__(self, logger, REDSHIFT_PARAMS, S3_BUCKET_NAME, S3_PARTITION_PREFIX, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION_NAME, MSG_TEXT, ETL_JOB_NAME, REDSHIFT_TABLE, DELIVERIES_ATTEMPTS_COLUMNS):
        # Store all config and credentials needed for loading
        self.logger = logger
        self.redshift_params = REDSHIFT_PARAMS
        self.s3_bucket_name = S3_BUCKET_NAME
        self.s3_partition_prefix = S3_PARTITION_PREFIX
        self.aws_access_key_id = AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = AWS_SECRET_ACCESS_KEY
        self.region_name = REGION_NAME
        self.msg_text = MSG_TEXT
        self.etl_job_name = ETL_JOB_NAME
        self.redshift_table = REDSHIFT_TABLE
        self.deliveries_attempts_columns = DELIVERIES_ATTEMPTS_COLUMNS
        self.s3_client = None

    def update_latest_updated_at(self, job_name, last_updated_at):
        # Update the ETL job metadata in Redshift with the latest processed date
        self.logger.info(f"Updating last_updated_at for job: {job_name} to {last_updated_at}")
        try:
            conn = psycopg2.connect(**self.redshift_params)
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE interns.etl_job_metadata
                SET last_updated_at = %s
                WHERE job_name = %s
                """,
                (last_updated_at, job_name),
            )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"{self.msg_text}: update last_updated_at Error: {str(e)}")
            raise

    def copy_from_s3_to_redshift(self, s3_object_key):
        # Use Redshift's COPY command to load the CSV from S3 into the target table
        self.logger.info("Copying data from S3 to Redshift")
        try:
            column_list_str = ', '.join(self.deliveries_attempts_columns)
            conn = psycopg2.connect(**self.redshift_params)
            cur = conn.cursor()
            copy_query = f"""
            COPY {self.redshift_table} ({column_list_str})
            FROM 's3://{self.s3_bucket_name}/{s3_object_key}'
            ACCESS_KEY_ID '{self.aws_access_key_id}'
            SECRET_ACCESS_KEY '{self.aws_secret_access_key}'
            CSV
            IGNOREHEADER 1
            """
            cur.execute(copy_query)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"{self.msg_text}: Redshift copy Error: {str(e)}")
            raise

    def cleanup_s3(self, s3_object_key):
        # Delete the processed file from S3 to keep the bucket clean
        self.logger.info("Cleaning up S3")
        try:
            if self.s3_client is None:
                self.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name,
                )
            self.s3_client.delete_object(Bucket=self.s3_bucket_name, Key=s3_object_key)
        except Exception as e:
            self.logger.error(f"{self.msg_text}: S3 cleanup Error: {str(e)}")
            raise
        
    def delete_duplicates_from_redshift(self):
        # Remove duplicate records from the Redshift table if needed
        self.logger.info("Deleting duplicates from Redshift")
        try:
            conn = psycopg2.connect(**self.redshift_params)
            cur = conn.cursor()

            delete_query = f"""
            DELETE FROM {self.redshift_table}
            WHERE (id, updatedAt) NOT IN (
                SELECT id, MAX(updatedAt)
                FROM {self.redshift_table}
                GROUP BY id
            );
            """
            cur.execute(delete_query)
            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            self.logger.error(f"{self.msg_text}: Redshift delete duplicates Error: {str(e)}")
            raise

    def run_loading(self, last_updated_at, s3_object_key):
        # Main entry point for the loading phase
        self.logger.info("Starting load phase")
        try:
            self.update_latest_updated_at(self.etl_job_name, last_updated_at)
            self.copy_from_s3_to_redshift(s3_object_key)
            self.cleanup_s3(s3_object_key)
            self.delete_duplicates_from_redshift()
        except Exception as e:
            self.logger.error(f"{self.msg_text}: Load phase failed: {str(e)}")
            raise