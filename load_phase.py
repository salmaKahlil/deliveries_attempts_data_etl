import psycopg2
import boto3
import io
from datetime import timedelta, date
from config import *


class DataLoader:
    def __init__(self):
        self.s3_client = None

    def update_latest_updated_at(self, job_name, last_updated_at):
        logger.info(
            f"Updating last_updated_at for job: {job_name} to {last_updated_at}"
        )
        try:
            conn = psycopg2.connect(**REDSHIFT_PARAMS)
            cursor = conn.cursor()

            # Update ETL metadata table with latest processed timestamp
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
            logger.info(
                f"Updated last_updated_at for job: {job_name} to {last_updated_at}"
            )

        except Exception as e:
            error_message = (
                f"{MSG_TEXT}: deliveryAttempts, update last_updated_at Error: {str(e)}"
            )
            logger.error(error_message)
            raise

    def upload_to_s3(self, insert_df):
        logger.info("Uploading data to S3")
        try:
            # Initialize S3 client with credentials
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME,
            )

            # Generate date-based filename for S3 object
            yesterday_date = date.today() - timedelta(days=1)

            # Convert DataFrame to CSV in memory buffer
            csv_buffer = io.StringIO()
            insert_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload the CSV to S3 with date-partitioned key
            s3_object_key = (
                f"{S3_PARTITION_PREFIX}{yesterday_date.strftime('%Y-%m-%d')}.csv"
            )
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_object_key,
                Body=csv_buffer.getvalue(),
            )

            logger.info(f"Data uploaded to S3 at {s3_object_key}")
            return s3_object_key

        except Exception as e:
            error_message = f"{MSG_TEXT}: deliveryAttempts, S3 upload Error: {str(e)}"
            logger.error(error_message)
            raise

    def copy_from_s3_to_redshift(self, s3_object_key):
        logger.info("Copying data from S3 to Redshift")
        try:
            conn = psycopg2.connect(**REDSHIFT_PARAMS)
            cur = conn.cursor()

            # Use Redshift COPY command to bulk load data from S3
            copy_query = f"""
            COPY {REDSHIFT_TABLE}
            FROM 's3://{S3_BUCKET_NAME}/{s3_object_key}'
            ACCESS_KEY_ID '{AWS_ACCESS_KEY_ID}'
            SECRET_ACCESS_KEY '{AWS_SECRET_ACCESS_KEY}'
            CSV
            IGNOREHEADER 1;
            """

            cur.execute(copy_query)
            conn.commit()
            cur.close()
            conn.close()

            logger.info("Data copied from S3 to Redshift successfully.")

        except Exception as e:
            error_message = (
                f"{MSG_TEXT}: deliveryAttempts, Redshift copy Error: {str(e)}"
            )
            logger.error(error_message)
            raise

    def cleanup_s3(self, s3_object_key):
        logger.info("Cleaning up S3")
        try:
            # Initialize S3 client if not already done
            if self.s3_client is None:
                self.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=REGION_NAME,
                )

            # Delete temporary CSV file from S3
            self.s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_object_key)
            logger.info("S3 file cleaned up successfully.")

        except Exception as e:
            error_message = f"{MSG_TEXT}: deliveryAttempts, S3 cleanup Error: {str(e)}"
            logger.error(error_message)
            raise

    def delete_duplicates_from_redshift(self, target_date):
        logger.info("Deleting duplicates from Redshift")
        try:
            conn = psycopg2.connect(**REDSHIFT_PARAMS)
            cur = conn.cursor()

            # Remove duplicate records, keeping only the most recent version of each ID
            delete_query = f"""
            DELETE FROM {REDSHIFT_TABLE}
            WHERE (id, updatedAt) NOT IN (
                SELECT id, MAX(updatedAt)
                FROM {REDSHIFT_TABLE}
                GROUP BY id
            );
            """
            cur.execute(delete_query)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Duplicates deleted successfully from Redshift.")

        except Exception as e:
            error_message = f"{MSG_TEXT}: deliveryAttempts, Redshift delete duplicates Error: {str(e)}"
            logger.error(error_message)
            raise

    def run_loading(self, insert_df, target_date, last_updated_at):
        logger.info("Starting load phase")
        try:
            # Update ETL metadata with latest processed timestamp
            self.update_latest_updated_at(ETL_JOB_NAME, last_updated_at)

            # Upload transformed data to S3 as staging area
            s3_object_key = self.upload_to_s3(insert_df)

            # Bulk load data from S3 to Redshift
            self.copy_from_s3_to_redshift(s3_object_key)

            # Remove temporary S3 file
            self.cleanup_s3(s3_object_key)

            # Clean up duplicate records in Redshift
            self.delete_duplicates_from_redshift(last_updated_at)

            logger.info("Data updated successfully")

        except Exception as e:
            error_message = f"{MSG_TEXT}: Load phase failed: {str(e)}"
            logger.error(error_message)
            raise


if __name__ == "__main__":
    from extract_phase import DataExtractor
    from transform_phase import DataTransformer

    # Test the complete ETL pipeline
    extractor = DataExtractor()
    cursor, mongo_df, target_date = extractor.run_extraction()

    transformer = DataTransformer()
    insert_df, last_updated_at = transformer.run_transformation(cursor)

    loader = DataLoader()
    loader.run_loading(insert_df, target_date, last_updated_at)
