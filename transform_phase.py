import pandas as pd
import numpy as np
import pytz
from flatten_json import flatten
import io
import boto3
from datetime import date, timedelta

class DataTransformer:
    def __init__(self, EGYPT_TZ, COLUMNS_TO_SELECT, MSG_TEXT, logger, DATA_TYPES, aws_access_key_id, aws_secret_access_key, s3_bucket_name, s3_partition_prefix, REGION_NAME):
        # Store all config and credentials needed for transformation
        self.egypt_tz = EGYPT_TZ
        self.columns_to_select = COLUMNS_TO_SELECT
        self.msg_text = MSG_TEXT
        self.logger = logger
        self.data_types = DATA_TYPES
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_bucket_name = s3_bucket_name
        self.s3_partition_prefix = s3_partition_prefix
        self.region_name = REGION_NAME

    def download_from_s3(self):
        # Download the raw extracted data from S3
        self.logger.info("Downloading data from S3")
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
            csv_buffer = io.BytesIO()
            s3.download_fileobj(self.s3_bucket_name, "data/delivery_attempts.csv", csv_buffer)
            csv_buffer.seek(0)
            self.logger.info(f"Downloaded data size: {csv_buffer.getbuffer().nbytes} bytes")
            return csv_buffer
        except Exception as e:
            self.logger.error(f"{self.msg_text}: Error downloading from S3: {e}")
            raise

    def flatten_mongo_data(self, csv_buffer):
        # Flatten nested MongoDB records into a flat DataFrame
        self.logger.info("Flattening MongoDB data")
        try:
            csv_buffer.seek(0)
            df_csv = pd.read_csv(csv_buffer)
            dict_flattened = (flatten(record, ".") for record in df_csv.to_dict(orient="records"))
            df = pd.DataFrame(dict_flattened)
            self.logger.info(f"Flattened {len(df)} records from MongoDB")
            return df
        except Exception as flatten_error:
            error_message = f"{self.msg_text}: deliveryAttempts, flatten Error: {str(flatten_error)}"
            self.logger.error(error_message)
            raise

    def select_required_columns(self, df):
        # Only keep the columns we care about for downstream
        self.logger.info("Selecting required columns from DataFrame")
        try:
            columns_to_select = self.columns_to_select
            existing_columns = [col for col in columns_to_select if col in df.columns]
            self.logger.info(f"Selected columns: {existing_columns}")
            df_selected = df[existing_columns].copy()
            return df_selected
        except Exception as selection_error:
            error_message = f"{self.msg_text}: deliveryAttempts, column selection Error: {str(selection_error)}"
            self.logger.error(error_message)
            raise

    def clean_column_names(self, df_selected):
        # Replace dots in column names with underscores for compatibility
        self.logger.info("Cleaning column names")
        try:
            df_selected.columns = [col.replace(".", "_") for col in df_selected.columns]
            df_selected.columns = df_selected.columns.str.replace("__", "_")
            self.logger.info("Column names cleaned successfully")
            return df_selected
        except Exception as clean_error:
            error_message = f"{self.msg_text}: deliveryAttempts, column name cleaning Error: {str(clean_error)}"
            self.logger.error(error_message)
            raise

    def rename_columns_to_standard_format(self, df):
        # Rename columns to match our Redshift schema
        self.logger.info("Renaming columns to standard format")
        try:
            df = df.rename(
                columns={
                    "_id": "id",
                    "deliveryId": "delivery_id",
                    "business_id": "business_id",
                    "type": "attempt_type",
                    "attemptDate": "date",
                    "routeId": "route_id",
                    "exception_time": "exception_at",
                    "exception_whatsAppVerification_conversationStatus_conversationStartedSuccessfully": "conversationStartedSuccessfully",
                    "exception_whatsAppVerification_conversationStatus_time": "exception_conversationStatus_time",
                }
            )
            self.logger.info("Columns renamed successfully")
            return df
        except Exception as manipulation_error:
            error_message = f"{self.msg_text}: deliveryAttempts, column renaming Error: {str(manipulation_error)}"
            self.logger.error(error_message)
            raise

    def handle_initial_boolean_columns(self, df_selected):
        # Fill missing boolean columns with False before type conversion
        self.logger.info("Handling initial boolean columns")
        try:
            if "exception_whatsAppVerification_fakeAttempt" in df_selected.columns:
                df_selected["exception_whatsAppVerification_fakeAttempt"] = df_selected[
                    "exception_whatsAppVerification_fakeAttempt"
                ].fillna(False)
            if "conversationStartedSuccessfully" in df_selected.columns:
                df_selected["conversationStartedSuccessfully"] = df_selected[
                    "conversationStartedSuccessfully"
                ].fillna(False)
            self.logger.info("Initial boolean columns handled successfully")
            return df_selected
        except Exception as boolean_error:
            error_message = f"{self.msg_text}: deliveryAttempts, initial boolean handling Error: {str(boolean_error)}"
            self.logger.error(error_message)
            raise

    def apply_data_types_and_handle_missing_columns(self, df_selected):
        # Make sure all columns exist and have the right types
        self.logger.info("Applying data types and handling missing columns")
        try:
            data_types = self.data_types
            for column_name in data_types.keys():
                if column_name not in df_selected.columns:
                    df_selected[column_name] = ""
            int_float_columns = [col for col, dtype in data_types.items() if dtype in ["int", "int64"]]
            df_selected[int_float_columns] = df_selected[int_float_columns].fillna(0)
            df_selected = df_selected.astype(data_types)
            self.logger.info("Data types applied successfully")
            return df_selected
        except Exception as manipulation_errors:
            error_message = f"{self.msg_text}: deliveryAttempts, data type application Error: {str(manipulation_errors)}"
            self.logger.error(error_message)
            raise

    def clean_string_columns_and_handle_nan_values(self, df_selected):
        # Replace NaN and similar values in string columns with empty strings
        self.logger.info("Cleaning string columns and handling NaN values")
        try:
            data_types = self.data_types
            str_columns = [col for col, dtype in data_types.items() if dtype == "str"]
            for col in str_columns:
                if col in df_selected.columns:
                    df_selected[col].replace({np.nan: "", "nan": "", "NAN": "", "NaN": "", "NaT": ""}, inplace=True)
            self.logger.info("String columns cleaned successfully")
            return df_selected
        except Exception as clean_error:
            error_message = f"{self.MSG_TEXT}: deliveryAttempts, string cleaning Error: {str(clean_error)}"
            self.logger.error(error_message)
            raise

    def truncate_string_columns_to_limits(self, df_selected):
        # Truncate long string columns to fit Redshift limits
        self.logger.info("Truncating string columns to limits")
        try:
            columns_to_truncate = ["business_name", "star_name"]
            if any(col in df_selected.columns for col in columns_to_truncate):
                df_selected[columns_to_truncate] = df_selected[columns_to_truncate].apply(lambda x: x.str.slice(0, 300))
            if "exception_reason" in df_selected.columns:
                df_selected["exception_reason"] = df_selected["exception_reason"].str.strip().str.slice(0, 200)
            if "consignee_name" in df_selected.columns:
                df_selected["consignee_name"] = df_selected["consignee_name"].str.strip().str.slice(0, 150)
            self.logger.info("String columns truncated successfully")
            return df_selected
        except Exception as truncate_error:
            error_message = f"{self.msg_text}: deliveryAttempts, string truncation Error: {str(truncate_error)}"
            self.logger.error(error_message)
            raise

    def handle_final_boolean_column_processing(self, insert_df):
        # Final pass to ensure boolean columns are correct
        self.logger.info("Handling final boolean column processing")
        try:
            if "exception_whatsAppVerification_verified" in insert_df.columns:
                insert_df["exception_whatsAppVerification_verified"].replace("nan", np.nan, inplace=True)
                insert_df["exception_whatsAppVerification_verified"] = insert_df[
                    "exception_whatsAppVerification_verified"
                ].astype("bool", errors="ignore")
                insert_df["exception_whatsAppVerification_verified"].fillna(False, inplace=True)
            if "exception_whatsAppVerification_fakeAttempt" in insert_df.columns:
                insert_df["exception_whatsAppVerification_fakeAttempt"].replace("nan", np.nan, inplace=True)
                insert_df["exception_whatsAppVerification_fakeAttempt"] = insert_df[
                    "exception_whatsAppVerification_fakeAttempt"
                ].astype("bool", errors="ignore")
                insert_df["exception_whatsAppVerification_fakeAttempt"].fillna(False, inplace=True)
            self.logger.info("Final boolean columns processed successfully")
            return insert_df
        except Exception as boolean_error:
            error_message = f"{self.msg_text}: deliveryAttempts, final boolean processing Error: {str(boolean_error)}"
            self.logger.error(error_message)
            raise

    def handle_final_datetime_column_formatting(self, insert_df):
        # Format all datetime columns to the expected string format for Redshift
        self.logger.info("Handling final datetime column formatting")
        datetime_columns = [
            "exception_at",
            "exception_whatsAppVerification_time",
            "exception_conversationStatus_time",
            "consignee_rescheduleDate",
            "createdAt",
            "updatedAt",
            "date"
        ]
        for col in datetime_columns:
            if col in insert_df.columns:
                insert_df[col] = pd.to_datetime(insert_df[col], errors="coerce")
                insert_df[col] = pd.to_datetime(insert_df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info("Final datetime columns formatted successfully")
        return insert_df

    def upload_to_s3(self, final_transformed_data):
        # Upload the transformed data to S3 for loading into Redshift
        self.logger.info("Uploading data to S3")
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            )
            yesterday_date = date.today() - timedelta(days=1)
            csv_buffer = io.StringIO()
            final_transformed_data.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            s3_object_key = f"{self.s3_partition_prefix}{yesterday_date.strftime('%Y-%m-%d')}.csv"
            self.s3_client.put_object(
                Bucket=self.s3_bucket_name,
                Key=s3_object_key,
                Body=csv_buffer.getvalue(),
            )
            return s3_object_key
        except Exception as e:
            self.logger.error(f"{self.msg_text}: S3 upload Error: {str(e)}")
            raise

    def run_transformation(self):
        # Main entry point for the transformation phase
        self.logger.info("Starting transformation phase")
        try:
            data = self.download_from_s3()
            df_flattened = self.flatten_mongo_data(data)
            df_selected = self.select_required_columns(df_flattened)
            df_selected = self.clean_column_names(df_selected)
            df_selected = self.rename_columns_to_standard_format(df_selected)
            df_selected = self.handle_initial_boolean_columns(df_selected)
            df_selected = self.apply_data_types_and_handle_missing_columns(df_selected)
            df_selected = self.clean_string_columns_and_handle_nan_values(df_selected)
            df_selected = self.truncate_string_columns_to_limits(df_selected)
            insert_df = self.handle_final_boolean_column_processing(df_selected)
            final_transformed_data = self.handle_final_datetime_column_formatting(insert_df)
            s3_object_key = self.upload_to_s3(final_transformed_data)
            last_updated_at = final_transformed_data["updatedAt"].max() if "updatedAt" in final_transformed_data.columns else None
            self.logger.info("Transformation phase completed successfully")
            return last_updated_at, s3_object_key
        except Exception as e:
            error_message = f"{self.msg_text}: Transformation phase failed: {str(e)}"
            self.logger.error(error_message)
            raise
