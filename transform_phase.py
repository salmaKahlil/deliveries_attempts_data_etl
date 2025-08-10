import pandas as pd
import numpy as np
import pytz
from flatten_json import flatten
from config import *


class DataTransformer:
    def __init__(self):
        self.egypt_tz = EGYPT_TZ

    def flatten_mongo_data(self, cursor):
        logger.info("Flattening MongoDB data")
        try:
            # Convert nested JSON documents to flat structure
            dict_flattened = (flatten(record, ".") for record in cursor)
            df = pd.DataFrame(dict_flattened)
            logger.info(f"Flattened {len(df)} records from MongoDB")
            return df
        except Exception as flatten_error:
            error_message = (
                f"{MSG_TEXT}: deliveryAttempts, flatten Error: {str(flatten_error)}"
            )
            logger.error(error_message)
            raise

    def select_required_columns(self, df):
        logger.info("Selecting required columns from DataFrame")
        try:
            # Define columns needed for the warehouse table
            columns_to_select = COLUMNS_TO_SELECT

            # Select only columns that exist in the DataFrame
            existing_columns = [col for col in columns_to_select if col in df.columns]
            logger.info(f"Selected columns: {existing_columns}")
            df_selected = df[existing_columns].copy()

            return df_selected

        except Exception as selection_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, column selection Error: {str(selection_error)}"
            logger.error(error_message)
            raise

    def clean_column_names(self, df_selected):
        logger.info("Cleaning column names")
        try:
            # Standardize column names for database compatibility
            df_selected.columns = [col.replace(".", "_") for col in df_selected.columns]
            df_selected.columns = df_selected.columns.str.replace("__", "_")

            logger.info("Column names cleaned successfully")
            return df_selected

        except Exception as clean_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, column name cleaning Error: {str(clean_error)}"
            logger.error(error_message)
            raise

    def rename_columns_to_standard_format(self, df):
        logger.info("Renaming columns to standard format")
        try:
            # Map MongoDB field names to warehouse column names
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

            logger.info("Columns renamed successfully")
            return df

        except Exception as manipulation_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, column renaming Error: {str(manipulation_error)}"
            logger.error(error_message)
            raise

    def handle_initial_boolean_columns(self, df_selected):
        logger.info("Handling initial boolean columns")
        try:
            # Fill missing boolean values with False
            if "exception_whatsAppVerification_fakeAttempt" in df_selected.columns:
                df_selected["exception_whatsAppVerification_fakeAttempt"] = df_selected[
                    "exception_whatsAppVerification_fakeAttempt"
                ].fillna(False)

            if "conversationStartedSuccessfully" in df_selected.columns:
                df_selected["conversationStartedSuccessfully"] = df_selected[
                    "conversationStartedSuccessfully"
                ].fillna(False)

            logger.info("Initial boolean columns handled successfully")
            return df_selected

        except Exception as boolean_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, initial boolean handling Error: {str(boolean_error)}"
            logger.error(error_message)
            raise

    def apply_data_types_and_handle_missing_columns(self, df_selected):
        logger.info("Applying data types and handling missing columns")
        try:
            # Use predefined data types from config
            data_types = DATA_TYPES

            # Add missing columns with default empty values
            for column_name in data_types.keys():
                if column_name not in df_selected.columns:
                    df_selected[column_name] = ""

            # Handle numeric columns - replace NaN with 0 before type conversion
            int_float_columns = [
                col for col, dtype in data_types.items() if dtype in ["int", "int64"]
            ]
            df_selected[int_float_columns] = df_selected[int_float_columns].fillna(0)

            # Apply data type conversions
            df_selected = df_selected.astype(data_types)
            logger.info("Data types applied successfully")
            return df_selected

        except Exception as manipulation_errors:
            error_message = f"{MSG_TEXT}: deliveryAttempts, data type application Error: {str(manipulation_errors)}"
            logger.error(error_message)
            raise

    def localize_datetime_columns_to_utc(self, df_selected):
        logger.info("Localizing datetime columns to UTC")
        try:
            datetime_columns = [
                "createdAt",
                "updatedAt",
                "date",
                "exception_at",
                "exception_whatsAppVerification_time",
                "exception_conversationStatus_time",
                "consignee_rescheduleDate",
            ]

            # Convert to datetime and set UTC timezone
            for col in datetime_columns:
                if col in df_selected.columns:
                    df_selected[col] = pd.to_datetime(df_selected[col]).dt.tz_localize(
                        "UTC"
                    )

            logger.info("Datetime columns localized to UTC successfully")
            return df_selected

        except Exception as localize_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, UTC localization Error: {str(localize_error)}"
            logger.error(error_message)
            raise

    def convert_datetime_columns_to_local_timezone(self, df_selected):
        logger.info("Converting datetime columns to local timezone")
        try:
            datetime_columns = [
                "createdAt",
                "updatedAt",
                "date",
                "exception_at",
                "exception_whatsAppVerification_time",
                "exception_conversationStatus_time",
                "consignee_rescheduleDate",
            ]

            # Convert UTC timestamps to Egypt local time
            for col in datetime_columns:
                if col in df_selected.columns:
                    df_selected[col] = df_selected[col].dt.tz_convert(self.egypt_tz)

            logger.info(
                "Datetime columns converted to Egypt local timezone successfully"
            )
            return df_selected

        except Exception as convert_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, local timezone conversion Error: {str(convert_error)}"
            logger.error(error_message)
            raise

    def clean_string_columns_and_handle_nan_values(self, df_selected):
        logger.info("Cleaning string columns and handling NaN values")
        try:
            # Use predefined data types to identify string columns
            data_types = DATA_TYPES

            # Clean all string columns - replace various null representations with empty string
            str_columns = [col for col, dtype in data_types.items() if dtype == "str"]
            for col in str_columns:
                if col in df_selected.columns:
                    df_selected[col].replace(
                        {np.nan: "", "nan": "", "NAN": "", "NaN": "", "NaT": ""},
                        inplace=True,
                    )

            logger.info("String columns cleaned successfully")
            return df_selected

        except Exception as clean_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, string cleaning Error: {str(clean_error)}"
            logger.error(error_message)
            raise

    def truncate_string_columns_to_limits(self, df_selected):
        logger.info("Truncating string columns to limits")
        try:
            # Apply database field length constraints
            columns_to_truncate = ["business_name", "star_name"]
            if any(col in df_selected.columns for col in columns_to_truncate):
                df_selected[columns_to_truncate] = df_selected[
                    columns_to_truncate
                ].apply(lambda x: x.str.slice(0, 300))

            if "exception_reason" in df_selected.columns:
                df_selected["exception_reason"] = (
                    df_selected["exception_reason"].str.strip().str.slice(0, 200)
                )

            if "consignee_name" in df_selected.columns:
                df_selected["consignee_name"] = (
                    df_selected["consignee_name"].str.strip().str.slice(0, 150)
                )

            logger.info("String columns truncated successfully")
            return df_selected

        except Exception as truncate_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, string truncation Error: {str(truncate_error)}"
            logger.error(error_message)
            raise

    def handle_final_boolean_column_processing(self, insert_df):
        logger.info("Handling final boolean column processing")
        try:
            # Final cleanup of boolean columns - handle edge cases
            if "exception_whatsAppVerification_verified" in insert_df.columns:
                insert_df["exception_whatsAppVerification_verified"].replace(
                    "nan", np.nan, inplace=True
                )
                insert_df["exception_whatsAppVerification_verified"] = insert_df[
                    "exception_whatsAppVerification_verified"
                ].astype("bool", errors="ignore")
                insert_df["exception_whatsAppVerification_verified"].fillna(
                    False, inplace=True
                )

            if "exception_whatsAppVerification_fakeAttempt" in insert_df.columns:
                insert_df["exception_whatsAppVerification_fakeAttempt"].replace(
                    "nan", np.nan, inplace=True
                )
                insert_df["exception_whatsAppVerification_fakeAttempt"] = insert_df[
                    "exception_whatsAppVerification_fakeAttempt"
                ].astype("bool", errors="ignore")
                insert_df["exception_whatsAppVerification_fakeAttempt"].fillna(
                    False, inplace=True
                )

            logger.info("Final boolean columns processed successfully")
            return insert_df

        except Exception as boolean_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, final boolean processing Error: {str(boolean_error)}"
            logger.error(error_message)
            raise

    def handle_final_datetime_column_formatting(self, insert_df):
        logger.info("Handling final datetime column formatting")
        try:
            # Ensure datetime columns are in correct format for database insertion
            if "exception_at" in insert_df.columns:
                insert_df["exception_at"] = pd.to_datetime(
                    insert_df["exception_at"], errors="coerce"
                )

            if "exception_whatsAppVerification_time" in insert_df.columns:
                insert_df["exception_whatsAppVerification_time"] = pd.to_datetime(
                    insert_df["exception_whatsAppVerification_time"],
                    format="%Y-%m-%d %H:%M:%S",
                    errors="coerce",
                )

            logger.info("Final datetime columns formatted successfully")
            return insert_df

        except Exception as datetime_error:
            error_message = f"{MSG_TEXT}: deliveryAttempts, final datetime formatting Error: {str(datetime_error)}"
            logger.error(error_message)
            raise

    def run_transformation(self, cursor):
        logger.info("Starting transformation phase")
        try:
            #  Flatten nested MongoDB documents
            df_flattened = self.flatten_mongo_data(cursor)

            #  Select only required columns
            df_selected = self.select_required_columns(df_flattened)

            #  Clean column names for database compatibility
            df_selected = self.clean_column_names(df_selected)

            #  Rename columns to standard format
            df_selected = self.rename_columns_to_standard_format(df_selected)

            #  Handle boolean columns initial processing
            df_selected = self.handle_initial_boolean_columns(df_selected)

            #  Apply data types and handle missing columns
            df_selected = self.apply_data_types_and_handle_missing_columns(df_selected)

            #  Set timezone to UTC for datetime processing
            df_selected = self.localize_datetime_columns_to_utc(df_selected)

            #  Convert to local timezone (Egypt)
            df_selected = self.convert_datetime_columns_to_local_timezone(df_selected)

            #  Clean string columns and handle null values
            df_selected = self.clean_string_columns_and_handle_nan_values(df_selected)

            #  Apply database field length constraints
            df_selected = self.truncate_string_columns_to_limits(df_selected)

            #  Final boolean column processing
            insert_df = self.handle_final_boolean_column_processing(df_selected)

            #  Final datetime formatting for database insertion
            insert_df = self.handle_final_datetime_column_formatting(insert_df)

            # Extract the latest update timestamp for ETL metadata
            last_updated_at = (
                insert_df["updatedAt"].max()
                if "updatedAt" in insert_df.columns
                else None
            )

            logger.info("Transformation phase completed successfully")
            return insert_df, last_updated_at

        except Exception as e:
            error_message = f"{MSG_TEXT}: Transformation phase failed: {str(e)}"
            logger.error(error_message)
            raise


if __name__ == "__main__":
    from extract_phase import DataExtractor

    extractor = DataExtractor()
    cursor, mongo_df, target_date = extractor.run_extraction()

    transformer = DataTransformer()
    insert_df, last_updated_at = transformer.run_transformation(cursor)
