import pytz
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

# Messaging
MSG_TEXT = "Hello Data Team"

# Logging Configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Redshift connection parameters
REDSHIFT_PARAMS = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": 5439,
}

# MongoDB connection
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING")
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION_NAME = "eu-west-1"
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PARTITION_PREFIX = "/"
REDSHIFT_TABLE = os.getenv("REDSHIFT_TABLE")


# Constants for date handling
ETL_JOB_NAME = "deliveryAttempts"
EGYPT_TZ = pytz.timezone("Africa/Cairo")


# Data schema
COLUMNS_TO_SELECT = [
    "_id",
    "deliveryId",
    "trackingNumber",
    "business._id",
    "business.name",
    "createdAt",
    "updatedAt",
    "state",
    "type",
    "attemptDate",
    "star._id",
    "star.name",
    "country.name",
    "warehouse.name",
    "routeId",
    "consignee.name",
    "exception.reason",
    "exception.time",
    "exception.whatsAppVerification.time",
    "exception.fakeAttempt",
    "exception.whatsAppVerification.verified",
    "returnGroupId",
    "star.phone",
    "exception.whatsAppVerification.fakeAttempt",
    "exception.whatsAppVerification.conversationStatus.conversationStartedSuccessfully",
    "exception.whatsAppVerification.conversationStatus.time",
    "exception.whatsAppVerification.consigneeRescheduleData.rescheduleDate",
]

DATA_TYPES = {
    "id": "str",
    "delivery_id": "str",
    "trackingNumber": "int64",
    "business_id": "str",
    "business_name": "str",
    "createdAt": "datetime64[ns]",
    "updatedAt": "datetime64[ns]",
    "state": "int",
    "attempt_type": "str",
    "date": "datetime64[ns]",
    "exception_at": "datetime64[ns]",
    "star_id": "str",
    "star_name": "str",
    "country_name": "str",
    "warehouse_name": "str",
    "route_id": "str",
    "consignee_name": "str",
    "exception_reason": "str",
    "exception_whatsAppVerification_time": "datetime64[ns]",
    "exception_fakeAttempt": bool,
    "exception_whatsAppVerification_verified": "str",
    "returnGroupId": "str",
    "star_phone": "str",
    "exception_whatsAppVerification_fakeAttempt": bool,
    "conversationStartedSuccessfully": bool,
    "exception_conversationStatus_time": "datetime64[ns]",
    "consignee_rescheduleDate": "datetime64[ns]",
}

DELIVERIES_ATTEMPTS_COLUMNS = [
    "id",
    "delivery_id",
    "trackingNumber",
    "createdAt",
    "updatedAt",
    "state",
    "attempt_type",
    "date",
    "route_id",
    "business_id",
    "business_name",
    "exception_at",
    "star_id",
    "star_name",
    "country_name",
    "warehouse_name",
    "consignee_name",
    "exception_reason",
    "exception_whatsAppVerification_time",
    "exception_fakeAttempt",
    "exception_whatsAppVerification_verified",
    "returnGroupId",
    "star_phone",
    "exception_whatsAppVerification_fakeAttempt",
    "conversationStartedSuccessfully",
    "exception_conversationStatus_time",
    "consignee_rescheduleDate"
]