# deliveries_attempts_data_etl

## Overview
ETL pipeline for extracting data from MongoDB, transforming it, and loading it into Amazon Redshift using Airflow.

## Project Structure
- `config.py`: Configuration and environment variable loading.
- `extract_phase.py`: Extracts data from MongoDB and uploads to S3.
- `transform_phase.py`: Transforms extracted data.
- `load_phase.py`: Loads data from S3 to Redshift.
- `etl_dag.py`: Airflow DAG definition.
- `requirements.txt`: Python dependencies.
- `airflow_home/`: Airflow configuration, database, and logs.
- `airflow_venv/`: Python virtual environment.

## Setup

1. **Clone the repository**
   ```sh
   git clone <repo-url>
   cd deliveries_attempts_data_etl
   ```

2. **Create and activate a virtual environment**
   ```sh
   python3 -m venv airflow_venv
   source airflow_venv/bin/activate
   ```

3. **Install dependencies**
   ```sh
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   - Copy `.env.example` to `.env` and fill in your credentials.

5. **Initialize Airflow**
   ```sh
   export AIRFLOW_HOME=$(pwd)/airflow_home
   airflow db init
   ```

## Running the ETL

- Start Airflow webserver and scheduler:
  ```sh
  airflow webserver &
  airflow scheduler &
  ```

- Trigger the DAG from the Airflow UI or CLI.

## Notes

- The `airflow_venv/` directory is ignored in `.gitignore`.
- Logs and Airflow database files are stored in `airflow_home/`.
