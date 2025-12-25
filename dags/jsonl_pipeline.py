"""
JSONL Data Pipeline DAG
Simple pipeline: JSONL → PostgreSQL → S3 (streaming upload)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts')

from jsonl_processor import JSONLProcessor
from postgres_handler import PostgresHandler
from aws_s3_handler import S3Handler

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jsonl_pipeline',
    default_args=default_args,
    description='Process JSONL file: Load to PostgreSQL and upload to S3',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['jsonl', 'postgresql', 's3'],
)


def load_to_postgres(**context):
    """Load JSONL data to PostgreSQL"""
    filepath = os.getenv('JSONL_FILE_PATH', '/opt/airflow/meta_Amazon_Fashion.jsonl')
    limit = int(os.getenv('JSONL_LIMIT', '10000'))  # Default 10k rows
    
    processor = JSONLProcessor()
    postgres = PostgresHandler()
    
    # Process JSONL
    print(f"Processing JSONL file: {filepath}")
    df = processor.jsonl_to_dataframe(filepath, limit=limit)
    
    # Create table schema (infer from DataFrame)
    columns = {}
    for col, dtype in df.dtypes.items():
        if dtype == 'object':
            columns[col] = 'TEXT'
        elif 'int' in str(dtype):
            columns[col] = 'INTEGER'
        elif 'float' in str(dtype):
            columns[col] = 'FLOAT'
        elif 'datetime' in str(dtype):
            columns[col] = 'TIMESTAMP'
        else:
            columns[col] = 'TEXT'
    
    # Create table
    postgres.connect()
    postgres.create_table('jsonl_data', columns)
    
    # Insert data
    postgres.insert_dataframe(df, 'jsonl_data', if_exists='append')
    postgres.disconnect()
    
    context['ti'].xcom_push(key='record_count', value=len(df))
    return f"Loaded {len(df)} records to PostgreSQL"


def upload_to_s3_streaming(**context):
    """Upload data to S3 in streaming batches"""
    filepath = os.getenv('JSONL_FILE_PATH', '/opt/airflow/meta_Amazon_Fashion.jsonl')
    batch_size = int(os.getenv('S3_BATCH_SIZE', '1000'))
    delay_seconds = int(os.getenv('S3_DELAY_SECONDS', '20'))
    
    # Import streaming upload function
    from upload_streaming import upload_in_batches
    
    # Upload in batches
    upload_in_batches(filepath, batch_size, delay_seconds)
    
    return f"Uploaded to S3 in batches of {batch_size} every {delay_seconds} seconds"


# Define tasks
load_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

upload_s3_task = PythonOperator(
    task_id='upload_to_s3_streaming',
    python_callable=upload_to_s3_streaming,
    dag=dag,
)

# Define task dependencies
load_postgres_task >> upload_s3_task

