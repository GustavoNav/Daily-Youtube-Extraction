import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

# Get date yyyy-mm-dd
current_date = datetime.now()
FORMATTED_DATE_TODAY = current_date.strftime('%Y-%m-%d') 

# Create s3 connection
AWS_S3_CONN_ID = "my_s3_conn"


def load_to_s3():
    # Load today's video data to an s3 bucket: youtube-data-bronze

    s3_hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
    bucket_name = "youtube-data-bronze"

    filename_videos = f"/home/gustavo/youtube_data/day_{FORMATTED_DATE_TODAY}/videos.csv"
    key_videos = f"day_{FORMATTED_DATE_TODAY}/videos.csv"

    filename_channels = f"/home/gustavo/youtube_data/day_{FORMATTED_DATE_TODAY}/channels.csv"
    key_channels = f"day_{FORMATTED_DATE_TODAY}/channels.csv"

    # Insert Videos
    if os.path.exists(filename_videos):
        s3_hook.load_file(
            filename=filename_videos, 
            key=key_videos, 
            bucket_name=bucket_name, 
            replace=True
        )
    else:
        print(f"File not found: {filename_videos}")

    # Insert Channels
    if os.path.exists(filename_channels):
        s3_hook.load_file(
            filename=filename_channels, 
            key=key_channels, 
            bucket_name=bucket_name, 
            replace=True
        )
    else:
        print(f"File not found: {filename_channels}")


default_args = {
    'owner': 'gustavo',
    'start_date': datetime(2024, 8, 24),
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
}

with DAG(
        dag_id='upload_files_to_bronze_bucket',
        default_args=default_args,
        schedule_interval='15 0 * * *',
        catchup=False
    ) as dag:

    task1 = PythonOperator(
        task_id="load_to_s3_task",
        python_callable=load_to_s3
    )

    task1
