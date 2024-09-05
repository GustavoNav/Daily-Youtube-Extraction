import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Add project directory to python path
sys.path.append(os.path.expanduser('~/projetos/projeto_airflow'))

from keys_s3 import acess_key, secret_key

# Create SparkSession
SPARK = SparkSession.builder \
    .appName('Process to Gold Bucket') \
    .config('spark.hadoop.fs.s3a.endpoint', 'http://localhost:9000') \
    .config('spark.hadoop.fs.s3a.access.key', f'{acess_key}') \
    .config('spark.hadoop.fs.s3a.secret.key', f'{secret_key}') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.901') \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


# Extract ---------------------------------------------------------------------------------------------
def extract_channels_silver():
    bucket_name = 'youtube-data-silver'
    key_channels = 'data/channels'

    df_channels_silver = SPARK.read.option("header", 'true').csv(f's3a://{bucket_name}/{key_channels}/*')
    return df_channels_silver

def extract_videos_silver():
    bucket_name = 'youtube-data-silver'
    key_videos = 'data/videos'

    df_videos_silver = SPARK.read.option("header", 'true').csv(f's3a://{bucket_name}/{key_videos}/*')
    return df_videos_silver

# Tranform ---------------------------------------------------------------------------------------------
def transform_channels_silver(df_channels_silver):
    df_distinct_channels = df_channels_silver.select('*').distinct()

    df_channels_transformed = df_distinct_channels \
        .withColumn('silver_play_button', expr('subscriptions >= 100000')) \
        .withColumn('gold_play_button', expr('subscriptions >= 1000000')) \
        .withColumn('diamond_play_button', expr('subscriptions >= 10000000'))
    
    return df_channels_transformed

def transform_videos_silver(df_videos_silver):
    df_distinct_silver = df_videos_silver.select('*').distinct()

    return df_distinct_silver

# Load ---------------------------------------------------------------------------------------------
def load_channels_to_gold(df_channels_transformed):
    bucket_name = 'youtube-data-gold'
    key_channels = 'data/channels'

    df_channels_transformed.write.mode('overwrite').option('header', 'true').csv(f's3a://{bucket_name}/{key_channels}')

def load_videos_to_gold(df_vides_transformed):
    bucket_name = 'youtube-data-gold'
    key_videos = 'data/videos'

    df_vides_transformed.write.mode('overwrite').option('header','true').csv(f's3a://{bucket_name}/{key_videos}')

# ETL ---------------------------------------------------------------------------------------------
def etl_channels_silver_to_gold():
    # Extract data
    df_channels_silver = extract_channels_silver()

    # Transform data
    df_channels_transformed = transform_channels_silver(df_channels_silver)
    # Show transformed data (optional, for debugging)
    df_channels_transformed.show()

    # Load data
    load_channels_to_gold(df_channels_transformed)

def etl_videos_silver_to_gold():
    # Extract data
    df_videos_silver = extract_videos_silver()
    
    df_videos_transformed = transform_videos_silver(df_videos_silver)
    # Show transformed data (optional, for debugging)
    df_videos_transformed.show()

    # Load data
    load_videos_to_gold(df_videos_transformed)


default_args = {
    'owner': 'gustavo',
    'start_date': datetime(2024, 8, 24),
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
}

with DAG(
    dag_id='etl_to_gold_bucket',
    default_args=default_args,
    schedule_interval='45 0 * * *',
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id = 'etl_channels_to_gold',
        python_callable=etl_channels_silver_to_gold
    )
    task2 = PythonOperator(
        task_id = 'etl_videos_to_gold',
        python_callable=etl_videos_silver_to_gold
    )
    (task1, task2)
