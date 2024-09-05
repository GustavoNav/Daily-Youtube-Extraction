import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, BooleanType

# Add project directory to python path
sys.path.append(os.path.expanduser('~/projetos/projeto_airflow'))

from keys_s3 import acess_key, secret_key

# Get date yyyy-mm-dd
current_date = datetime.now()
FORMATTED_DATE_TODAY = current_date.strftime('%Y-%m-%d') 

# Create s3 connection
AWS_S3_CONN_ID = "my_s3_conn"

# Create SparkSession
SPARK = SparkSession.builder \
    .appName("Process to Silver Bucket") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", f"{acess_key}") \
    .config("spark.hadoop.fs.s3a.secret.key", f"{secret_key}") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


def check_silver_bucket():
    # check if data already exists in silver bucket, if not, it create empty data

    s3_hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
    bucket_name = "youtube-data-silver" 
    key_directory = f"data/channels/_SUCCESS"

    directory_exists = s3_hook.check_for_key(key_directory, bucket_name=bucket_name)

    if directory_exists:
        print('directory exist') 
    else:
        # Create empty dataframes and save
        print('not exist')

        schema = StructType([
            StructField("channel_id", StringType(), True),
            StructField("channel_name", StringType(), True),
            StructField("icon", StringType(), True),
            StructField("banner", StringType(), True),
            StructField("subscriptions", IntegerType(), True),
            StructField("videos", IntegerType(), True),
            StructField("views", IntegerType(), True),
            StructField("date_creation", DateType(), True),
            StructField("channel_location", StringType(), True),
            StructField("extraction_date", DateType(), True)
        ])

        df_channels_empty = SPARK.createDataFrame(SPARK.sparkContext.emptyRDD(), schema)
        df_channels_empty.write.csv("s3a://youtube-data-silver/data/channels", header=True)

        schema = StructType([
            StructField("title", StringType(), True),
            StructField("channel_id", StringType(), True),
            StructField("channel_link", StringType(), True),
            StructField("likes", IntegerType(), True),
            StructField("views", IntegerType(), True),
            StructField("comments", IntegerType(), True),
            StructField("duration", TimestampType(), True),
            StructField("date", DateType(), True),
            StructField("thumb", StringType(), True),
            StructField("family_friend", BooleanType(), True),
            StructField("extraction_date", DateType(), True)
        ])

        df_videos_empty = SPARK.createDataFrame(SPARK.sparkContext.emptyRDD(), schema)
        df_videos_empty.write.csv("s3a://youtube-data-silver/data/videos", header=True)
        

# EXTRACT ----------------------------------------------------------------------------------------------
def extract_channels_bronze():
    bucket_name = "youtube-data-bronze" 
    key_channels = f"day_{FORMATTED_DATE_TODAY}/channels.csv"

    df_channels_bronze = SPARK.read.csv(f"s3a://{bucket_name}/{key_channels}", header=True)
    return df_channels_bronze

def extract_videos_bronze():
    bucket_name = "youtube-data-bronze" 
    key_videos = f"day_{FORMATTED_DATE_TODAY}/videos.csv"

    df_videos_bronze = SPARK.read.csv(f"s3a://{bucket_name}/{key_videos}", header=True)
    return df_videos_bronze

def extract_channels_silver():
    bucket_name = "youtube-data-silver"  
    key_channels = "data/channels/"

    df_channels_silver = SPARK.read.option("header", "true").csv(f"s3a://{bucket_name}/{key_channels}/*")
    
    return df_channels_silver

def extract_videos_silver():
    bucket_name = "youtube-data-silver"  
    key_videos = "data/videos/"

    df_videos_silver = SPARK.read.option("header", "true").csv(f"s3a://{bucket_name}/{key_videos}/*")
    
    return df_videos_silver


# TRANSFORM ----------------------------------------------------------------------------------------------
def transform_channels_data(df_channels_bronze, df_channels_silver):
    transformed_channels_silver = df_channels_silver.union(df_channels_bronze)

    return transformed_channels_silver

def transform_videos_data(df_videos_bronze, df_videos_silver):
    transformed_videos_silver = df_videos_silver.union(df_videos_bronze)

    return transformed_videos_silver

# LOAD ---------------------------------------------------------------------------------------------------
def load_channels_to_silver_bucket(transformed_channels_silver):
    bucket_name = "youtube-data-silver"  
    key_channels = "data/channels/"
    
    transformed_channels_silver.write.mode("append").option("header", "true").csv(f"s3a://{bucket_name}/{key_channels}")

def load_videos_to_silver_bucket(transformed_videos_silver):
    bucket_name = "youtube-data-silver"  
    key_videos = "data/videos/"
    
    transformed_videos_silver.write.mode("append").option("header", "true").csv(f"s3a://{bucket_name}/{key_videos}")

# Execute ETL
def etl_channels():

    # Extract channels.csv
    df_channels_bronze = extract_channels_bronze()
    df_channels_silver = extract_channels_silver()

    # Show data (optional, for debugging)
    print('silver')
    df_channels_silver.show()
    print('bronze')
    df_channels_bronze.show()

    # Transform data
    transformed_channels_silver = transform_channels_data(df_channels_bronze, df_channels_silver)

    # Load data
    load_channels_to_silver_bucket(transformed_channels_silver)

def etl_videos():

    # Extract videos.csv
    df_videos_bronze = extract_videos_bronze()
    df_videos_silver = extract_videos_silver()

    # Show data (optional, for debugging)
    print('silver')
    df_videos_silver.show()
    print('bronze')
    df_videos_bronze.show()

    # Transform data  
    transformed_videos_silver = transform_videos_data(df_videos_bronze, df_videos_silver)

    # Load data
    load_videos_to_silver_bucket(transformed_videos_silver)


default_args = {
    'owner': 'gustavo',
    'start_date': datetime(2024, 8, 24),
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
}

with DAG(
        dag_id='etl_to_silver_bucket',
        default_args=default_args,
        schedule_interval='30 0 * * *',
        catchup=False
    ) as dag:

    task1 = PythonOperator(
        task_id="check_silver_bucket",
        python_callable=check_silver_bucket
    )
    task2 = PythonOperator(
        task_id="etl_channels_to_silver",
        python_callable=etl_channels,
    )
    task3 = PythonOperator(
        task_id="etl_videos_to_silver",
        python_callable=etl_videos,
    )

    task1 >> (task2, task3) 
