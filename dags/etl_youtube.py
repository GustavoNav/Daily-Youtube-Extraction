import sys
import os
import csv
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pympler import asizeof

# Add project directory to python path
sys.path.append(os.path.expanduser('~/projetos/Daily-Youtube-Extraction'))

# Import extract functions
from extractors.collect_trending.run import run_extract_trending
from extractors.collect_video.run import run_extract_videos
from extractors.collect_channels.run import run_extract_channels

# Get date yyyy-mm-dd
current_date = datetime.now()
FORMATTED_DATE_TODAY = current_date.strftime('%Y-%m-%d') 

default_args = {
    'owner': 'gustavo',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='youtube_get_etl',
    default_args=default_args,
    start_date=datetime(2024, 8, 19),
    schedule_interval='0 0 * * *',
    catchup=False
)
def youtube_etl():

    @task()
    def create_directory():
        # Create a directory in local machine to save the data

        base_directory = os.path.expanduser(f"~/youtube_data/day_{FORMATTED_DATE_TODAY}")
        
        try:
            os.makedirs(base_directory, exist_ok=True)
        except Exception as exception:
            print(exception)
    
    @task()
    def extract_trending():
        # Extract trending urls from youtube pt-br

        urls = run_extract_trending()
        print(f'List size: {asizeof.asizeof(urls)} bytes')

        return urls
    
    @task()
    def extract_videos(urls):
        # Extract video data from YouTube URLs

        csv_file = os.path.expanduser(f"~/youtube_data/day_{FORMATTED_DATE_TODAY}/videos.csv")

        videos_data = run_extract_videos(urls)

        with open(csv_file, mode='w', newline='') as file:
            field_names = videos_data[0].keys()

            writer = csv.DictWriter(file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(videos_data)

        channel_links = [video['channel_link'] for video in videos_data]
        print(f'videos_data size: {asizeof.asizeof(videos_data)} bytes')
        print(f'List size: {asizeof.asizeof(channel_links)} bytes')

        return channel_links

    @task()
    def extract_channels(channel_links):
        # Extract channel data from YouTube trending videos

        csv_file = os.path.expanduser(f"~/youtube_data/day_{FORMATTED_DATE_TODAY}/channels.csv")

        channels_data = run_extract_channels(channel_links)

        with open(csv_file, mode='w', newline='') as file:
            field_names = channels_data[0].keys()

            writer = csv.DictWriter(file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(channels_data)

        print(f'channels_data size: {asizeof.asizeof(channels_data)} bytes')
        print(f'{csv_file}')


    create_directory_task = create_directory()
    extract_trending_task = extract_trending()
    extract_videos_task = extract_videos(extract_trending_task)
    extract_channels_task = extract_channels(extract_videos_task)

    create_directory_task >> extract_trending_task >> extract_videos_task >> extract_channels_task

youtube_etl()
