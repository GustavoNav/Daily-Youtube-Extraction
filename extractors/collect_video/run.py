from extractors.collect_video.src.main import main_pipeline

# Function to extract video data
def run_extract_videos(urls: list) -> list:
    channel_informations = main_pipeline.run_pipeline(urls)

    return channel_informations