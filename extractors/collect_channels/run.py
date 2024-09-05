from extractors.collect_channels.src.main import main_pipeline

# Function to extract channel informations
def run_extract_channels(urls: list) -> list:
    channels_informations = main_pipeline.run_pipeline(urls)

    return channels_informations
