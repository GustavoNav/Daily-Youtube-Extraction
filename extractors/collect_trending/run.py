from extractors.collect_trending.src.main import main_pipeline

#  Function to extract trending videos urls
def run_extract_trending() -> list:
    urls = main_pipeline.run_pipeline()

    return urls