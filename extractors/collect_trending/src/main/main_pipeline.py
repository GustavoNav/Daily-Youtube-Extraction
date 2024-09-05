from extractors.collect_trending.src.stages.extract.extract_html import ExtractHtml

class MainPipeline:
    def __init__(self) -> None:
        self.__extract_html = ExtractHtml()

    def run_pipeline(self):

        urls = self.__extract_html.extract()

        return urls

        