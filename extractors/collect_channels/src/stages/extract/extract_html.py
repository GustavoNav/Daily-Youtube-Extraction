from extractors.collect_channels.src.drivers.http_requester import HttpRequester

class ExtractHtml:
    def __init__(self) -> None:
        self.__http_requester = HttpRequester()

    def extract(self, urls):
        self.__http_requester.request(urls)
