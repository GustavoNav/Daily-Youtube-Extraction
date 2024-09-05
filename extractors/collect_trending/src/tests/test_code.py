import unittest
from extractors.collect_trending.src.stages.extract.extract_html import ExtractHtml
from extractors.collect_trending.run import run_pipeline_trending

class Test(unittest.TestCase):

    #python -m unittest extractors.collect_trending.src.tests.test_code.Test.test_extract_html
    def test_extract_html(self):

        extractor = ExtractHtml()

        urls = extractor.extract()
        print(len(urls))
        print(urls)

    #python -m unittest extractors.collect_trending.src.tests.test_code.Test.test_run
    def test_run(self):
        run_pipeline_trending()