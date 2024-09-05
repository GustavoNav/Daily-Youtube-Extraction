import unittest
from extractors.collect_video.src.stages.extract.extract_html import ExtractHtml
from extractors.collect_video.src.stages.transform.transform_html import TransformHtml

class Test(unittest.TestCase):
    #python -m unittest extractors.collect_video.src.tests.test_code.Test.test_extract_html
    def test_extract_html(self):

        urls = ['https://www.youtube.com/watch?v=mOTQy90szvw', 'https://www.youtube.com/watch?v=Cj7ozYqCg04']

        extractor = ExtractHtml()

        extractor.extract(urls)
    
    #python -m unittest extractors.collect_video.src.tests.test_code.Test.test_transform_html
    def test_transform_html(self):

        transformer = TransformHtml()

        informations = []
        informations = transformer.transform()

        print(informations)

        

