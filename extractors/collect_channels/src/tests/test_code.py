import unittest
from extractors.collect_channels.src.stages.extract.extract_html import ExtractHtml
from extractors.collect_channels.src.stages.transform.transform_html import TransformHtml



class Test(unittest.TestCase):
    #python -m unittest extractors.collect_channels.src.tests.test_code.Test.test_extract_html
    def test_extract_html(self):

        urls =  ['https://www.youtube.com/channel/UCR7ZwQz60rW9dK59Dirdc8w']
        extract_html = ExtractHtml()

        extract_html.extract(urls)

    #python -m unittest extractors.collect_channels.src.tests.test_code.Test.test_transform_html
    def test_transform_html(self):

        transform_html = TransformHtml()

        channel_informations = transform_html.transform()

        print(channel_informations)        
        # channel_ids = []
        # for channel in channel_informations:
        #     channel_ids.append(channel['channel_id'])
        
        # print(channel_ids)

        # # DUPLICATES
        # from collections import defaultdict

        # # Online Python - IDE, Editor, Compiler, Interpreter


        # keys = defaultdict(list)

        # # Percorra todos os elementos da lista
        # for key, value in enumerate(channel_ids):
        #     # Adicione o índice do valor na lista de índices
        #     keys[value].append(key)

        # # Exiba os resultados para valores repetidos
        # for value in keys:
        #     if len(keys[value]) > 1:
        #         print(f"Valor {value} se repete nos índices {keys[value]}")