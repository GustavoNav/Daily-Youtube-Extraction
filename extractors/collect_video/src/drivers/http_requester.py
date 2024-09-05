import requests
import os
import json
from bs4 import BeautifulSoup

class HttpRequester():
    def request(self, urls):

        responses = self.__request_url(urls)

        sites = self.__get_sites(responses)

        self.__save(sites)


    def __request_url(self, urls):
        headers = {'Accept-Language': 'pt-BR'}
        reponses = []

        for url in urls:
            try:
                reponses.append(requests.get(url, headers=headers))
            except:
                print('Error: ', url)
                continue
            
        return reponses
    
    def __get_sites(self, responses):
        sites = []
        for response in responses:
            sites.append(BeautifulSoup(response.text, 'html.parser'))
        
        return sites

    def __save(self, sites):
        sites_serializable = [str(site) for site in sites]

        dir_path = os.path.abspath(os.path.join(__file__, '../../../export/extract'))

        os.makedirs(dir_path, exist_ok=True)

        file_path = os.path.join(dir_path, 'html.json')

        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(sites_serializable, file, ensure_ascii=False, indent=4)